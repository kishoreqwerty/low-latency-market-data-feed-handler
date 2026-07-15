#include <atomic>
#include <chrono>

#include <catch2/catch_test_macros.hpp>

#include "io/async_network_reader.hpp"

using namespace mdfh::io;

namespace {

// Dedicated, minimal test coroutines -- separate from read_packets_async --
// so the Reactor/Task/WaitFor mechanism itself can be exercised directly,
// independent of FeedGenerator/ShardedPipeline.

Task counting_coroutine(Reactor& reactor, int iterations, std::atomic<int>& completed_iterations,
                         std::atomic<bool>& finished) {
    for (int i = 0; i < iterations; ++i) {
        completed_iterations.fetch_add(1, std::memory_order_relaxed);
        co_await WaitFor{reactor, std::chrono::microseconds(0)};
    }
    finished.store(true, std::memory_order_release);
}

Task cancellable_coroutine(Reactor& reactor, const std::atomic<bool>& stop_requested,
                            std::atomic<int>& iterations_run, std::atomic<bool>& finished) {
    while (!stop_requested.load(std::memory_order_acquire)) {
        iterations_run.fetch_add(1, std::memory_order_relaxed);
        co_await WaitFor{reactor, std::chrono::microseconds(0)};
    }
    finished.store(true, std::memory_order_release);
}

}  // namespace

TEST_CASE("Reactor drives a coroutine through every suspend/resume cycle to completion",
          "[async_network_reader]") {
    Reactor reactor;
    std::atomic<int> completed{0};
    std::atomic<bool> finished{false};

    Task t = counting_coroutine(reactor, 1000, completed, finished);
    (void)t;  // fire-and-forget: no handle to hold, run_until drives it to completion

    reactor.run_until([] { return false; });  // run to natural completion, never stop early

    REQUIRE(finished.load());
    REQUIRE(completed.load() == 1000);
}

TEST_CASE("Reactor correctly interleaves and completes multiple independent coroutines",
          "[async_network_reader]") {
    Reactor reactor;
    std::atomic<int> completed_a{0};
    std::atomic<int> completed_b{0};
    std::atomic<bool> finished_a{false};
    std::atomic<bool> finished_b{false};

    Task a = counting_coroutine(reactor, 500, completed_a, finished_a);
    Task b = counting_coroutine(reactor, 300, completed_b, finished_b);
    (void)a;
    (void)b;

    reactor.run_until([] { return false; });

    REQUIRE(finished_a.load());
    REQUIRE(finished_b.load());
    REQUIRE(completed_a.load() == 500);
    REQUIRE(completed_b.load() == 300);
}

TEST_CASE("Requesting stop mid-flight lets a suspended coroutine finish cleanly instead of leaking",
          "[async_network_reader]") {
    // Left alone, this coroutine would run indefinitely (bounded only by
    // stop_requested). Task's get_return_object()/initial_suspend() being
    // suspend_never means calling it below runs one iteration eagerly and
    // leaves the coroutine suspended in the reactor's queue -- the
    // interesting case is confirming that a queued-but-not-yet-scheduled
    // coroutine still gets resumed, observes the stop, and completes
    // (rather than run_until returning while it's still parked, orphaning
    // its heap-allocated frame).
    Reactor reactor;
    std::atomic<bool> stop_requested{false};
    std::atomic<int> iterations_run{0};
    std::atomic<bool> finished{false};

    Task t = cancellable_coroutine(reactor, stop_requested, iterations_run, finished);
    (void)t;

    REQUIRE(iterations_run.load() == 1);  // ran once eagerly before its first suspend
    REQUIRE_FALSE(finished.load());       // and is now genuinely suspended, not done

    stop_requested.store(true, std::memory_order_release);
    reactor.run_until([&] { return stop_requested.load(std::memory_order_acquire); });

    // If this is false, the coroutine never got resumed after stop_requested
    // was set -- i.e. exactly the leak/hang scenario this test exists to
    // catch.
    REQUIRE(finished.load());
}

TEST_CASE("Stop requested before a coroutine even starts still lets it complete without leaking",
          "[async_network_reader]") {
    Reactor reactor;
    std::atomic<bool> stop_requested{true};  // already set before the coroutine is called at all
    std::atomic<int> iterations_run{0};
    std::atomic<bool> finished{false};

    Task t = cancellable_coroutine(reactor, stop_requested, iterations_run, finished);
    (void)t;

    // The coroutine's own while-condition check happens before its body,
    // so it should complete immediately on this very first call, with no
    // pending resume left in the reactor at all.
    REQUIRE(finished.load());
    REQUIRE(iterations_run.load() == 0);

    reactor.run_until([&] { return true; });  // nothing pending; must return promptly either way
    REQUIRE(finished.load());
}
