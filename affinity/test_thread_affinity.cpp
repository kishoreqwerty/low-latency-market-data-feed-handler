// Phase 8: unit coverage for the thin macOS affinity wrapper itself.
//
// Does NOT assert that set_thread_affinity_tag() returns true. It was
// initially written that way, and immediately failed on this project's
// actual development machine (Apple Silicon, M5 Pro): thread_policy_set()
// with THREAD_AFFINITY_POLICY returns KERN_NOT_SUPPORTED (46) there, not
// KERN_SUCCESS -- confirmed with a standalone probe outside Catch2/TSan to
// rule out an interaction with either. That's not a bug in this wrapper;
// it's this Mac's kernel outright rejecting the affinity-tag call, which
// is a stronger statement than "advisory hint that might be ignored" (the
// documented Intel-era behavior) -- on Apple Silicon there may be no
// working userspace affinity mechanism via this API at all. See
// thread_affinity.hpp and bench/pinning_bench.cpp for what this means for
// the actual before/after pinning numbers.
//
// So what this test actually verifies: the call completes without
// crashing or hanging regardless of which kern_return_t the kernel gives
// back, and that tagging N different threads with N different tags is
// safe to do in a loop. Real evidence of any performance effect belongs in
// bench/pinning_bench.cpp's timing, not here.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "affinity/thread_affinity.hpp"

using namespace mdfh::affinity;

TEST_CASE("set_thread_affinity_tag completes without crashing or hanging on a live thread", "[affinity]") {
    std::atomic<bool> stop{false};
    std::thread t([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    // Captured, not REQUIRE'd: whether this is true or false is a fact
    // about the kernel this test happens to run on (see file header), not
    // a correctness property of the wrapper. Join unconditionally so a
    // std::thread destructor never sees a still-joinable thread no matter
    // what the kernel returned.
    const bool applied = set_thread_affinity_tag(t, 1);
    (void)applied;

    stop.store(true, std::memory_order_relaxed);
    t.join();

    SUCCEED("set_thread_affinity_tag returned without crashing or hanging");
}

TEST_CASE("Multiple threads can each be tagged, with distinct tags, without error", "[affinity]") {
    constexpr int kNumThreads = 4;
    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }

    for (int i = 0; i < kNumThreads; ++i) {
        // Not REQUIRE'd for the same reason as above -- see file header.
        (void)set_thread_affinity_tag(threads[static_cast<std::size_t>(i)], static_cast<AffinityTag>(i + 1));
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto& t : threads) {
        t.join();
    }

    SUCCEED("tagging 4 distinct live threads with distinct tags did not crash or hang");
}
