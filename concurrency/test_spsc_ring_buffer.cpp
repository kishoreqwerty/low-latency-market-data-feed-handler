#include <atomic>
#include <chrono>
#include <thread>

#include <catch2/catch_test_macros.hpp>

#include "concurrency/spsc_ring_buffer.hpp"

using namespace mdfh::concurrency;

TEST_CASE("SpscRingBuffer pop on an empty buffer returns false", "[spsc_ring_buffer]") {
    SpscRingBuffer<int, 4> buf;
    int out = -1;
    REQUIRE_FALSE(buf.try_pop(out));
    REQUIRE(out == -1);  // untouched
    REQUIRE(buf.dropped_updates_total() == 0);
}

TEST_CASE("SpscRingBuffer preserves FIFO order for a simple push/pop sequence", "[spsc_ring_buffer]") {
    SpscRingBuffer<int, 4> buf;

    buf.push(10);
    buf.push(20);
    buf.push(30);

    int out = 0;
    REQUIRE(buf.try_pop(out));
    REQUIRE(out == 10);
    REQUIRE(buf.try_pop(out));
    REQUIRE(out == 20);
    REQUIRE(buf.try_pop(out));
    REQUIRE(out == 30);
    REQUIRE_FALSE(buf.try_pop(out));  // now empty

    REQUIRE(buf.dropped_updates_total() == 0);
}

TEST_CASE("SpscRingBuffer fills exactly to capacity without dropping anything", "[spsc_ring_buffer]") {
    SpscRingBuffer<int, 4> buf;

    buf.push(1);
    buf.push(2);
    buf.push(3);
    buf.push(4);  // exactly full, no overflow yet

    REQUIRE(buf.dropped_updates_total() == 0);

    int out = 0;
    for (int expected = 1; expected <= 4; ++expected) {
        REQUIRE(buf.try_pop(out));
        REQUIRE(out == expected);
    }
    REQUIRE_FALSE(buf.try_pop(out));
}

TEST_CASE("SpscRingBuffer drops exactly the oldest entry on a single-item overflow", "[spsc_ring_buffer]") {
    SpscRingBuffer<int, 4> buf;

    buf.push(1);
    buf.push(2);
    buf.push(3);
    buf.push(4);  // full
    buf.push(5);  // overflow: drops 1 (the oldest), keeps 2..5

    REQUIRE(buf.dropped_updates_total() == 1);

    int out = 0;
    for (int expected = 2; expected <= 5; ++expected) {
        REQUIRE(buf.try_pop(out));
        REQUIRE(out == expected);
    }
    REQUIRE_FALSE(buf.try_pop(out));
}

TEST_CASE("SpscRingBuffer wraps around correctly across many cycles", "[spsc_ring_buffer]") {
    SpscRingBuffer<int, 4> buf;

    // Push and pop one at a time, well past capacity, so the underlying
    // index wraps around the physical array many times over. FIFO order
    // must hold at every step, and nothing should ever be dropped since the
    // buffer never holds more than one item at a time here.
    for (int i = 0; i < 100; ++i) {
        buf.push(i);
        int out = -1;
        REQUIRE(buf.try_pop(out));
        REQUIRE(out == i);
    }

    REQUIRE(buf.dropped_updates_total() == 0);
    int out = -1;
    REQUIRE_FALSE(buf.try_pop(out));
}

TEST_CASE("SpscRingBuffer drop-oldest keeps only the newest Capacity items after a large overflow",
          "[spsc_ring_buffer]") {
    constexpr std::size_t kCapacity = 8;
    constexpr int kTotalPushed      = 25;  // more than 3x capacity, all before any pop
    SpscRingBuffer<int, kCapacity> buf;

    for (int i = 0; i < kTotalPushed; ++i) {
        buf.push(i);
    }

    REQUIRE(buf.dropped_updates_total() == kTotalPushed - static_cast<int>(kCapacity));

    // Only the newest kCapacity items (17..24) should remain, oldest-first.
    int out = 0;
    for (int expected = kTotalPushed - static_cast<int>(kCapacity); expected < kTotalPushed; ++expected) {
        REQUIRE(buf.try_pop(out));
        REQUIRE(out == expected);
    }
    REQUIRE_FALSE(buf.try_pop(out));
}

TEST_CASE("SpscRingBuffer handles repeated fill-drain cycles interleaved with overflow", "[spsc_ring_buffer]") {
    constexpr std::size_t kCapacity = 4;
    SpscRingBuffer<int, kCapacity> buf;
    int next_value = 0;

    for (int cycle = 0; cycle < 20; ++cycle) {
        // Overfill by 2 each cycle: pushes kCapacity + 2, drops 2.
        int first_in_cycle = next_value;
        for (std::size_t i = 0; i < kCapacity + 2; ++i) {
            buf.push(next_value++);
        }

        int out = 0;
        for (int expected = first_in_cycle + 2; expected < next_value; ++expected) {
            REQUIRE(buf.try_pop(out));
            REQUIRE(out == expected);
        }
        REQUIRE_FALSE(buf.try_pop(out));  // fully drained before the next cycle
    }

    REQUIRE(buf.dropped_updates_total() == 20 * 2);
}

// Phase 12: basic correctness tests for wait_for_data()/notify_waiters(),
// the blocking-consumer API replacing io/pipeline_runner.cpp's old
// sleep_for(50us) poll. The genuine block-then-wake behavior can only be
// tested with a second real thread (that's the point of it), but this file
// still covers what doesn't need real concurrency: the non-blocking fast
// path, and one deliberately minimal 2-thread test proving an actual wake,
// not just an eventual return. Real concurrent stress (sustained
// overwhelmed producer, slowed consumer) is in
// test_spsc_ring_buffer_stress.cpp, matching Phase 5's split between
// logic tests here and TSan-verified concurrency tests there.

TEST_CASE("SpscRingBuffer wait_for_data() returns immediately when data is already present",
          "[spsc_ring_buffer]") {
    SpscRingBuffer<int, 4> buf;
    buf.push(7);

    const auto start = std::chrono::steady_clock::now();
    buf.wait_for_data();  // must NOT block: data is already there
    const auto elapsed = std::chrono::steady_clock::now() - start;

    REQUIRE(elapsed < std::chrono::milliseconds(50));  // generous bound; real cost is sub-microsecond

    int out = 0;
    REQUIRE(buf.try_pop(out));
    REQUIRE(out == 7);
}

TEST_CASE("SpscRingBuffer notify_waiters() with nobody waiting is a safe no-op", "[spsc_ring_buffer]") {
    SpscRingBuffer<int, 4> buf;
    buf.notify_waiters();  // no waiter exists at all -- must not crash or hang
    SUCCEED("notify_waiters() with no waiter returned without incident");
}

TEST_CASE("SpscRingBuffer wait_for_data() blocks until a genuine push() wakes it, not just eventually",
          "[spsc_ring_buffer]") {
    SpscRingBuffer<int, 4> buf;
    std::atomic<bool> woke{false};

    std::thread waiter([&] {
        buf.wait_for_data();
        woke.store(true, std::memory_order_release);
    });

    // Give the waiter thread every chance to actually reach wait_for_data()
    // and genuinely block before pushing -- otherwise this test could pass
    // for the wrong reason (racing the push against the thread not having
    // started yet, rather than proving a real block-then-wake).
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    REQUIRE_FALSE(woke.load(std::memory_order_acquire));  // still genuinely blocked, not a spurious return

    buf.push(42);

    // Bounded wait, not an unconditional join: if wait_for_data() ever has a
    // missed-wakeup bug, this fails the assertion below loudly instead of
    // hanging the whole test binary. The safety-net notify_waiters() call
    // guarantees the waiter thread becomes joinable either way, so the test
    // process always terminates cleanly regardless of outcome.
    constexpr auto kBound = std::chrono::seconds(2);
    const auto deadline   = std::chrono::steady_clock::now() + kBound;
    while (!woke.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!woke.load(std::memory_order_acquire)) {
        buf.notify_waiters();  // safety net only -- not expected to be needed
    }
    waiter.join();

    REQUIRE(woke.load(std::memory_order_acquire));

    int out = 0;
    REQUIRE(buf.try_pop(out));
    REQUIRE(out == 42);
}
