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
