// Real two-thread concurrency tests for SpscRingBuffer. These are the tests
// that actually exercise the producer/consumer race the design in
// spsc_ring_buffer.hpp is built to handle -- the single-threaded tests in
// test_spsc_ring_buffer.cpp check the *logic*, but only genuine concurrent
// execution under ThreadSanitizer can confirm the synchronization is
// actually correct. This phase is not done until both tests here pass
// cleanly under the debug-tsan preset with zero reported races.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "concurrency/spsc_ring_buffer.hpp"

using namespace mdfh::concurrency;

namespace {

// Runs a producer (pushing 0..count-1) and a consumer (draining until the
// producer is done and the buffer is confirmed empty) on real, separate
// threads, then verifies: every pushed value is either received exactly
// once or counted in dropped_updates_total() -- never both, never neither,
// never duplicated, never reordered, never corrupted.
template <std::size_t Capacity, typename ConsumerDelay>
void run_concurrency_check(std::uint64_t count, ConsumerDelay consumer_delay) {
    SpscRingBuffer<std::uint64_t, Capacity> buf;
    std::atomic<bool> producer_done{false};
    std::vector<std::uint64_t> received;
    received.reserve(static_cast<std::size_t>(count));

    std::thread producer([&] {
        for (std::uint64_t i = 0; i < count; ++i) {
            buf.push(i);
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::thread consumer([&] {
        std::uint64_t value = 0;
        for (;;) {
            if (buf.try_pop(value)) {
                received.push_back(value);
            } else if (producer_done.load(std::memory_order_acquire)) {
                break;  // no more pushes are coming; final drain below handles stragglers
            }
            consumer_delay();
        }
        while (buf.try_pop(value)) {
            received.push_back(value);
        }
    });

    producer.join();
    consumer.join();

    const std::uint64_t dropped = buf.dropped_updates_total();

    INFO("count=" << count << " capacity=" << Capacity << " received=" << received.size()
                   << " dropped=" << dropped);
    REQUIRE(received.size() + dropped == count);

    // No duplicates, nothing out of range, and survivors strictly
    // increasing -- drop-oldest must skip entries, never reorder or
    // duplicate them.
    std::vector<bool> seen(static_cast<std::size_t>(count), false);
    std::uint64_t last = 0;
    bool first         = true;
    for (std::uint64_t v : received) {
        REQUIRE(v < count);
        REQUIRE_FALSE(seen[static_cast<std::size_t>(v)]);
        seen[static_cast<std::size_t>(v)] = true;
        if (!first) {
            REQUIRE(v > last);
        }
        last  = v;
        first = false;
    }
}

}  // namespace

TEST_CASE("SpscRingBuffer: sustained overwhelmed producer under real concurrency", "[spsc_ring_buffer][stress]") {
    // Small capacity + high volume + an unthrottled consumer: the producer
    // reliably and repeatedly outruns the consumer for the entire run, so
    // the drop-oldest eviction path (the CAS race in push()/try_pop()) fires
    // continuously and sustained, not just once at start-up.
    run_concurrency_check<64>(2'000'000, [] {});
}

TEST_CASE("SpscRingBuffer: artificially slowed consumer forces the eviction-collision spin-wait",
          "[spsc_ring_buffer][stress]") {
    // A consumer this slow relative to a tight producer loop is very likely
    // to lose the head_ CAS race to a producer eviction while genuinely
    // mid-read, specifically exercising the reader_active spin-wait in
    // push()'s losing branch rather than the immediate-win path. Smaller
    // item count since every pop is deliberately paced.
    run_concurrency_check<16>(5'000, [] { std::this_thread::sleep_for(std::chrono::microseconds(20)); });
}

namespace {

// Phase 12: same correctness contract as run_concurrency_check() above
// (every pushed value received exactly once or accounted for in
// dropped_updates_total(), never both, never neither, never reordered), but
// the consumer uses the NEW wait_for_data() blocking wait instead of a
// busy-spin/sleep poll -- this is what actually exercises the
// std::atomic::wait/notify mechanism under real concurrency, not just the
// existing try_pop()/push() CAS race (which run_concurrency_check already
// covers and remains unchanged).
//
// Mirrors io/pipeline_runner.cpp's real shutdown protocol exactly: the
// producer sets producer_done AND explicitly calls notify_waiters() as its
// last act, since producer_done is a plain bool the ring buffer knows
// nothing about -- a consumer blocked in wait_for_data() would otherwise
// never wake to notice the producer finished.
template <std::size_t Capacity, typename ProducerPacing, typename ConsumerPacing>
void run_blocking_concurrency_check(std::uint64_t count, ProducerPacing producer_pacing,
                                     ConsumerPacing consumer_pacing) {
    SpscRingBuffer<std::uint64_t, Capacity> buf;
    std::atomic<bool> producer_done{false};
    std::vector<std::uint64_t> received;
    received.reserve(static_cast<std::size_t>(count));

    std::thread producer([&] {
        for (std::uint64_t i = 0; i < count; ++i) {
            buf.push(i);
            producer_pacing();
        }
        producer_done.store(true, std::memory_order_release);
        buf.notify_waiters();  // wake a consumer that's blocked with nothing left coming
    });

    std::thread consumer([&] {
        std::uint64_t value = 0;
        for (;;) {
            if (buf.try_pop(value)) {
                received.push_back(value);
                consumer_pacing();
                continue;
            }
            if (producer_done.load(std::memory_order_acquire)) {
                break;  // no more pushes coming; final drain below handles stragglers
            }
            buf.wait_for_data();  // genuine OS-level block, not a poll
        }
        while (buf.try_pop(value)) {
            received.push_back(value);
        }
    });

    producer.join();
    consumer.join();

    const std::uint64_t dropped = buf.dropped_updates_total();

    INFO("count=" << count << " capacity=" << Capacity << " received=" << received.size()
                   << " dropped=" << dropped);
    REQUIRE(received.size() + dropped == count);

    std::vector<bool> seen(static_cast<std::size_t>(count), false);
    std::uint64_t last = 0;
    bool first         = true;
    for (std::uint64_t v : received) {
        REQUIRE(v < count);
        REQUIRE_FALSE(seen[static_cast<std::size_t>(v)]);
        seen[static_cast<std::size_t>(v)] = true;
        if (!first) {
            REQUIRE(v > last);
        }
        last  = v;
        first = false;
    }
}

}  // namespace

TEST_CASE("SpscRingBuffer: blocking consumer under sustained overwhelmed producer",
          "[spsc_ring_buffer][stress][blocking]") {
    // Same shape as the try_pop()-race version above (small capacity, high
    // volume, unthrottled producer) but the consumer now uses
    // wait_for_data(): with the producer this far ahead almost the entire
    // run, the queue is essentially never empty from the consumer's point
    // of view, so this mostly stresses "does push()'s unconditional
    // notify_one() cause any correctness problem under sustained
    // saturation" (it's called ~2 million times here, almost always with
    // no one waiting) plus drop-oldest still firing continuously -- and the
    // few genuine block/wake transitions this DOES force, at the very
    // start and the very end of the run.
    run_blocking_concurrency_check<64>(
        2'000'000, [] {}, [] {});
}

TEST_CASE("SpscRingBuffer: slowed consumer relies on the producer-done notify_waiters() wake, not luck",
          "[spsc_ring_buffer][stress][blocking]") {
    // The consumer is deliberately slow (sleeps after every successful
    // pop) relative to an unthrottled producer -- exactly Phase 5's
    // "artificially slowed consumer" shape, so the queue predictably fills
    // and drop-oldest fires throughout the run, but the interesting new
    // question is what happens at the END: the producer finishes and sets
    // producer_done long before this slow consumer has drained everything
    // still queued, so the consumer WILL, at some point, observe an empty
    // queue with producer_done still false, genuinely block in
    // wait_for_data(), and must be woken by the producer thread's final
    // notify_waiters() call rather than a subsequent push() (there isn't
    // one). This is the exact mechanism io/pipeline_runner.cpp's real
    // shutdown protocol depends on for every shard.
    run_blocking_concurrency_check<16>(
        5'000, [] {}, [] { std::this_thread::sleep_for(std::chrono::microseconds(20)); });
}
