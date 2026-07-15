#include "io/async_network_reader.hpp"

#include <chrono>

#include "concurrency/latency_trace.hpp"

namespace mdfh::io {

Task read_packets_async(FeedGenerator& feed, ShardedPipeline& pipeline, Reactor& reactor,
                         const std::atomic<bool>& stop_requested, std::atomic<bool>& producer_done) {
    // Tiny, mostly-zero simulated inter-packet delay: real enough that the
    // suspend/resume cycle genuinely round-trips through the reactor every
    // iteration, small enough that thousands of packets don't make tests
    // slow (Reactor::run_until only actually sleeps when the scheduled
    // wake time hasn't already passed by the time it's dequeued).
    constexpr std::chrono::microseconds kInterPacketDelay{1};

    while (!stop_requested.load(std::memory_order_acquire)) {
        FeedGenerator::Packet packet = feed.next();
        if (packet.exhausted) {
            break;
        }
        if (packet.bytes.has_value()) {
            // Network-I/O stage timestamp: captured immediately, right as
            // the bytes become available -- see ShardedPipeline::demux()'s
            // comment for why this is passed in rather than let demux()
            // capture its own "now" (they'd be nearly identical here, but
            // silently defaulting would mean never actually confirming
            // that instead of measuring it).
            const auto t_received = concurrency::LatencyClock::now();
            (void)pipeline.demux(*packet.bytes, t_received);
        }
        // If bytes is nullopt, the packet was lost in transit -- simulating
        // that, correctly, means NOT delivering it anywhere, not retrying.

        co_await WaitFor{reactor, kInterPacketDelay};
    }

    producer_done.store(true, std::memory_order_release);

    // Phase 12: every shard's decoder thread may currently be genuinely
    // blocked in Shard::queue.wait_for_data() (see io/pipeline_runner.cpp),
    // and a queue with nothing left coming will never see a push() again to
    // wake it on its own -- explicitly notify each one so it promptly
    // re-checks producer_done() instead of waiting for data that will never
    // arrive. Mirrors the old poll-based design's behavior (every decoder
    // would notice producer_done on its next ~50us tick regardless), just
    // immediate instead of bounded by a poll interval.
    for (std::size_t i = 0; i < pipeline.num_shards(); ++i) {
        pipeline.shard(i).queue.notify_waiters();
    }
}

}  // namespace mdfh::io
