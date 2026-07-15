#include "io/async_network_reader.hpp"

#include <chrono>

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
            (void)pipeline.demux(*packet.bytes);
        }
        // If bytes is nullopt, the packet was lost in transit -- simulating
        // that, correctly, means NOT delivering it anywhere, not retrying.

        co_await WaitFor{reactor, kInterPacketDelay};
    }

    producer_done.store(true, std::memory_order_release);
}

}  // namespace mdfh::io
