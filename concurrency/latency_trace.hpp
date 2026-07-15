#pragma once

// Phase 10: tick-to-book-update latency instrumentation.
//
// Lives in concurrency/ (not io/ or publisher/) deliberately: both
// io/shard_demux.hpp (QueuedMessage) and publisher/book_delta.hpp
// (BookDelta) need this type, and concurrency/ already sits below both in
// the dependency graph (see publisher/book_delta.hpp's header comment for
// why io depends on publisher, not the reverse) -- putting it in either of
// those two would create the exact cycle that design already avoids.
//
// Five stage-boundary timestamps, matching the pipeline's actual order
// (decode happens INSIDE demux(), before routing -- Cancel/Execute/Replace
// need the decoded order_id to look up their route -- so "decode" precedes
// "demux" here even though it's easy to say them in the other order):
//
//   network I/O -> decode -> demux (route + enqueue) -> [wait in shard's
//   input ring buffer] -> book update -> [wait in shard's output ring
//   buffer] -> publish
//
// t_dequeued and t_book_updated bracket the "wait in the input ring
// buffer" and "actual apply() cost" separately, since those are genuinely
// different things (queue contention vs. processing time) worth telling
// apart in a percentile breakdown. The final "publish" boundary isn't a
// field here -- publisher::LatencyRecordingSink (publisher/publisher.hpp)
// captures it locally, since by the time a delta reaches the publisher
// stage there's nothing further to propagate.
//
// Always populated, not compiled-out: this is a deliberate, documented
// exception to the "zero dynamic allocation in the hot path" rule's
// SPIRIT, not its letter -- these are plain steady_clock::now() calls
// (tens of nanoseconds each, no allocation), not free, but the cost of
// NOT measuring is worse for a project whose whole point is being able to
// state real latency numbers rather than assumed ones. See
// bench/latency_bench.cpp for where these traces actually get consumed.

#include <chrono>

namespace mdfh::concurrency {

using LatencyClock = std::chrono::steady_clock;

struct LatencyTrace {
    LatencyClock::time_point t_received{};      // network I/O: raw bytes available
    LatencyClock::time_point t_decoded{};        // decode: protocol::decode() returned
    LatencyClock::time_point t_demuxed{};         // demux: routed + pushed to shard's input queue
    LatencyClock::time_point t_dequeued{};        // decoder thread popped it from that queue
    LatencyClock::time_point t_book_updated{};    // OrderBookT::apply() returned
};

}  // namespace mdfh::concurrency
