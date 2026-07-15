#pragma once

// Phase 9: the downstream-facing record type for order-book changes, plus
// the per-shard output queue that carries them from a shard's decoder
// thread to that shard's publisher thread.
//
// Deliberately depends on nothing above concurrency/ and protocol/ -- not
// on io/shard_demux.hpp's Shard or ShardedPipeline, even though this is
// what populates Shard::output_queue (see io/shard_demux.hpp/.cpp). That
// asymmetry is intentional: io/ (which needs BookDelta/DeltaQueue's TYPES
// to define Shard) depends on publisher/ for those types, while
// publisher/'s actual consumer logic (publisher.hpp) only needs a
// DeltaQueue& and an "upstream done" signal -- generic enough to depend on
// neither io::Shard nor io::ShardedPipeline. Reversing that (publisher/
// depending on io/ for Shard access) would create a cycle the moment
// io/pipeline_runner.cpp needs to spawn publisher threads using
// publisher/'s logic.

#include <cstddef>

#include "concurrency/latency_trace.hpp"
#include "concurrency/spsc_ring_buffer.hpp"
#include "protocol/message_types.hpp"

namespace mdfh::publisher {

// A single price-level change, enriched with the symbol and the input
// message's own seq_num/timestamp -- orderbook::LevelDelta (see
// orderbook/order_book.hpp) carries everything BUT the symbol, since a
// single OrderBookT instance only ever tracks one symbol; the shard layer
// (which maps symbol -> OrderBook, see io/shard_demux.hpp) is what adds it
// back. total_quantity == 0 means the level is now empty (removed).
struct BookDelta {
    protocol::Symbol symbol{};
    protocol::Side side{};
    protocol::Price price = 0;
    protocol::Quantity total_quantity = 0;
    protocol::SeqNum seq_num          = 0;  // the shard-sequence input message that caused this delta
    protocol::Timestamp timestamp     = 0;

    // Phase 10: carried forward from the QueuedMessage that produced this
    // delta (io/shard_demux.cpp's process_shard() copies it in, adding
    // t_dequeued/t_book_updated) -- see concurrency/latency_trace.hpp and
    // publisher::LatencyRecordingSink, which is what actually turns this
    // into reported percentiles.
    concurrency::LatencyTrace trace{};
};

// Depth of each shard's OUTPUT queue -- a second, independent SPSC ring
// buffer per shard (Shard::output_queue), same drop-oldest policy as
// Phase 5's input queues (concurrency/spsc_ring_buffer.hpp) and the same
// reasoning: a slow/stalled downstream consumer should never be able to
// block or slow down the decoder thread producing deltas, only fall
// behind and lose the OLDEST unconsumed deltas for that shard. Sized
// independently from kShardQueueCapacity (io/shard_demux.hpp) since a
// single input message can produce up to 2 deltas (ReplaceOrder).
inline constexpr std::size_t kDeltaQueueCapacity = 4096;

using DeltaQueue = concurrency::SpscRingBuffer<BookDelta, kDeltaQueueCapacity>;

}  // namespace mdfh::publisher
