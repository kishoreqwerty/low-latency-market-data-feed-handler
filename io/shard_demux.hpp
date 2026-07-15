#pragma once

// Sharding + demux (build_guide.md Phase 6): assigns symbols to shards,
// and wires one ring buffer + one gap detector + a symbol-keyed set of
// order books per shard.
//
// One deliberate refinement from a literal reading of the build guide:
// "one order book per shard" would, if taken as one flat OrderBook shared
// by every symbol hashed into that shard, merge different instruments'
// price levels together the moment two symbols collide onto the same
// shard -- a real correctness bug, not a simplification. Instead, each
// Shard owns a symbol -> OrderBook map: one book per DISTINCT SYMBOL,
// grouped for parallelism by shard. The gap detector, by contrast, stays
// genuinely one-per-shard, matching architecture_spec.md Section 3's
// explicit "per-shard sequence number" model -- sequence numbers are
// defined at the shard level, not per symbol, so gap detection belongs at
// that same granularity.
//
// Phase 7 makes this genuinely multi-threaded: one network I/O thread
// calls demux() while each shard's OWN decoder thread calls
// process_shard() concurrently (see io/pipeline_runner.hpp). Each Shard's
// queue is the SPSC ring buffer it was designed to be (network I/O thread
// is the sole producer for a given shard's queue; that shard's own thread
// is the sole consumer), and each Shard's gap_detector/books are touched
// only by that shard's own thread -- so neither needs extra locking. The
// one piece of state genuinely shared across ALL of those threads is
// order_route_ (demux() writes it for Add/Cancel/Replace; process_shard()
// also erases from it on a full Execute fill -- see process_shard's
// comment for why that specific cleanup can't happen in demux() at all),
// so it's guarded by order_route_mutex_.

#include <cstddef>
#include <expected>
#include <memory>
#include <mutex>
#include <span>
#include <unordered_map>
#include <vector>

#include "concurrency/gap_detector.hpp"
#include "concurrency/spsc_ring_buffer.hpp"
#include "orderbook/order_book.hpp"
#include "protocol/decoder.hpp"
#include "protocol/message_types.hpp"

namespace mdfh::io {

enum class DemuxError {
    DecodeFailed,       // the raw message failed protocol::decode()
    UnroutableOrderId,  // Cancel/Execute/Replace referenced an order_id with no known route
};

// Depth of each shard's inbound message queue. Independent of the order
// book pool's capacity (orderbook::kShardOrderPoolCapacity) -- this bounds
// in-flight *messages* per shard, not resting *orders*.
inline constexpr std::size_t kShardQueueCapacity = 4096;

// Travels through a shard's queue alongside the decoded message so
// process_pending() knows which symbol's book to apply it to without a
// second order_id lookup -- Cancel/Execute/Replace messages don't carry a
// symbol on the wire, only the order_id, so that lookup already happened
// once in demux(); no need to repeat it at drain time.
struct QueuedMessage {
    protocol::Symbol symbol;
    protocol::DecodedMessage message;
};

// One shard's full pipeline. Multiple symbols can and do land on the same
// shard (see shard_for_symbol), each getting its own OrderBook via `books`.
struct Shard {
    concurrency::SpscRingBuffer<QueuedMessage, kShardQueueCapacity> queue;
    concurrency::GapDetector gap_detector;
    std::unordered_map<protocol::Symbol, orderbook::OrderBook, protocol::SymbolHash> books;
};

// Hashes a symbol to a shard index in [0, num_shards).
std::size_t shard_for_symbol(const protocol::Symbol& symbol, std::size_t num_shards) noexcept;

class ShardedPipeline {
public:
    // num_shards must be in [1, orderbook::kMaxShards].
    explicit ShardedPipeline(std::size_t num_shards);

    // Decodes `raw`, determines its shard (via symbol for AddOrder, or via
    // the order_id routing table recorded by an earlier AddOrder for
    // Cancel/Execute/Replace), and enqueues it onto that shard's ring
    // buffer. Returns the shard index on success.
    std::expected<std::size_t, DemuxError> demux(std::span<const std::byte> raw);

    // Drains everything currently queued for ONE shard, feeding each
    // message through that shard's gap detector and the correct symbol's
    // order book. Returns true if it processed at least one message.
    // Meant to be called repeatedly by that shard's own dedicated thread
    // (see io/pipeline_runner.hpp) -- calling it for the same shard from
    // more than one thread concurrently is not safe (SpscRingBuffer::try_pop
    // requires a single consumer).
    bool process_shard(std::size_t shard_index);

    // Convenience wrapper: process_shard() for every shard, in order.
    // Single-threaded use only (see orderbook Phase 6 tests) -- Phase 7's
    // real pipeline uses process_shard() directly, one call site per
    // dedicated thread.
    void process_pending();

    std::size_t num_shards() const noexcept { return shards_.size(); }
    std::size_t shard_for_symbol(const protocol::Symbol& symbol) const noexcept;

    Shard& shard(std::size_t index) { return *shards_.at(index); }
    const Shard& shard(std::size_t index) const { return *shards_.at(index); }

    // Diagnostic only: how many orders the routing table currently tracks.
    // Cancel and Replace's old_order_id are removed at demux() time (the
    // wire message alone guarantees the order is gone/renamed); Execute is
    // removed in process_shard(), after checking whether the book still
    // has the order, since only the book knows if a fill was partial or
    // full. This should stay bounded to the number of currently-resting
    // orders, not grow with total messages processed. Thread-safe (locks
    // order_route_mutex_) since Phase 7 can call this while the pipeline
    // is live.
    std::size_t routing_table_size() const;

private:
    struct OrderRoute {
        std::size_t shard_index;
        protocol::Symbol symbol;
    };

    std::vector<std::unique_ptr<Shard>> shards_;

    mutable std::mutex order_route_mutex_;
    std::unordered_map<protocol::OrderId, OrderRoute> order_route_;
};

}  // namespace mdfh::io
