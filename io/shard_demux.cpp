#include "io/shard_demux.hpp"

#include <stdexcept>
#include <type_traits>
#include <variant>

namespace mdfh::io {

std::size_t shard_for_symbol(const protocol::Symbol& symbol, std::size_t num_shards) noexcept {
    return protocol::hash_symbol(symbol) % num_shards;
}

ShardedPipeline::ShardedPipeline(std::size_t num_shards) {
    if (num_shards == 0 || num_shards > orderbook::kMaxShards) {
        throw std::invalid_argument("ShardedPipeline: num_shards must be in [1, orderbook::kMaxShards]");
    }
    shards_.reserve(num_shards);
    for (std::size_t i = 0; i < num_shards; ++i) {
        shards_.push_back(std::make_unique<Shard>());
    }
}

ShardedPipeline::~ShardedPipeline() {
    // Each Shard's OrderBooks were populated by that shard's own decoder
    // thread (see io/pipeline_runner.cpp), which called
    // set_current_shard_index(i) exactly once at thread start -- every
    // pool-backed node those OrderBooks hold was allocated from shard i's
    // own FixedBlockPool instances (object_pool.hpp). But this destructor
    // only ever runs after PipelineRunner has already joined every shard
    // thread, so it executes on some OTHER thread (typically main), whose
    // current_shard_index defaults to 0 -- it never called
    // set_current_shard_index for any of these shards.
    //
    // Left to the implicitly-generated destructor, shards_'s own
    // destruction would deallocate every shard's nodes while
    // current_shard_index==0 the whole time: shards 1..N-1's pools would
    // leak (in_use never decrements, since their freed blocks land on
    // shard 0's free list instead of their own) while shard 0's pool gets
    // silently corrupted with blocks that don't actually belong to its
    // storage array. This was caught empirically, not hypothetically: a
    // benchmark that repeatedly constructed and destroyed ShardedPipeline
    // in one process accumulated exactly this leak across runs until a
    // shard's pool hit capacity and threw std::bad_alloc -- see
    // bench/pinning_bench.cpp's history and object_pool.hpp's debug-mode
    // deallocate() assertion, added at the same time, which now catches
    // this class of bug immediately instead of only after enough leaked
    // capacity accumulates to overflow.
    //
    // Fix: explicitly re-set current_shard_index before destroying each
    // shard in turn, on this one thread. Safe without any lock -- by the
    // time this destructor runs, no other thread touches any of this
    // memory, so reusing one thread sequentially across every shard index
    // has nothing to race with.
    for (std::size_t i = 0; i < shards_.size(); ++i) {
        orderbook::set_current_shard_index(i);
        shards_[i].reset();
    }
}

std::size_t ShardedPipeline::shard_for_symbol(const protocol::Symbol& symbol) const noexcept {
    return io::shard_for_symbol(symbol, shards_.size());
}

std::size_t ShardedPipeline::routing_table_size() const {
    std::lock_guard<std::mutex> lock(order_route_mutex_);
    return order_route_.size();
}

std::expected<std::size_t, DemuxError> ShardedPipeline::demux(std::span<const std::byte> raw) {
    auto decoded = protocol::decode(raw);
    if (!decoded) {
        return std::unexpected(DemuxError::DecodeFailed);
    }

    return std::visit(
        [this](auto&& msg) -> std::expected<std::size_t, DemuxError> {
            using Msg = std::decay_t<decltype(msg)>;

            std::size_t shard_index;
            protocol::Symbol symbol;

            // Scoped narrowly to the order_route_ bookkeeping -- released
            // before the ring-buffer push below, so a shard thread's brief
            // wait on order_route_mutex_ (in process_shard(), for the
            // Execute cleanup) is never extended by an unrelated push.
            {
                std::lock_guard<std::mutex> lock(order_route_mutex_);

                if constexpr (std::is_same_v<Msg, protocol::AddOrder>) {
                    shard_index                = shard_for_symbol(msg.symbol);
                    symbol                      = msg.symbol;
                    order_route_[msg.order_id] = OrderRoute{shard_index, symbol};
                } else if constexpr (std::is_same_v<Msg, protocol::ReplaceOrder>) {
                    auto it = order_route_.find(msg.old_order_id);
                    if (it == order_route_.end()) {
                        return std::unexpected(DemuxError::UnroutableOrderId);
                    }
                    shard_index = it->second.shard_index;
                    symbol      = it->second.symbol;
                    order_route_.erase(it);
                    order_route_[msg.new_order_id] = OrderRoute{shard_index, symbol};
                } else {
                    // CancelOrder or ExecuteOrder: both key off order_id alone.
                    auto it = order_route_.find(msg.order_id);
                    if (it == order_route_.end()) {
                        return std::unexpected(DemuxError::UnroutableOrderId);
                    }
                    shard_index = it->second.shard_index;
                    symbol      = it->second.symbol;
                    if constexpr (std::is_same_v<Msg, protocol::CancelOrder>) {
                        order_route_.erase(it);  // cancel always fully removes the order
                    }
                }
            }

            shards_[shard_index]->queue.push(QueuedMessage{symbol, protocol::DecodedMessage{msg}});
            return shard_index;
        },
        *decoded);
}

bool ShardedPipeline::process_shard(std::size_t shard_index) {
    Shard& s        = *shards_[shard_index];
    bool did_work   = false;
    QueuedMessage qm;
    while (s.queue.try_pop(qm)) {
        did_work = true;
        s.gap_detector.observe(std::visit([](auto&& m) { return m.seq_num; }, qm.message));

        orderbook::OrderBook& book = s.books[qm.symbol];
        std::visit(
            [&](auto&& m) {
                (void)book.apply(m);

                // Cancel and Replace's old_order_id are already removed
                // from order_route_ at demux() time, since those wire
                // messages guarantee the order is gone/renamed on their
                // own. An Execute might be partial or full, and only the
                // book -- after applying it -- knows which; that's why
                // this one cleanup happens here instead of there.
                using Msg = std::decay_t<decltype(m)>;
                if constexpr (std::is_same_v<Msg, protocol::ExecuteOrder>) {
                    if (!book.has_order(m.order_id)) {
                        std::lock_guard<std::mutex> lock(order_route_mutex_);
                        order_route_.erase(m.order_id);
                    }
                }
            },
            qm.message);
    }
    return did_work;
}

void ShardedPipeline::process_pending() {
    for (std::size_t i = 0; i < shards_.size(); ++i) {
        process_shard(i);
    }
}

}  // namespace mdfh::io
