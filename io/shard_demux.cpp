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

std::size_t ShardedPipeline::shard_for_symbol(const protocol::Symbol& symbol) const noexcept {
    return io::shard_for_symbol(symbol, shards_.size());
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

            if constexpr (std::is_same_v<Msg, protocol::AddOrder>) {
                shard_index = shard_for_symbol(msg.symbol);
                symbol      = msg.symbol;
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

            shards_[shard_index]->queue.push(QueuedMessage{symbol, protocol::DecodedMessage{msg}});
            return shard_index;
        },
        *decoded);
}

void ShardedPipeline::process_pending() {
    for (auto& shard_ptr : shards_) {
        Shard& s = *shard_ptr;
        QueuedMessage qm;
        while (s.queue.try_pop(qm)) {
            s.gap_detector.observe(std::visit([](auto&& m) { return m.seq_num; }, qm.message));

            orderbook::OrderBook& book = s.books[qm.symbol];
            std::visit(
                [&](auto&& m) {
                    (void)book.apply(m);

                    // Cancel and Replace's old_order_id are already removed
                    // from order_route_ at demux() time, since those wire
                    // messages guarantee the order is gone/renamed on their
                    // own. An Execute might be partial or full, and only
                    // the book -- after applying it -- knows which; that's
                    // why this one cleanup happens here instead of there.
                    using Msg = std::decay_t<decltype(m)>;
                    if constexpr (std::is_same_v<Msg, protocol::ExecuteOrder>) {
                        if (!book.has_order(m.order_id)) {
                            order_route_.erase(m.order_id);
                        }
                    }
                },
                qm.message);
        }
    }
}

}  // namespace mdfh::io
