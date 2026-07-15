#include "io/feed_generator.hpp"

#include "io/shard_demux.hpp"
#include "protocol/encoder.hpp"

namespace mdfh::io {

FeedGenerator::FeedGenerator(Config config)
    : config_(std::move(config)), rng_(config_.seed), resting_orders_per_symbol_(config_.symbols.size()) {}

std::vector<std::byte> FeedGenerator::generate_next_message() {
    std::uniform_int_distribution<std::size_t> symbol_pick(0, config_.symbols.size() - 1);
    const std::size_t symbol_idx      = symbol_pick(rng_);
    const protocol::Symbol& symbol    = config_.symbols[symbol_idx];
    const std::size_t shard           = shard_for_symbol(symbol, config_.num_shards);
    const protocol::SeqNum seq        = ++next_seq_per_shard_[shard];

    auto& resting = resting_orders_per_symbol_[symbol_idx];

    std::uniform_real_distribution<double> action_pick(0.0, 1.0);
    const double roll = action_pick(rng_);

    // Bias toward Add whenever there's nothing resting yet to act on;
    // otherwise mix in cancels/executes/replaces against existing orders.
    if (resting.empty() || roll < 0.5) {
        const protocol::OrderId id = next_order_id_++;
        const protocol::Side side  = ((id % 2) == 0) ? protocol::Side::Buy : protocol::Side::Sell;
        const protocol::Price price     = 100 + static_cast<protocol::Price>(id % 50);
        const protocol::Quantity qty    = 10 + static_cast<protocol::Quantity>(id % 20);
        resting.push_back(id);

        protocol::AddOrder msg{
            .seq_num = seq, .order_id = id, .symbol = symbol, .side = side, .price = price, .quantity = qty,
            .timestamp = seq};
        std::vector<std::byte> buf(protocol::AddOrder::kWireSize);
        (void)protocol::encode(msg, buf);
        return buf;
    }

    std::uniform_int_distribution<std::size_t> order_pick(0, resting.size() - 1);
    const std::size_t idx      = order_pick(rng_);
    const protocol::OrderId id = resting[idx];

    if (roll < 0.65) {  // cancel
        resting.erase(resting.begin() + static_cast<std::ptrdiff_t>(idx));
        protocol::CancelOrder msg{.seq_num = seq, .order_id = id, .timestamp = seq};
        std::vector<std::byte> buf(protocol::CancelOrder::kWireSize);
        (void)protocol::encode(msg, buf);
        return buf;
    }
    if (roll < 0.85) {  // execute (small partial fill; the order stays resting)
        const protocol::Quantity exec_qty = 1 + static_cast<protocol::Quantity>(id % 5);
        protocol::ExecuteOrder msg{.seq_num = seq, .order_id = id, .executed_quantity = exec_qty, .timestamp = seq};
        std::vector<std::byte> buf(protocol::ExecuteOrder::kWireSize);
        (void)protocol::encode(msg, buf);
        return buf;
    }
    // replace
    const protocol::OrderId new_id  = next_order_id_++;
    resting[idx]                    = new_id;
    const protocol::Price new_price = 100 + static_cast<protocol::Price>(new_id % 50);
    const protocol::Quantity new_qty = 10 + static_cast<protocol::Quantity>(new_id % 20);
    protocol::ReplaceOrder msg{
        .seq_num = seq, .old_order_id = id, .new_order_id = new_id, .price = new_price, .quantity = new_qty,
        .timestamp = seq};
    std::vector<std::byte> buf(protocol::ReplaceOrder::kWireSize);
    (void)protocol::encode(msg, buf);
    return buf;
}

FeedGenerator::Packet FeedGenerator::next() {
    if (emitted_count_ >= config_.message_count) {
        return Packet{.exhausted = true, .bytes = std::nullopt};
    }
    ++emitted_count_;

    // Generate (and update internal order-tracking state) BEFORE the loss
    // coin flip: the exchange's own view of the book stays coherent
    // regardless of what's lost downstream -- loss happens in transit,
    // after a valid message was produced, exactly like real multicast.
    std::vector<std::byte> bytes = generate_next_message();

    std::uniform_real_distribution<double> loss_pick(0.0, 1.0);
    if (loss_pick(rng_) < config_.packet_loss_rate) {
        ++total_dropped_;
        return Packet{.exhausted = false, .bytes = std::nullopt};
    }
    return Packet{.exhausted = false, .bytes = std::move(bytes)};
}

}  // namespace mdfh::io
