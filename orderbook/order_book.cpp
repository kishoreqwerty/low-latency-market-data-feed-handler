#include "orderbook/order_book.hpp"

#include <algorithm>

namespace mdfh::orderbook {

using protocol::OrderId;
using protocol::Price;
using protocol::Side;

OrderBook::Level* OrderBook::level_for(Side side, Price price) {
    if (side == Side::Buy) {
        auto it = bids_.find(price);
        return it != bids_.end() ? &it->second : nullptr;
    }
    auto it = asks_.find(price);
    return it != asks_.end() ? &it->second : nullptr;
}

void OrderBook::insert_resting_order(RestingOrder order) {
    const OrderId order_id = order.order_id;
    const Side side        = order.side;
    const Price price      = order.price;
    const protocol::Quantity quantity = order.quantity;

    std::list<RestingOrder>::iterator it;
    if (side == Side::Buy) {
        Level& level = bids_[price];
        level.orders.push_back(std::move(order));
        level.total_quantity += quantity;
        it = std::prev(level.orders.end());
    } else {
        Level& level = asks_[price];
        level.orders.push_back(std::move(order));
        level.total_quantity += quantity;
        it = std::prev(level.orders.end());
    }

    order_index_[order_id] = OrderLocation{side, price, it};
}

void OrderBook::remove_resting_order(OrderId order_id) {
    auto idx_it              = order_index_.find(order_id);
    const OrderLocation& loc = idx_it->second;

    if (loc.side == Side::Buy) {
        auto level_it = bids_.find(loc.price);
        level_it->second.total_quantity -= loc.it->quantity;
        level_it->second.orders.erase(loc.it);
        if (level_it->second.orders.empty()) {
            bids_.erase(level_it);
        }
    } else {
        auto level_it = asks_.find(loc.price);
        level_it->second.total_quantity -= loc.it->quantity;
        level_it->second.orders.erase(loc.it);
        if (level_it->second.orders.empty()) {
            asks_.erase(level_it);
        }
    }

    order_index_.erase(idx_it);
}

std::expected<void, ApplyError> OrderBook::apply(const protocol::AddOrder& msg) {
    if (order_index_.contains(msg.order_id)) {
        return std::unexpected(ApplyError::DuplicateOrderId);
    }

    insert_resting_order(RestingOrder{
        .order_id = msg.order_id,
        .side     = msg.side,
        .price    = msg.price,
        .quantity = msg.quantity,
    });

    return {};
}

std::expected<void, ApplyError> OrderBook::apply(const protocol::CancelOrder& msg) {
    if (!order_index_.contains(msg.order_id)) {
        return std::unexpected(ApplyError::OrderNotFound);
    }

    remove_resting_order(msg.order_id);
    return {};
}

std::expected<void, ApplyError> OrderBook::apply(const protocol::ExecuteOrder& msg) {
    auto idx_it = order_index_.find(msg.order_id);
    if (idx_it == order_index_.end()) {
        return std::unexpected(ApplyError::OrderNotFound);
    }

    const OrderLocation& loc = idx_it->second;
    RestingOrder& order      = *loc.it;

    if (msg.executed_quantity > order.quantity) {
        return std::unexpected(ApplyError::ExecutedQuantityExceedsRemaining);
    }

    Level* level = level_for(loc.side, loc.price);
    level->total_quantity -= msg.executed_quantity;
    order.quantity -= msg.executed_quantity;

    if (order.quantity == 0) {
        remove_resting_order(msg.order_id);
    }

    return {};
}

std::expected<void, ApplyError> OrderBook::apply(const protocol::ReplaceOrder& msg) {
    auto idx_it = order_index_.find(msg.old_order_id);
    if (idx_it == order_index_.end()) {
        return std::unexpected(ApplyError::OrderNotFound);
    }
    if (order_index_.contains(msg.new_order_id)) {
        return std::unexpected(ApplyError::DuplicateOrderId);
    }

    // Replace preserves side (the wire message never restates it) but drops
    // time priority: the new order goes to the back of its new price level.
    const Side side = idx_it->second.side;
    remove_resting_order(msg.old_order_id);
    insert_resting_order(RestingOrder{
        .order_id = msg.new_order_id,
        .side     = side,
        .price    = msg.price,
        .quantity = msg.quantity,
    });

    return {};
}

BestBidAsk OrderBook::best_bid_ask() const {
    BestBidAsk result;
    if (!bids_.empty()) {
        const auto& [price, level] = *bids_.begin();
        result.bid = PriceLevelView{price, level.total_quantity, level.orders.size()};
    }
    if (!asks_.empty()) {
        const auto& [price, level] = *asks_.begin();
        result.ask = PriceLevelView{price, level.total_quantity, level.orders.size()};
    }
    return result;
}

DepthSnapshot OrderBook::depth_snapshot(std::size_t levels) const {
    DepthSnapshot snapshot;
    snapshot.bids.reserve(std::min(levels, bids_.size()));
    snapshot.asks.reserve(std::min(levels, asks_.size()));

    std::size_t count = 0;
    for (const auto& [price, level] : bids_) {
        if (count++ >= levels) break;
        snapshot.bids.push_back(PriceLevelView{price, level.total_quantity, level.orders.size()});
    }

    count = 0;
    for (const auto& [price, level] : asks_) {
        if (count++ >= levels) break;
        snapshot.asks.push_back(PriceLevelView{price, level.total_quantity, level.orders.size()});
    }

    return snapshot;
}

}  // namespace mdfh::orderbook
