#include "orderbook/order_book.hpp"

#include <algorithm>
#include <memory>

namespace mdfh::orderbook {

using protocol::OrderId;
using protocol::Price;
using protocol::Side;

template <template <typename> class Alloc>
typename OrderBookT<Alloc>::Level* OrderBookT<Alloc>::level_for(Side side, Price price) {
    if (side == Side::Buy) {
        auto it = bids_.find(price);
        return it != bids_.end() ? &it->second : nullptr;
    }
    auto it = asks_.find(price);
    return it != asks_.end() ? &it->second : nullptr;
}

template <template <typename> class Alloc>
protocol::Quantity OrderBookT<Alloc>::insert_resting_order(RestingOrder order) {
    const OrderId order_id             = order.order_id;
    const Side side                    = order.side;
    const Price price                  = order.price;
    const protocol::Quantity quantity  = order.quantity;

    typename OrderList::iterator it;
    protocol::Quantity resulting_total;
    if (side == Side::Buy) {
        Level& level = bids_[price];
        level.orders.push_back(std::move(order));
        level.total_quantity += quantity;
        it              = std::prev(level.orders.end());
        resulting_total = level.total_quantity;
    } else {
        Level& level = asks_[price];
        level.orders.push_back(std::move(order));
        level.total_quantity += quantity;
        it              = std::prev(level.orders.end());
        resulting_total = level.total_quantity;
    }

    order_index_[order_id] = OrderLocation{side, price, it};
    return resulting_total;
}

template <template <typename> class Alloc>
LevelDelta OrderBookT<Alloc>::remove_resting_order(OrderId order_id) {
    auto idx_it              = order_index_.find(order_id);
    const OrderLocation& loc = idx_it->second;
    const Side side          = loc.side;
    const Price price        = loc.price;
    protocol::Quantity remaining_total = 0;  // stays 0 if the level is now empty

    if (side == Side::Buy) {
        auto level_it = bids_.find(price);
        level_it->second.total_quantity -= loc.it->quantity;
        level_it->second.orders.erase(loc.it);
        if (level_it->second.orders.empty()) {
            bids_.erase(level_it);
        } else {
            remaining_total = level_it->second.total_quantity;
        }
    } else {
        auto level_it = asks_.find(price);
        level_it->second.total_quantity -= loc.it->quantity;
        level_it->second.orders.erase(loc.it);
        if (level_it->second.orders.empty()) {
            asks_.erase(level_it);
        } else {
            remaining_total = level_it->second.total_quantity;
        }
    }

    order_index_.erase(idx_it);
    return LevelDelta{side, price, remaining_total};
}

template <template <typename> class Alloc>
std::expected<ApplyResult, ApplyError> OrderBookT<Alloc>::apply(const protocol::AddOrder& msg) {
    if (order_index_.contains(msg.order_id)) {
        return std::unexpected(ApplyError::DuplicateOrderId);
    }

    protocol::Quantity total = insert_resting_order(RestingOrder{
        .order_id = msg.order_id,
        .side     = msg.side,
        .price    = msg.price,
        .quantity = msg.quantity,
    });

    ApplyResult result;
    result.add(msg.side, msg.price, total);
    return result;
}

template <template <typename> class Alloc>
std::expected<ApplyResult, ApplyError> OrderBookT<Alloc>::apply(const protocol::CancelOrder& msg) {
    if (!order_index_.contains(msg.order_id)) {
        return std::unexpected(ApplyError::OrderNotFound);
    }

    LevelDelta touched = remove_resting_order(msg.order_id);
    ApplyResult result;
    result.add(touched.side, touched.price, touched.total_quantity);
    return result;
}

template <template <typename> class Alloc>
std::expected<ApplyResult, ApplyError> OrderBookT<Alloc>::apply(const protocol::ExecuteOrder& msg) {
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

    ApplyResult result;
    if (order.quantity == 0) {
        // remove_resting_order() re-looks-up order_id itself and erases
        // order_index_ -- everything above (loc, order) must not be
        // touched again after this call, only the returned LevelDelta.
        LevelDelta touched = remove_resting_order(msg.order_id);
        result.add(touched.side, touched.price, touched.total_quantity);
    } else {
        result.add(loc.side, loc.price, level->total_quantity);
    }
    return result;
}

template <template <typename> class Alloc>
std::expected<ApplyResult, ApplyError> OrderBookT<Alloc>::apply(const protocol::ReplaceOrder& msg) {
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
    LevelDelta vacated = remove_resting_order(msg.old_order_id);
    protocol::Quantity new_total = insert_resting_order(RestingOrder{
        .order_id = msg.new_order_id,
        .side     = side,
        .price    = msg.price,
        .quantity = msg.quantity,
    });

    // Always 2 records, even when the new price equals the old one (the
    // level never truly "vacates" in that case) -- a downstream consumer
    // applying deltas in order still ends up at the correct final state
    // either way, and callers don't need to special-case same-price
    // replaces.
    ApplyResult result;
    result.add(vacated.side, vacated.price, vacated.total_quantity);
    result.add(side, msg.price, new_total);
    return result;
}

template <template <typename> class Alloc>
BestBidAsk OrderBookT<Alloc>::best_bid_ask() const {
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

template <template <typename> class Alloc>
DepthSnapshot OrderBookT<Alloc>::depth_snapshot(std::size_t levels) const {
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

template class OrderBookT<OrderPoolAllocator>;
template class OrderBookT<std::allocator>;

}  // namespace mdfh::orderbook
