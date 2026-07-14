#pragma once

#include <cstddef>
#include <expected>
#include <list>
#include <map>
#include <optional>
#include <unordered_map>
#include <vector>

#include "protocol/message_types.hpp"

namespace mdfh::orderbook {

enum class ApplyError {
    OrderNotFound,                     // Cancel/Execute/Replace referenced an unknown order_id
    ExecutedQuantityExceedsRemaining,  // ExecuteOrder asked to fill more than is resting
    DuplicateOrderId,                  // Add/Replace introduced an order_id already resting
};

struct RestingOrder {
    protocol::OrderId order_id;
    protocol::Side side;
    protocol::Price price;
    protocol::Quantity quantity;  // remaining (unexecuted) quantity

    friend bool operator==(const RestingOrder&, const RestingOrder&) = default;
};

// A price level's aggregate view, as exposed by best_bid_ask()/depth_snapshot().
struct PriceLevelView {
    protocol::Price price;
    protocol::Quantity total_quantity;
    std::size_t order_count;

    friend bool operator==(const PriceLevelView&, const PriceLevelView&) = default;
};

struct BestBidAsk {
    std::optional<PriceLevelView> bid;  // highest resting buy price
    std::optional<PriceLevelView> ask;  // lowest resting sell price
};

struct DepthSnapshot {
    std::vector<PriceLevelView> bids;  // best (highest) first
    std::vector<PriceLevelView> asks;  // best (lowest) first
};

// Single-symbol, single-threaded limit order book reconstructed from a feed
// of Add/Cancel/Execute/Replace messages (not a matching engine -- the
// exchange already matched; this just tracks the resulting resting state).
// Price levels are FIFO queues of orders, giving price-time priority.
class OrderBook {
public:
    std::expected<void, ApplyError> apply(const protocol::AddOrder& msg);
    std::expected<void, ApplyError> apply(const protocol::CancelOrder& msg);
    std::expected<void, ApplyError> apply(const protocol::ExecuteOrder& msg);
    std::expected<void, ApplyError> apply(const protocol::ReplaceOrder& msg);

    BestBidAsk best_bid_ask() const;
    DepthSnapshot depth_snapshot(std::size_t levels) const;

private:
    struct Level {
        protocol::Quantity total_quantity = 0;
        std::list<RestingOrder> orders;  // FIFO: front = oldest = highest time priority
    };

    struct OrderLocation {
        protocol::Side side;
        protocol::Price price;
        std::list<RestingOrder>::iterator it;
    };

    // Bids ordered highest-first, asks ordered lowest-first, so begin() is
    // always the best price on each side.
    std::map<protocol::Price, Level, std::greater<>> bids_;
    std::map<protocol::Price, Level> asks_;
    std::unordered_map<protocol::OrderId, OrderLocation> order_index_;

    Level* level_for(protocol::Side side, protocol::Price price);
    void insert_resting_order(RestingOrder order);
    void remove_resting_order(protocol::OrderId order_id);
};

}  // namespace mdfh::orderbook
