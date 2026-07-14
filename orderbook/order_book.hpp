#pragma once

#include <cstddef>
#include <expected>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "orderbook/object_pool.hpp"
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

// Maximum number of orders one book is expected to hold resting at once.
// Bounds the pool allocator below -- a per-book capacity, not a message
// volume figure (arbitrarily more messages than this can flow through over
// the book's lifetime, as long as the resting count stays under the cap).
inline constexpr std::size_t kMaxRestingOrdersPerBook = 100'000;

template <typename T>
using OrderPoolAllocator = PoolAllocator<T, kMaxRestingOrdersPerBook>;

// Single-symbol, single-threaded limit order book reconstructed from a feed
// of Add/Cancel/Execute/Replace messages (not a matching engine -- the
// exchange already matched; this just tracks the resulting resting state).
// Price levels are FIFO queues of orders, giving price-time priority.
//
// Templated on the node allocator so the exact same logic can run against
// both the production pool allocator and a plain std::allocator baseline
// (see bench/allocation_bench.cpp). OrderBook/NaiveOrderBook below are the
// two aliases anything outside this file should use.
template <template <typename> class Alloc>
class OrderBookT {
public:
    OrderBookT() { order_index_.reserve(kMaxRestingOrdersPerBook); }

    std::expected<void, ApplyError> apply(const protocol::AddOrder& msg);
    std::expected<void, ApplyError> apply(const protocol::CancelOrder& msg);
    std::expected<void, ApplyError> apply(const protocol::ExecuteOrder& msg);
    std::expected<void, ApplyError> apply(const protocol::ReplaceOrder& msg);

    BestBidAsk best_bid_ask() const;
    DepthSnapshot depth_snapshot(std::size_t levels) const;

private:
    using OrderList = std::list<RestingOrder, Alloc<RestingOrder>>;

    struct Level {
        protocol::Quantity total_quantity = 0;
        OrderList orders;  // FIFO: front = oldest = highest time priority
    };

    struct OrderLocation {
        protocol::Side side;
        protocol::Price price;
        typename OrderList::iterator it;
    };

    // Bids ordered highest-first, asks ordered lowest-first, so begin() is
    // always the best price on each side.
    using BidMap = std::map<protocol::Price, Level, std::greater<protocol::Price>,
                             Alloc<std::pair<const protocol::Price, Level>>>;
    using AskMap = std::map<protocol::Price, Level, std::less<protocol::Price>,
                             Alloc<std::pair<const protocol::Price, Level>>>;
    using OrderIndex =
        std::unordered_map<protocol::OrderId, OrderLocation, std::hash<protocol::OrderId>,
                            std::equal_to<protocol::OrderId>,
                            Alloc<std::pair<const protocol::OrderId, OrderLocation>>>;

    BidMap bids_;
    AskMap asks_;
    OrderIndex order_index_;

    Level* level_for(protocol::Side side, protocol::Price price);
    void insert_resting_order(RestingOrder order);
    void remove_resting_order(protocol::OrderId order_id);
};

// Production default: every container above draws its node memory from the
// static pool instead of the heap.
using OrderBook = OrderBookT<OrderPoolAllocator>;

// Comparison baseline for bench/allocation_bench.cpp only: identical logic,
// but backed by the plain heap allocator.
using NaiveOrderBook = OrderBookT<std::allocator>;

extern template class OrderBookT<OrderPoolAllocator>;
extern template class OrderBookT<std::allocator>;

}  // namespace mdfh::orderbook
