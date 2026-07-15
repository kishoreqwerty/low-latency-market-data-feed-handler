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

// How many parallel shard pipelines this system supports (Phase 6+): a
// small number governing thread/core count. Deliberately NOT what drives
// the pool capacity below -- many symbols hash into each shard (see
// io/shard_demux.hpp), so shard count and live-order-book count are
// different numbers; see kMaxSymbols.
inline constexpr std::size_t kMaxShards = 16;

// How many distinct symbols' order books this system is sized to track
// live at once. Phase 6 gives each DISTINCT SYMBOL its own OrderBook
// (never merging two instruments into one book), and many symbols land in
// the same shard via hashing -- so this, not kMaxShards, is what actually
// drives the pool capacity below.
inline constexpr std::size_t kMaxSymbols = 500;

// Per-symbol working-set estimate: used only to pre-reserve each
// OrderBook's own order_index_ hash table so it doesn't rehash during
// normal operation. NOT the pool's capacity -- see kOrderPoolCapacity.
inline constexpr std::size_t kExpectedRestingOrdersPerSymbol = 2'000;

// The pool backing OrderPoolAllocator (object_pool.hpp) is a PROCESS-WIDE
// singleton keyed purely by node type: every OrderBook instance, across
// every symbol and every shard, draws from the SAME underlying
// FixedBlockPool for a given node type -- not a separate pool per instance
// (see object_pool.hpp's "Known limitation" note, which this constant
// resolves). That means capacity must cover the system-wide total resting
// orders across ALL live symbols combined:
//
//     kOrderPoolCapacity = kMaxSymbols * kExpectedRestingOrdersPerSymbol
//                        = 500 * 2,000 = 1,000,000
//
// Sizing this for just one book's expected load (100,000, as it was before
// Phase 6 introduced multiple concurrently-live books) would silently
// exhaust the pool the moment a second or third symbol's book got busy,
// even though each individual book stayed well within its own expected
// size. See orderbook/test_object_pool.cpp for a test that fills
// kMaxSymbols books to exactly this capacity and confirms none of them
// throw.
inline constexpr std::size_t kOrderPoolCapacity = kMaxSymbols * kExpectedRestingOrdersPerSymbol;

template <typename T>
using OrderPoolAllocator = PoolAllocator<T, kOrderPoolCapacity>;

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
    OrderBookT() { order_index_.reserve(kExpectedRestingOrdersPerSymbol); }

    std::expected<void, ApplyError> apply(const protocol::AddOrder& msg);
    std::expected<void, ApplyError> apply(const protocol::CancelOrder& msg);
    std::expected<void, ApplyError> apply(const protocol::ExecuteOrder& msg);
    std::expected<void, ApplyError> apply(const protocol::ReplaceOrder& msg);

    BestBidAsk best_bid_ask() const;
    DepthSnapshot depth_snapshot(std::size_t levels) const;

    // True if order_id is currently resting (i.e. has not been cancelled or
    // fully executed away). Lets callers that track their own order_id ->
    // shard/symbol routing (see io/shard_demux.hpp) find out, after
    // applying an ExecuteOrder, whether that order is now gone -- the wire
    // message alone doesn't say whether a fill was partial or full, only
    // the book knows after the fact.
    bool has_order(protocol::OrderId order_id) const noexcept { return order_index_.contains(order_id); }

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
