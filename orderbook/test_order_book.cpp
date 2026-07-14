#include <catch2/catch_test_macros.hpp>

#include "orderbook/order_book.hpp"
#include "protocol/message_types.hpp"

using namespace mdfh::orderbook;
namespace protocol = mdfh::protocol;

namespace {

protocol::AddOrder make_add(protocol::SeqNum seq, protocol::OrderId id, protocol::Side side,
                             protocol::Price price, protocol::Quantity qty) {
    return protocol::AddOrder{
        .seq_num   = seq,
        .order_id  = id,
        .symbol    = protocol::make_symbol("TEST"),
        .side      = side,
        .price     = price,
        .quantity  = qty,
        .timestamp = seq * 100,
    };
}

protocol::CancelOrder make_cancel(protocol::SeqNum seq, protocol::OrderId id) {
    return protocol::CancelOrder{.seq_num = seq, .order_id = id, .timestamp = seq * 100};
}

protocol::ExecuteOrder make_execute(protocol::SeqNum seq, protocol::OrderId id, protocol::Quantity qty) {
    return protocol::ExecuteOrder{.seq_num = seq, .order_id = id, .executed_quantity = qty, .timestamp = seq * 100};
}

protocol::ReplaceOrder make_replace(protocol::SeqNum seq, protocol::OrderId old_id, protocol::OrderId new_id,
                                     protocol::Price price, protocol::Quantity qty) {
    return protocol::ReplaceOrder{
        .seq_num      = seq,
        .old_order_id = old_id,
        .new_order_id = new_id,
        .price        = price,
        .quantity     = qty,
        .timestamp    = seq * 100,
    };
}

}  // namespace

TEST_CASE("OrderBook reconstructs price-time priority through a scripted sequence", "[orderbook]") {
    OrderBook book;

    // Step 1-2: two buy orders at 100 (order 1 then order 2) -> FIFO within the level.
    REQUIRE(book.apply(make_add(1, 1, protocol::Side::Buy, 100, 10)).has_value());
    REQUIRE(book.apply(make_add(2, 2, protocol::Side::Buy, 100, 5)).has_value());
    // Step 3: a second, lower buy level.
    REQUIRE(book.apply(make_add(3, 3, protocol::Side::Buy, 99, 20)).has_value());
    // Step 4-5: two sell levels.
    REQUIRE(book.apply(make_add(4, 4, protocol::Side::Sell, 101, 8)).has_value());
    REQUIRE(book.apply(make_add(5, 5, protocol::Side::Sell, 102, 12)).has_value());

    {
        auto bba = book.best_bid_ask();
        REQUIRE(bba.bid == PriceLevelView{.price = 100, .total_quantity = 15, .order_count = 2});
        REQUIRE(bba.ask == PriceLevelView{.price = 101, .total_quantity = 8, .order_count = 1});
    }
    {
        auto snap = book.depth_snapshot(2);
        REQUIRE(snap.bids == std::vector<PriceLevelView>{
                                  {.price = 100, .total_quantity = 15, .order_count = 2},
                                  {.price = 99, .total_quantity = 20, .order_count = 1},
                              });
        REQUIRE(snap.asks == std::vector<PriceLevelView>{
                                  {.price = 101, .total_quantity = 8, .order_count = 1},
                                  {.price = 102, .total_quantity = 12, .order_count = 1},
                              });
    }

    // Step 6: cancel order 2 -> level 100 drops to just order 1 (qty 10).
    REQUIRE(book.apply(make_cancel(6, 2)).has_value());
    REQUIRE(book.best_bid_ask().bid == PriceLevelView{.price = 100, .total_quantity = 10, .order_count = 1});

    // Step 7: partially execute order 1 (10 -> 6). Order stays resting.
    REQUIRE(book.apply(make_execute(7, 1, 4)).has_value());
    REQUIRE(book.best_bid_ask().bid == PriceLevelView{.price = 100, .total_quantity = 6, .order_count = 1});

    // Step 8: fully execute the remaining 6 -> order 1 is removed, level 100
    // is now empty and disappears entirely, so the best bid falls through to 99.
    REQUIRE(book.apply(make_execute(8, 1, 6)).has_value());
    {
        auto bba = book.best_bid_ask();
        REQUIRE(bba.bid == PriceLevelView{.price = 99, .total_quantity = 20, .order_count = 1});
        REQUIRE(bba.ask == PriceLevelView{.price = 101, .total_quantity = 8, .order_count = 1});
    }

    // Step 9: replace order 4 (Sell 101 x8) with order 6 (Sell 103 x8).
    // Level 101 empties and disappears; level 103 is created; best ask
    // falls through to the untouched level 102.
    REQUIRE(book.apply(make_replace(9, 4, 6, 103, 8)).has_value());
    {
        auto bba = book.best_bid_ask();
        REQUIRE(bba.bid == PriceLevelView{.price = 99, .total_quantity = 20, .order_count = 1});
        REQUIRE(bba.ask == PriceLevelView{.price = 102, .total_quantity = 12, .order_count = 1});
    }
    {
        auto snap = book.depth_snapshot(2);
        REQUIRE(snap.bids == std::vector<PriceLevelView>{
                                  {.price = 99, .total_quantity = 20, .order_count = 1},
                              });
        REQUIRE(snap.asks == std::vector<PriceLevelView>{
                                  {.price = 102, .total_quantity = 12, .order_count = 1},
                                  {.price = 103, .total_quantity = 8, .order_count = 1},
                              });
    }
}

TEST_CASE("OrderBook rejects cancelling a non-existent order and leaves state unchanged", "[orderbook]") {
    OrderBook book;
    REQUIRE(book.apply(make_add(1, 1, protocol::Side::Buy, 100, 10)).has_value());

    auto result = book.apply(make_cancel(2, /*order_id=*/999));
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == ApplyError::OrderNotFound);

    // Untouched: order 1 is still resting exactly as it was.
    REQUIRE(book.best_bid_ask().bid == PriceLevelView{.price = 100, .total_quantity = 10, .order_count = 1});
}

TEST_CASE("OrderBook rejects executing more quantity than remains and leaves the order unchanged", "[orderbook]") {
    OrderBook book;
    REQUIRE(book.apply(make_add(1, 1, protocol::Side::Buy, 100, 20)).has_value());

    auto result = book.apply(make_execute(2, 1, 25));
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == ApplyError::ExecutedQuantityExceedsRemaining);

    // Untouched: the order's full 20 units are still resting.
    REQUIRE(book.best_bid_ask().bid == PriceLevelView{.price = 100, .total_quantity = 20, .order_count = 1});
}

TEST_CASE("OrderBook rejects an AddOrder that reuses a resting order_id", "[orderbook]") {
    OrderBook book;
    REQUIRE(book.apply(make_add(1, 1, protocol::Side::Buy, 100, 10)).has_value());

    auto result = book.apply(make_add(2, /*order_id=*/1, protocol::Side::Buy, 105, 3));
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == ApplyError::DuplicateOrderId);

    // Untouched: still one order resting at the original price/quantity.
    REQUIRE(book.best_bid_ask().bid == PriceLevelView{.price = 100, .total_quantity = 10, .order_count = 1});
}

TEST_CASE("OrderBook rejects a ReplaceOrder referencing an unknown old_order_id", "[orderbook]") {
    OrderBook book;

    auto result = book.apply(make_replace(1, /*old_id=*/999, /*new_id=*/1000, 100, 10));
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == ApplyError::OrderNotFound);

    auto bba = book.best_bid_ask();
    REQUIRE_FALSE(bba.bid.has_value());
    REQUIRE_FALSE(bba.ask.has_value());
}

TEST_CASE("OrderBook rejects a ReplaceOrder that introduces a duplicate new_order_id", "[orderbook]") {
    OrderBook book;
    REQUIRE(book.apply(make_add(1, 1, protocol::Side::Buy, 100, 10)).has_value());
    REQUIRE(book.apply(make_add(2, 2, protocol::Side::Buy, 99, 5)).has_value());

    // Try to replace order 1 with order_id 2, which is already resting.
    auto result = book.apply(make_replace(3, /*old_id=*/1, /*new_id=*/2, 101, 7));
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == ApplyError::DuplicateOrderId);

    // Untouched: both original orders are still resting exactly as they were.
    auto snap = book.depth_snapshot(2);
    REQUIRE(snap.bids == std::vector<PriceLevelView>{
                              {.price = 100, .total_quantity = 10, .order_count = 1},
                              {.price = 99, .total_quantity = 5, .order_count = 1},
                          });
}
