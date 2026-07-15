#include <array>
#include <cstddef>
#include <unordered_map>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "io/shard_demux.hpp"
#include "protocol/encoder.hpp"
#include "protocol/message_types.hpp"

using namespace mdfh::io;
namespace protocol  = mdfh::protocol;
namespace orderbook = mdfh::orderbook;

namespace {

std::vector<std::byte> encode_add(protocol::SeqNum seq, protocol::OrderId id, protocol::Symbol symbol,
                                   protocol::Side side, protocol::Price price, protocol::Quantity qty) {
    protocol::AddOrder msg{
        .seq_num = seq, .order_id = id, .symbol = symbol, .side = side, .price = price, .quantity = qty,
        .timestamp = seq};
    std::vector<std::byte> buf(protocol::AddOrder::kWireSize);
    REQUIRE(protocol::encode(msg, buf).has_value());
    return buf;
}

std::vector<std::byte> encode_cancel(protocol::SeqNum seq, protocol::OrderId id) {
    protocol::CancelOrder msg{.seq_num = seq, .order_id = id, .timestamp = seq};
    std::vector<std::byte> buf(protocol::CancelOrder::kWireSize);
    REQUIRE(protocol::encode(msg, buf).has_value());
    return buf;
}

std::vector<std::byte> encode_execute(protocol::SeqNum seq, protocol::OrderId id, protocol::Quantity qty) {
    protocol::ExecuteOrder msg{.seq_num = seq, .order_id = id, .executed_quantity = qty, .timestamp = seq};
    std::vector<std::byte> buf(protocol::ExecuteOrder::kWireSize);
    REQUIRE(protocol::encode(msg, buf).has_value());
    return buf;
}

std::vector<std::byte> encode_replace(protocol::SeqNum seq, protocol::OrderId old_id, protocol::OrderId new_id,
                                       protocol::Price price, protocol::Quantity qty) {
    protocol::ReplaceOrder msg{
        .seq_num = seq, .old_order_id = old_id, .new_order_id = new_id, .price = price, .quantity = qty,
        .timestamp = seq};
    std::vector<std::byte> buf(protocol::ReplaceOrder::kWireSize);
    REQUIRE(protocol::encode(msg, buf).has_value());
    return buf;
}

}  // namespace

TEST_CASE("shard_for_symbol is deterministic and stays within bounds", "[shard_demux]") {
    protocol::Symbol aapl = protocol::make_symbol("AAPL");
    std::size_t first     = shard_for_symbol(aapl, 8);
    for (int i = 0; i < 5; ++i) {
        REQUIRE(shard_for_symbol(aapl, 8) == first);
    }
    REQUIRE(first < 8);

    for (const char* s : {"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NFLX", "META", "NVDA"}) {
        REQUIRE(shard_for_symbol(protocol::make_symbol(s), 4) < 4);
    }
}

TEST_CASE("ShardedPipeline rejects an unroutable order_id instead of guessing a shard", "[shard_demux]") {
    ShardedPipeline pipeline(2);
    auto bytes  = encode_cancel(1, /*order_id=*/999);  // never added
    auto result = pipeline.demux(bytes);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == DemuxError::UnroutableOrderId);
}

TEST_CASE("Mixed-symbol stream: each symbol's book ends up correct and isolated from every other symbol",
          "[shard_demux][integration]") {
    // 4 symbols over 3 shards guarantees (pigeonhole) at least one shard
    // hosts more than one symbol -- exactly the case that would silently
    // merge books together if Shard held one flat OrderBook instead of a
    // symbol-keyed map of them.
    ShardedPipeline pipeline(3);

    const protocol::Symbol aapl = protocol::make_symbol("AAPL");
    const protocol::Symbol msft = protocol::make_symbol("MSFT");
    const protocol::Symbol goog = protocol::make_symbol("GOOG");
    const protocol::Symbol amzn = protocol::make_symbol("AMZN");

    const std::size_t shard_aapl = pipeline.shard_for_symbol(aapl);
    const std::size_t shard_msft = pipeline.shard_for_symbol(msft);
    const std::size_t shard_goog = pipeline.shard_for_symbol(goog);
    const std::size_t shard_amzn = pipeline.shard_for_symbol(amzn);

    // Per-shard sequence counters -- architecture_spec.md Section 3 defines
    // seq_num at shard granularity, not per symbol, so messages sharing a
    // shard must share one contiguous sequence regardless of which symbol
    // they're for.
    std::unordered_map<std::size_t, protocol::SeqNum> next_seq;
    auto seq_for = [&](std::size_t shard) { return ++next_seq[shard]; };

    // seq_num=1 on shard_aapl: AddOrder(1, AAPL, Buy, 100 x10)
    auto m1 = encode_add(seq_for(shard_aapl), 1, aapl, protocol::Side::Buy, 100, 10);
    // seq_num=1 on shard_msft: AddOrder(2, MSFT, Buy, 200 x20)
    auto m2 = encode_add(seq_for(shard_msft), 2, msft, protocol::Side::Buy, 200, 20);
    // seq_num=2 on shard_aapl: AddOrder(3, AAPL, Sell, 105 x5)
    auto m3 = encode_add(seq_for(shard_aapl), 3, aapl, protocol::Side::Sell, 105, 5);
    // seq_num=1 on shard_goog: AddOrder(4, GOOG, Buy, 300 x15)
    auto m4 = encode_add(seq_for(shard_goog), 4, goog, protocol::Side::Buy, 300, 15);

    // Deliberate gap: shard_msft's sequence jumps from 1 straight to 3,
    // skipping 2, so ONLY shard_msft's gap detector should ever go STALE.
    next_seq[shard_msft] += 2;
    auto m5 = encode_cancel(next_seq[shard_msft], /*order_id=*/2);  // seq_num=3 on shard_msft

    // seq_num=3 on shard_aapl: ExecuteOrder(1, executed_quantity=4) -- partial fill
    auto m6 = encode_execute(seq_for(shard_aapl), 1, 4);
    // seq_num=4 on shard_msft: AddOrder(5, MSFT, Sell, 210 x8)
    auto m7 = encode_add(seq_for(shard_msft), 5, msft, protocol::Side::Sell, 210, 8);
    // seq_num=2 on shard_goog: ReplaceOrder(old=4, new=6, price=301, qty=15)
    auto m8 = encode_replace(seq_for(shard_goog), 4, 6, 301, 15);
    // seq_num=1 on shard_amzn: AddOrder(7, AMZN, Buy, 400 x25)
    auto m9 = encode_add(seq_for(shard_amzn), 7, amzn, protocol::Side::Buy, 400, 25);

    // Feed everything through demux() interleaved (not grouped by symbol),
    // simulating a real multiplexed feed, and confirm each message actually
    // routes to the shard we computed above.
    struct Expected {
        const std::vector<std::byte>* bytes;
        std::size_t shard;
    };
    for (const Expected& e : {Expected{&m1, shard_aapl}, Expected{&m2, shard_msft}, Expected{&m3, shard_aapl},
                               Expected{&m4, shard_goog}, Expected{&m5, shard_msft}, Expected{&m6, shard_aapl},
                               Expected{&m7, shard_msft}, Expected{&m8, shard_goog}, Expected{&m9, shard_amzn}}) {
        auto result = pipeline.demux(*e.bytes);
        REQUIRE(result.has_value());
        REQUIRE(*result == e.shard);
    }

    pipeline.process_pending();

    // --- Per-symbol book correctness, hand-computed ---

    const auto& aapl_book = pipeline.shard(shard_aapl).books.at(aapl);
    {
        auto bba = aapl_book.best_bid_ask();
        REQUIRE(bba.bid == orderbook::PriceLevelView{.price = 100, .total_quantity = 6, .order_count = 1});
        REQUIRE(bba.ask == orderbook::PriceLevelView{.price = 105, .total_quantity = 5, .order_count = 1});
    }

    const auto& msft_book = pipeline.shard(shard_msft).books.at(msft);
    {
        auto bba = msft_book.best_bid_ask();
        REQUIRE_FALSE(bba.bid.has_value());  // order 2 (the only bid) was cancelled
        REQUIRE(bba.ask == orderbook::PriceLevelView{.price = 210, .total_quantity = 8, .order_count = 1});
    }

    const auto& goog_book = pipeline.shard(shard_goog).books.at(goog);
    {
        auto bba = goog_book.best_bid_ask();
        REQUIRE(bba.bid == orderbook::PriceLevelView{.price = 301, .total_quantity = 15, .order_count = 1});
        REQUIRE_FALSE(bba.ask.has_value());
    }

    const auto& amzn_book = pipeline.shard(shard_amzn).books.at(amzn);
    {
        auto bba = amzn_book.best_bid_ask();
        REQUIRE(bba.bid == orderbook::PriceLevelView{.price = 400, .total_quantity = 25, .order_count = 1});
        REQUIRE_FALSE(bba.ask.has_value());
    }

    // --- Cross-symbol isolation: no symbol's price ever leaks into another
    // symbol's book, whether or not they share a physical shard. This is
    // the real correctness bar for this phase, not just "each symbol
    // works when checked alone" above. ---
    struct SymbolCheck {
        const orderbook::OrderBook* book;
        std::size_t shard;
        protocol::Price telltale_price;  // appears ONLY in this symbol's book
    };
    const std::vector<SymbolCheck> checks = {
        {&aapl_book, shard_aapl, 100},
        {&msft_book, shard_msft, 210},
        {&goog_book, shard_goog, 301},
        {&amzn_book, shard_amzn, 400},
    };

    auto contains_price = [](const orderbook::OrderBook& book, protocol::Price price) {
        auto snap = book.depth_snapshot(10);
        for (const auto& lvl : snap.bids) {
            if (lvl.price == price) return true;
        }
        for (const auto& lvl : snap.asks) {
            if (lvl.price == price) return true;
        }
        return false;
    };

    for (const auto& mine : checks) {
        REQUIRE(contains_price(*mine.book, mine.telltale_price));
        for (const auto& other : checks) {
            if (&other == &mine) continue;
            REQUIRE_FALSE(contains_price(*mine.book, other.telltale_price));
        }
    }

    // If two symbols happen to share a physical shard, their books must
    // still be genuinely distinct OrderBook instances, not the same one.
    for (std::size_t i = 0; i < checks.size(); ++i) {
        for (std::size_t j = i + 1; j < checks.size(); ++j) {
            if (checks[i].shard == checks[j].shard) {
                REQUIRE(checks[i].book != checks[j].book);
            }
        }
    }

    // --- Gap detector isolation: the deliberate gap on shard_msft must
    // mark ONLY that shard STALE. Any OTHER shard -- including one that
    // happens to also host one of our other symbols -- must remain LIVE
    // with zero recorded gaps. ---
    for (std::size_t s = 0; s < pipeline.num_shards(); ++s) {
        const auto& gd = pipeline.shard(s).gap_detector;
        if (s == shard_msft) {
            REQUIRE(gd.is_stale());
            REQUIRE(gd.sequence_gaps_total() == 1);
        } else {
            REQUIRE_FALSE(gd.is_stale());
            REQUIRE(gd.sequence_gaps_total() == 0);
        }
    }
}

TEST_CASE("ShardedPipeline's order routing table stays bounded over a long add/cancel/execute run",
          "[shard_demux]") {
    // Cancel and Replace's old_order_id are cleaned up at demux() time (the
    // wire message alone guarantees the order is gone/renamed), but a full
    // ExecuteOrder fill can only be recognized after the book applies it
    // (see ShardedPipeline::process_pending()). This exercises BOTH removal
    // paths, cycling far more orders through than the routing table should
    // ever hold onto at once, and checks its size after every single
    // cycle -- not just at the end -- so a leak on either path would show
    // up immediately as the count climbing past the working-set size.
    ShardedPipeline pipeline(1);
    const protocol::Symbol sym = protocol::make_symbol("CYCLE");

    constexpr int kWorkingSetSize = 50;    // orders resting at any given time
    constexpr int kTotalCycles    = 2000;  // far more than the working set

    protocol::SeqNum seq      = 0;
    protocol::OrderId next_id = 1;
    std::vector<protocol::OrderId> resting;
    resting.reserve(kWorkingSetSize);

    auto push_add = [&](protocol::OrderId id) {
        auto bytes = encode_add(++seq, id, sym, protocol::Side::Buy, 100, 10);
        REQUIRE(pipeline.demux(bytes).has_value());
    };
    auto push_cancel = [&](protocol::OrderId id) {
        auto bytes = encode_cancel(++seq, id);
        REQUIRE(pipeline.demux(bytes).has_value());
    };
    auto push_execute_full = [&](protocol::OrderId id) {
        auto bytes = encode_execute(++seq, id, /*executed_quantity=*/10);  // full fill: quantity was 10
        REQUIRE(pipeline.demux(bytes).has_value());
    };

    // Prime the working set.
    for (int i = 0; i < kWorkingSetSize; ++i) {
        push_add(next_id);
        resting.push_back(next_id);
        ++next_id;
    }
    pipeline.process_pending();
    REQUIRE(pipeline.routing_table_size() == static_cast<std::size_t>(kWorkingSetSize));

    for (int cycle = 0; cycle < kTotalCycles; ++cycle) {
        const std::size_t slot          = static_cast<std::size_t>(cycle) % resting.size();
        const protocol::OrderId old_id  = resting[slot];

        // Alternate how each resting order leaves, exercising both cleanup
        // paths (demux-time for cancel, process_pending-time for a full
        // execute) roughly equally.
        if (cycle % 2 == 0) {
            push_cancel(old_id);
        } else {
            push_execute_full(old_id);
        }
        push_add(next_id);
        resting[slot] = next_id;
        ++next_id;

        pipeline.process_pending();

        REQUIRE(pipeline.routing_table_size() == static_cast<std::size_t>(kWorkingSetSize));
    }
}
