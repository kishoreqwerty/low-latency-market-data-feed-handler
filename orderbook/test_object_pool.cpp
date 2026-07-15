#include <list>
#include <memory>
#include <new>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "orderbook/object_pool.hpp"
#include "orderbook/order_book.hpp"
#include "orderbook/test_alloc_counter.hpp"
#include "protocol/message_types.hpp"

using namespace mdfh::orderbook;
using mdfh::test_util::delete_call_count;
using mdfh::test_util::new_call_count;
using mdfh::test_util::reset_alloc_counters;
namespace protocol = mdfh::protocol;

TEST_CASE("FixedBlockPool hands out distinct blocks up to capacity, then throws", "[object_pool]") {
    FixedBlockPool<sizeof(int), alignof(int), 4> pool;
    REQUIRE(pool.capacity() == 4);
    REQUIRE(pool.in_use() == 0);

    void* a = pool.allocate();
    void* b = pool.allocate();
    void* c = pool.allocate();
    void* d = pool.allocate();
    REQUIRE(pool.in_use() == 4);

    REQUIRE(a != b);
    REQUIRE(a != c);
    REQUIRE(a != d);
    REQUIRE(b != c);
    REQUIRE(b != d);
    REQUIRE(c != d);

    REQUIRE_THROWS_AS(pool.allocate(), std::bad_alloc);

    pool.deallocate(a);
    pool.deallocate(b);
    pool.deallocate(c);
    pool.deallocate(d);
}

TEST_CASE("FixedBlockPool reuses a block after it's freed", "[object_pool]") {
    FixedBlockPool<sizeof(int), alignof(int), 2> pool;
    void* a = pool.allocate();
    void* b = pool.allocate();
    REQUIRE(pool.in_use() == 2);

    pool.deallocate(a);
    REQUIRE(pool.in_use() == 1);

    void* c = pool.allocate();
    REQUIRE(pool.in_use() == 2);
    REQUIRE(c == a);  // freelist is LIFO: the just-freed block comes back first

    pool.deallocate(b);
    pool.deallocate(c);
}

TEST_CASE("PoolAllocator backs a std::list with zero heap allocations", "[object_pool]") {
    // Distinct capacity from other tests/OrderBook so this exercises its own
    // pool instance, independent of anything else in the binary.
    {
        // Warm-up: the first-ever use of PoolAllocator<int, 8> on this
        // thread's current shard slot lazily constructs that shard's Pool
        // via std::make_unique -- a genuine, one-time heap allocation
        // (this is what actually makes per-shard pools lock-free: a
        // process-wide static singleton lived in static storage with zero
        // allocation ever, but that's exactly the shared-across-threads
        // object this redesign eliminates). Same "startup cost is fine,
        // the hot path must be zero" pattern already used for
        // order_index_.reserve() below -- reset counters only after this
        // one-time cost has already happened.
        std::list<int, PoolAllocator<int, 8>> warm_up;
        warm_up.push_back(0);
    }

    reset_alloc_counters();
    {
        std::list<int, PoolAllocator<int, 8>> list;
        for (int i = 0; i < 8; ++i) {
            list.push_back(i);
        }
        list.clear();
        for (int i = 0; i < 8; ++i) {
            list.push_back(i);
        }
    }
    REQUIRE(new_call_count() == 0);
    REQUIRE(delete_call_count() == 0);
}

TEST_CASE("OrderBook (pool-backed) causes zero heap allocations for steady-state message traffic",
          "[object_pool]") {
    OrderBook book;  // construction reserves order_index_ up front -- allowed to allocate

    // Warm-up: prime a small working set of resting orders. Startup cost,
    // not the hot path, so this is allowed to touch the heap (it won't,
    // since order counts here are trivially under kShardOrderPoolCapacity,
    // but that's not what's being asserted in this block).
    constexpr protocol::Quantity kQty = 10;
    for (protocol::OrderId id = 1; id <= 100; ++id) {
        protocol::Side side   = (id % 2 == 0) ? protocol::Side::Buy : protocol::Side::Sell;
        protocol::Price price = 100 + static_cast<protocol::Price>(id % 10);
        REQUIRE(book.apply(protocol::AddOrder{
                                .seq_num   = id,
                                .order_id  = id,
                                .symbol    = protocol::make_symbol("POOL"),
                                .side      = side,
                                .price     = price,
                                .quantity  = kQty,
                                .timestamp = id,
                            })
                    .has_value());
    }

    // Steady state: cycle orders in and out via cancel+add. This is the hot
    // path, and per CLAUDE.md's "zero dynamic allocation in the hot path"
    // rule, it must not touch the heap at all.
    reset_alloc_counters();
    protocol::OrderId next_id = 101;
    for (protocol::OrderId id = 1; id <= 100; ++id) {
        REQUIRE(book.apply(protocol::CancelOrder{.seq_num = next_id, .order_id = id, .timestamp = next_id})
                    .has_value());

        protocol::Side side   = (next_id % 2 == 0) ? protocol::Side::Buy : protocol::Side::Sell;
        protocol::Price price = 100 + static_cast<protocol::Price>(next_id % 10);
        REQUIRE(book.apply(protocol::AddOrder{
                                .seq_num   = next_id,
                                .order_id  = next_id,
                                .symbol    = protocol::make_symbol("POOL"),
                                .side      = side,
                                .price     = price,
                                .quantity  = kQty,
                                .timestamp = next_id,
                            })
                    .has_value());
        ++next_id;
    }

    REQUIRE(new_call_count() == 0);
    REQUIRE(delete_call_count() == 0);
}

TEST_CASE("NaiveOrderBook (std::allocator) causes real heap allocations for the same traffic",
          "[object_pool]") {
    // Control case: same workload, same assertions inverted, proving the
    // zero-allocation result above is because of the pool and not because
    // the workload itself happens to avoid the heap.
    NaiveOrderBook book;

    constexpr protocol::Quantity kQty = 10;
    for (protocol::OrderId id = 1; id <= 100; ++id) {
        protocol::Side side   = (id % 2 == 0) ? protocol::Side::Buy : protocol::Side::Sell;
        protocol::Price price = 100 + static_cast<protocol::Price>(id % 10);
        REQUIRE(book.apply(protocol::AddOrder{
                                .seq_num   = id,
                                .order_id  = id,
                                .symbol    = protocol::make_symbol("NAIV"),
                                .side      = side,
                                .price     = price,
                                .quantity  = kQty,
                                .timestamp = id,
                            })
                    .has_value());
    }

    reset_alloc_counters();
    protocol::OrderId next_id = 101;
    for (protocol::OrderId id = 1; id <= 100; ++id) {
        REQUIRE(book.apply(protocol::CancelOrder{.seq_num = next_id, .order_id = id, .timestamp = next_id})
                    .has_value());

        protocol::Side side   = (next_id % 2 == 0) ? protocol::Side::Buy : protocol::Side::Sell;
        protocol::Price price = 100 + static_cast<protocol::Price>(next_id % 10);
        REQUIRE(book.apply(protocol::AddOrder{
                                .seq_num   = next_id,
                                .order_id  = next_id,
                                .symbol    = protocol::make_symbol("NAIV"),
                                .side      = side,
                                .price     = price,
                                .quantity  = kQty,
                                .timestamp = next_id,
                            })
                    .has_value());
        ++next_id;
    }

    REQUIRE(new_call_count() > 0);
    REQUIRE(delete_call_count() > 0);
}

namespace {

void fill_book_with_orders(OrderBook& book, protocol::OrderId& next_id, protocol::Quantity count) {
    for (protocol::Quantity i = 0; i < count; ++i) {
        protocol::Side side   = (i % 2 == 0) ? protocol::Side::Buy : protocol::Side::Sell;
        protocol::Price price = 100 + static_cast<protocol::Price>(i % 1000);
        auto result           = book.apply(protocol::AddOrder{
                      .seq_num   = next_id,
                      .order_id  = next_id,
                      .symbol    = protocol::make_symbol("SYM"),
                      .side      = side,
                      .price     = price,
                      .quantity  = 10,
                      .timestamp = next_id,
        });
        REQUIRE(result.has_value());
        ++next_id;
    }
}

}  // namespace

TEST_CASE("A single shard's pool capacity covers that shard's full expected symbol load",
          "[object_pool][sharding]") {
    // Phase 6 gives each DISTINCT SYMBOL its own OrderBook (see
    // io/shard_demux.hpp). Phase 7 gave each SHARD its own independent
    // pool (see object_pool.hpp) rather than one pool shared across every
    // shard -- so the capacity that matters is per shard, not system-wide.
    // This simulates the worst case order_book.hpp's kShardOrderPoolCapacity
    // is sized for: one shard hosting kExpectedSymbolsPerShard symbols,
    // each holding kExpectedRestingOrdersPerSymbol resting orders --
    // exactly at capacity. If it were still sized for the OLD system-wide
    // total (kMaxSymbols books), this would be off by roughly kMaxShards/2x
    // too generous per shard; sized too small, this test would throw
    // std::bad_alloc partway through.
    //
    // No set_current_shard_index() call here: single-threaded tests all
    // share pool slot 0 by default, which this test uses as "some shard."
    std::vector<std::unique_ptr<OrderBook>> books;
    books.reserve(kExpectedSymbolsPerShard);

    protocol::OrderId next_id = 1;
    for (std::size_t s = 0; s < kExpectedSymbolsPerShard; ++s) {
        books.push_back(std::make_unique<OrderBook>());
        fill_book_with_orders(*books.back(), next_id, static_cast<protocol::Quantity>(kExpectedRestingOrdersPerSymbol));
    }

    // Not just "didn't throw" -- spot-check a few books actually hold
    // correct, live state under this full per-shard load.
    REQUIRE(books.front()->best_bid_ask().bid.has_value());
    REQUIRE(books[kExpectedSymbolsPerShard / 2]->best_bid_ask().bid.has_value());
    REQUIRE(books.back()->best_bid_ask().bid.has_value());
}

TEST_CASE("Different shards' pools are genuinely independent instances, not shared capacity",
          "[object_pool][sharding]") {
    // Behavioral proof, not just inspecting a counter: fill TWO DIFFERENT
    // shards' pools, each independently, to just under their own full
    // per-shard capacity, AT THE SAME TIME (both sets of books alive
    // simultaneously). If the two shards secretly drew from one shared
    // pool sized for a single shard's capacity, this would exceed it and
    // throw partway through the second shard -- exactly the Phase 7 bug
    // this redesign fixes. Using shard indices 14/15 (rather than 0)
    // specifically to avoid any dependence on ordering versus other tests
    // in this file, which all implicitly use the default slot 0.
    constexpr std::size_t kOrdersEach = kShardOrderPoolCapacity - 100;  // just under, headroom for map/hashmap nodes

    set_current_shard_index(14);
    OrderBook shard_a_book;
    protocol::OrderId id_a = 1;
    fill_book_with_orders(shard_a_book, id_a, static_cast<protocol::Quantity>(kOrdersEach));

    set_current_shard_index(15);
    OrderBook shard_b_book;
    protocol::OrderId id_b = 10'000'000;  // distinct range, avoids any cross-shard order_id collision
    fill_book_with_orders(shard_b_book, id_b, static_cast<protocol::Quantity>(kOrdersEach));

    // Both books, still alive simultaneously, both genuinely populated --
    // proof neither pool starved the other.
    REQUIRE(shard_a_book.best_bid_ask().bid.has_value());
    REQUIRE(shard_b_book.best_bid_ask().bid.has_value());

    set_current_shard_index(0);  // restore the default for any tests that run after this one
}
