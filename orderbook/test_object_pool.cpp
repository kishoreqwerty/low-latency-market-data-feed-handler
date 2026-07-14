#include <list>
#include <new>

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
    // pool singleton, independent of anything else in the binary.
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
    // since order counts here are trivially under kMaxRestingOrdersPerBook,
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
