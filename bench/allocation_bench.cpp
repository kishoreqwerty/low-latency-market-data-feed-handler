// Benchmarks the effect of Phase 3's pool allocator on the order book by
// running the identical OrderBookT logic (see orderbook/order_book.hpp)
// through a realistic message volume twice: once backed by the pool
// (OrderBook) and once backed by plain std::allocator (NaiveOrderBook).
//
// Reports both wall-clock time and real heap-allocation call counts (via
// mdfh::test_util's global operator new/delete override) for the identical
// steady-state workload, so the comparison isn't just timing noise -- it's
// tied to an actual, measured allocation count difference.

#include <chrono>
#include <cstdio>
#include <vector>

#include "orderbook/order_book.hpp"
#include "orderbook/test_alloc_counter.hpp"
#include "protocol/message_types.hpp"

namespace protocol = mdfh::protocol;
using namespace mdfh::orderbook;
using mdfh::test_util::delete_call_count;
using mdfh::test_util::new_call_count;
using mdfh::test_util::reset_alloc_counters;

namespace {

constexpr std::size_t kWorkingSetSize  = 20'000;     // orders resting at any given time
constexpr std::size_t kOrdersProcessed = 2'000'000;  // Add+remove cycles run through the book
constexpr std::size_t kNumPriceLevels  = 50;         // distinct price levels per side

protocol::AddOrder make_add(protocol::OrderId id, protocol::Side side, protocol::Price price,
                             protocol::Quantity qty) {
    return protocol::AddOrder{
        .seq_num   = id,
        .order_id  = id,
        .symbol    = protocol::make_symbol("BENCH"),
        .side      = side,
        .price     = price,
        .quantity  = qty,
        .timestamp = id,
    };
}

protocol::CancelOrder make_cancel(protocol::OrderId id) {
    return protocol::CancelOrder{.seq_num = id, .order_id = id, .timestamp = id};
}

protocol::ExecuteOrder make_execute(protocol::OrderId id, protocol::Quantity qty) {
    return protocol::ExecuteOrder{.seq_num = id, .order_id = id, .executed_quantity = qty, .timestamp = id};
}

// Fills `book` with kWorkingSetSize resting orders. Startup cost -- run
// before the timed/counted region, not as part of it.
template <typename Book>
std::vector<protocol::OrderId> prime_working_set(Book& book) {
    std::vector<protocol::OrderId> resting;
    resting.reserve(kWorkingSetSize);
    for (std::size_t i = 0; i < kWorkingSetSize; ++i) {
        protocol::Side side   = (i % 2 == 0) ? protocol::Side::Buy : protocol::Side::Sell;
        protocol::Price price = 10'000 + static_cast<protocol::Price>(i % kNumPriceLevels);
        auto id                = static_cast<protocol::OrderId>(i + 1);
        book.apply(make_add(id, side, price, 100));
        resting.push_back(id);
    }
    return resting;
}

// Runs kOrdersProcessed deterministic Add+(Cancel|Execute) cycles, keeping
// the resting count pinned at kWorkingSetSize throughout -- this is the hot
// path being measured. Alternates cancel/execute so both removal paths (and
// both node-freeing branches in remove_resting_order) get exercised.
template <typename Book>
std::chrono::nanoseconds run_steady_state(Book& book, std::vector<protocol::OrderId>& resting) {
    auto next_id = static_cast<protocol::OrderId>(resting.size() + 1);

    const auto start = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < kOrdersProcessed; ++i) {
        const std::size_t slot         = i % resting.size();
        const protocol::OrderId old_id = resting[slot];

        if (i % 2 == 0) {
            book.apply(make_cancel(old_id));
        } else {
            book.apply(make_execute(old_id, 100));
        }

        protocol::Side side   = (slot % 2 == 0) ? protocol::Side::Buy : protocol::Side::Sell;
        protocol::Price price = 10'000 + static_cast<protocol::Price>(slot % kNumPriceLevels);
        book.apply(make_add(next_id, side, price, 100));
        resting[slot] = next_id;
        ++next_id;
    }
    const auto end = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
}

template <typename Book>
void report(const char* label) {
    Book book;
    auto resting = prime_working_set(book);

    reset_alloc_counters();
    auto elapsed = run_steady_state(book, resting);
    auto news    = new_call_count();
    auto deletes = delete_call_count();

    const double ms  = std::chrono::duration<double, std::milli>(elapsed).count();
    const double ns_per_msg = static_cast<double>(elapsed.count()) / static_cast<double>(kOrdersProcessed * 2);

    std::printf("%-28s %12.2f %14.1f %16zu %16zu\n", label, ms, ns_per_msg, news, deletes);
}

}  // namespace

int main() {
    std::printf(
        "Allocation benchmark: %zu Add+remove cycles (%zu messages total), "
        "%zu concurrently resting orders, %zu price levels/side\n\n",
        kOrdersProcessed, kOrdersProcessed * 2, kWorkingSetSize, kNumPriceLevels);
    std::printf("%-28s %12s %14s %16s %16s\n", "Variant", "Time (ms)", "ns/message", "new() calls",
                "delete() calls");

    report<OrderBook>("Pool-backed (OrderBook)");
    report<NaiveOrderBook>("Naive (std::allocator)");

    return 0;
}
