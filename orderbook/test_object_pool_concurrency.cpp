// Dedicated concurrency verification for the per-shard pool redesign: each
// thread exclusively owns one shard index (via set_current_shard_index),
// so its PoolAllocator calls only ever touch that shard's own, lazily
// created FixedBlockPool instances -- no other thread ever touches the
// same memory, and object_pool.hpp has NO mutex anywhere (removed
// entirely, not just relaxed). This is the actual claim to verify: run it
// under debug-tsan and confirm zero reported races. Kept in its own
// executable, same reasoning as mdfh_spsc_stress_test in Phase 5 and
// mdfh_pipeline_e2e_test in Phase 7: it spins up real OS threads and needs
// to be run/verified specifically under debug-tsan, separately from the
// fast single-threaded unit tests in test_object_pool.cpp.

#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "orderbook/object_pool.hpp"
#include "orderbook/order_book.hpp"
#include "protocol/message_types.hpp"

using namespace mdfh::orderbook;
namespace protocol = mdfh::protocol;

namespace {

// Deliberately does NOT use Catch2 assertion macros -- REQUIRE et al. are
// not documented as safe to call concurrently from multiple threads, since
// Catch2's result-reporting machinery isn't built for that. Failures are
// instead recorded into a shared atomic<bool> and asserted once, on the
// main test thread, after every worker has joined.
void run_shard_workload(std::size_t shard_index, int ops_per_thread, std::atomic<bool>& all_ok) {
    set_current_shard_index(shard_index);
    OrderBook book;

    constexpr int kWorkingSetSize = 200;
    std::vector<protocol::OrderId> resting;
    resting.reserve(kWorkingSetSize);

    // Disjoint order_id ranges per thread -- not required for pool
    // correctness (each thread's pool access is already fully independent),
    // just keeps the workload easy to reason about if something does fail.
    protocol::OrderId next_id = static_cast<protocol::OrderId>(shard_index) * 10'000'000ULL + 1;

    auto check = [&](auto result) {
        if (!result.has_value()) {
            all_ok.store(false, std::memory_order_relaxed);
        }
    };

    for (int i = 0; i < kWorkingSetSize; ++i) {
        protocol::Side side   = (i % 2 == 0) ? protocol::Side::Buy : protocol::Side::Sell;
        protocol::Price price = 100 + static_cast<protocol::Price>(i % 50);
        check(book.apply(protocol::AddOrder{
            .seq_num = next_id, .order_id = next_id, .symbol = protocol::make_symbol("CONC"), .side = side,
            .price = price, .quantity = 10, .timestamp = next_id}));
        resting.push_back(next_id);
        ++next_id;
    }

    for (int i = 0; i < ops_per_thread; ++i) {
        const std::size_t slot         = static_cast<std::size_t>(i) % resting.size();
        const protocol::OrderId old_id = resting[slot];

        if (i % 2 == 0) {
            check(book.apply(protocol::CancelOrder{.seq_num = next_id, .order_id = old_id, .timestamp = next_id}));
        } else {
            check(book.apply(protocol::ExecuteOrder{
                .seq_num = next_id, .order_id = old_id, .executed_quantity = 10, .timestamp = next_id}));
        }
        ++next_id;

        protocol::Side side   = (slot % 2 == 0) ? protocol::Side::Buy : protocol::Side::Sell;
        protocol::Price price = 100 + static_cast<protocol::Price>(slot % 50);
        check(book.apply(protocol::AddOrder{
            .seq_num = next_id, .order_id = next_id, .symbol = protocol::make_symbol("CONC"), .side = side,
            .price = price, .quantity = 10, .timestamp = next_id}));
        resting[slot] = next_id;
        ++next_id;
    }
}

}  // namespace

TEST_CASE("Per-shard pools need no synchronization: many threads hammering their own shard's pool concurrently",
          "[object_pool][concurrency]") {
    constexpr std::size_t kNumThreads = 8;
    constexpr int kOpsPerThread       = 20'000;

    std::atomic<bool> all_ok{true};
    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (std::size_t s = 0; s < kNumThreads; ++s) {
        threads.emplace_back(run_shard_workload, s, kOpsPerThread, std::ref(all_ok));
    }
    for (auto& t : threads) {
        t.join();
    }

    REQUIRE(all_ok.load());
}
