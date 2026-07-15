// End-to-end test for build_guide.md Phase 7's "Done when": the full
// pipeline running against a live (simulated) feed, with real per-shard
// decoder threads, correctly handling both normal traffic and injected
// packet loss.

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "io/pipeline_runner.hpp"
#include "protocol/message_types.hpp"

using namespace mdfh::io;
namespace protocol = mdfh::protocol;

namespace {

std::vector<protocol::Symbol> eight_symbols() {
    return {
        protocol::make_symbol("AAPL"), protocol::make_symbol("MSFT"), protocol::make_symbol("GOOG"),
        protocol::make_symbol("AMZN"), protocol::make_symbol("TSLA"), protocol::make_symbol("NFLX"),
        protocol::make_symbol("META"), protocol::make_symbol("NVDA"),
    };
}

}  // namespace

TEST_CASE(
    "Full pipeline end-to-end: real concurrent threads, live packet loss, gap detection observed while "
    "other shards are actively processing",
    "[pipeline_e2e]") {
    constexpr std::size_t kNumShards = 4;

    // High enough volume that the run takes a meaningfully observable
    // amount of wall-clock time even in the fully-optimized benchmark
    // preset (no sanitizer instrumentation slowing anything down). An
    // earlier version of this test used 40,000 messages and a 200us poll
    // interval; it passed reliably alone or under debug-tsan (where
    // instrumentation overhead stretches the run to ~600ms), but was
    // genuinely flaky under `ctest` running the full suite in the fast
    // benchmark/debug presets, where the whole run could finish in low
    // single-digit milliseconds -- faster than the poll loop below could
    // reliably sample. That was a test-timing bug, not a production one:
    // the underlying claim (gap detection happens while other shards are
    // still concurrently processing) was still true, just too brief a
    // window for a coarse poll to reliably catch. 500,000 messages at 1%
    // loss guarantees thousands of gap events spread across the whole run,
    // and a tighter poll interval gives many more samples during it.
    FeedGenerator::Config feed_config{
        .symbols          = eight_symbols(),
        .num_shards       = kNumShards,
        .message_count    = 500'000,
        .packet_loss_rate = 0.01,
        .seed             = 777,
    };

    PipelineRunner runner(feed_config, kNumShards);
    runner.start();

    // Poll WHILE the pipeline is actively running (before join()) to catch
    // a shard going STALE live. This is the crux of what this test needs
    // to prove that Phase 6's single-threaded test structurally could not:
    // the gap must be observed while the producer is still concurrently
    // feeding OTHER shards, not "generate all the loss, then process
    // everything afterward" sequentially.
    bool observed_live_stale = false;
    const auto deadline      = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        if (runner.producer_done()) {
            break;  // finished before we caught it live -- loop below still checks the final state
        }
        for (std::size_t s = 0; s < kNumShards; ++s) {
            if (runner.pipeline().shard(s).gap_detector.is_stale()) {
                observed_live_stale = true;
                break;
            }
        }
        if (observed_live_stale) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(20));
    }

    runner.join();  // wait for full natural completion regardless of what the poll loop found

    REQUIRE(observed_live_stale);  // the key assertion: caught mid-flight, not only after the fact

    REQUIRE(runner.feed().total_dropped() > 0);  // real loss actually happened

    std::size_t shards_with_gaps = 0;
    for (std::size_t s = 0; s < kNumShards; ++s) {
        if (runner.pipeline().shard(s).gap_detector.sequence_gaps_total() > 0) {
            ++shards_with_gaps;
        }
    }
    REQUIRE(shards_with_gaps > 0);

    // Confirm other shards made real, independent progress too -- this
    // wasn't a degenerate run where one shard did everything and the rest
    // sat idle. With 8 symbols spread over 4 shards, every shard should
    // have received traffic; check most of them did.
    std::size_t shards_with_books = 0;
    for (std::size_t s = 0; s < kNumShards; ++s) {
        if (!runner.pipeline().shard(s).books.empty()) {
            ++shards_with_books;
        }
    }
    REQUIRE(shards_with_books >= 2);

    // Demonstrate resync clears STALE. Called here, after join(), by
    // design: GapDetector's documented contract (see gap_detector.hpp) is
    // safe for concurrent reads while one thread writes via observe(), but
    // NOT safe for observe() and resync() to run concurrently from
    // different threads -- and by this point every shard thread has
    // already stopped calling observe(), so there's no concurrent writer
    // left to race with.
    for (std::size_t s = 0; s < kNumShards; ++s) {
        auto& gd = runner.pipeline().shard(s).gap_detector;
        if (gd.is_stale()) {
            auto last = gd.last_seq_num();
            REQUIRE(last.has_value());
            gd.resync(*last + 1000);  // simulate a snapshot/replay landing well past the gap
            REQUIRE_FALSE(gd.is_stale());
        }
    }
}

TEST_CASE("Stopping the pipeline mid-flight joins promptly without hanging", "[pipeline_e2e]") {
    constexpr std::size_t kNumShards = 3;

    FeedGenerator::Config feed_config{
        .symbols          = {protocol::make_symbol("AAPL"), protocol::make_symbol("MSFT")},
        .num_shards       = kNumShards,
        .message_count    = 1'000'000,  // deliberately huge -- we stop it long before this
        .packet_loss_rate = 0.01,
        .seed             = 555,
    };

    PipelineRunner runner(feed_config, kNumShards);
    runner.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(20));  // let it run for a bit
    REQUIRE_FALSE(runner.producer_done());                       // sanity: genuinely still mid-flight

    runner.stop();  // must return promptly, not hang waiting for 1,000,000 messages

    REQUIRE(runner.producer_done());
    REQUIRE(runner.feed().total_generated() < 1'000'000);  // proves it stopped early, not that it just finished
}

TEST_CASE("A PipelineRunner destroyed without an explicit stop() still shuts down cleanly", "[pipeline_e2e]") {
    constexpr std::size_t kNumShards = 2;

    FeedGenerator::Config feed_config{
        .symbols          = {protocol::make_symbol("AAPL")},
        .num_shards       = kNumShards,
        .message_count    = 1'000'000,
        .packet_loss_rate = 0.0,
        .seed             = 999,
    };

    {
        PipelineRunner runner(feed_config, kNumShards);
        runner.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        // No stop()/join() call here -- the destructor has to handle it.
    }
    // Reaching this line without hanging IS the assertion: the destructor's
    // implicit stop()+join() worked. Nothing left to check afterward.
    SUCCEED("PipelineRunner destructor cleaned up without hanging");
}
