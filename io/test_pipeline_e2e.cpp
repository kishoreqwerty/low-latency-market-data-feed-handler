// End-to-end test for build_guide.md Phase 7's "Done when": the full
// pipeline running against a live (simulated) feed, with real per-shard
// decoder threads, correctly handling both normal traffic and injected
// packet loss.

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <thread>
#include <tuple>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "io/pipeline_runner.hpp"
#include "protocol/message_types.hpp"
#include "publisher/publisher.hpp"

using namespace mdfh::io;
namespace protocol  = mdfh::protocol;
namespace publisher = mdfh::publisher;

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

TEST_CASE("Repeated PipelineRunner construct/run/destroy cycles in one process don't leak pool capacity",
          "[pipeline_e2e][object_pool]") {
    // Regression test for a real bug found while building Phase 8's
    // pinning benchmark: ShardedPipeline's destructor (io/shard_demux.cpp)
    // used to destroy every shard's OrderBooks with current_shard_index
    // stuck at whatever the destroying thread's default was (0) instead of
    // each shard's own index -- see ~ShardedPipeline()'s comment for the
    // full mechanism. Freed nodes silently landed in the WRONG shard's
    // pool instead of their own: shards other than 0 leaked (their pools'
    // in_use never decremented across runs) while shard 0's pool got
    // corrupted with blocks that didn't belong to its storage array.
    //
    // Any SINGLE PipelineRunner run stayed safely under
    // orderbook::kShardOrderPoolCapacity, so this never surfaced in any
    // prior test -- it only showed up when something repeatedly
    // constructed and destroyed PipelineRunner within one process, which
    // is exactly what bench/pinning_bench.cpp does (multiple timed runs
    // in a single benchmark binary): each run's leaked growth accumulated
    // on top of the last until a shard's pool threw std::bad_alloc after
    // roughly a dozen runs. This test reruns that exact pattern, with
    // enough iterations to have reliably crashed pre-fix, as a standing
    // regression guard -- object_pool.hpp's debug-mode deallocate()
    // assertion (also added alongside this fix) would additionally fail
    // loudly under the `debug`/`debug-tsan` presets if this regresses.
    constexpr std::size_t kNumShards  = 4;
    constexpr int kIterations         = 30;
    constexpr std::size_t kMsgPerIter = 50'000;

    for (int iter = 0; iter < kIterations; ++iter) {
        FeedGenerator::Config feed_config{
            .symbols          = eight_symbols(),
            .num_shards       = kNumShards,
            .message_count    = kMsgPerIter,
            .packet_loss_rate = 0.0,
            .seed             = static_cast<std::uint64_t>(2000 + iter),
        };
        PipelineRunner runner(feed_config, kNumShards);
        runner.start();
        runner.join();
        // Destructor runs here, at the end of this iteration -- exactly
        // the moment the original bug corrupted/leaked pool state.
    }

    SUCCEED("completed repeated construct/run/destroy cycles without pool exhaustion");
}

namespace {

// Records every delta a shard's publisher thread sees, in the order it
// sees them -- which is the order that shard's decoder thread applied the
// corresponding input messages, since Shard::output_queue is SPSC with
// that decoder as its sole producer (see io/shard_demux.cpp).
struct RecordingSink : publisher::DeltaSink {
    std::vector<publisher::BookDelta> deltas;
    void publish(const publisher::BookDelta& delta) override { deltas.push_back(delta); }
};

}  // namespace

TEST_CASE(
    "Publisher output: replaying a shard's recorded book deltas in order reconstructs the exact same "
    "price-level state as the live book -- Phase 9's Done-when criterion",
    "[pipeline_e2e][publisher]") {
    constexpr std::size_t kNumShards = 2;

    // Loss disabled: this test is checking that deltas are a complete,
    // correct record of what apply() did, not re-litigating gap-driven
    // divergence (already Phase 6/7's concern, covered by other tests).
    FeedGenerator::Config feed_config{
        .symbols          = eight_symbols(),
        .num_shards       = kNumShards,
        .message_count    = 20'000,
        .packet_loss_rate = 0.0,
        .seed             = 4242,
    };

    PipelineRunner runner(feed_config, kNumShards);

    std::vector<RecordingSink*> sinks;
    for (std::size_t i = 0; i < kNumShards; ++i) {
        auto sink = std::make_unique<RecordingSink>();
        sinks.push_back(sink.get());
        runner.set_delta_sink(i, std::move(sink));
    }

    runner.start();
    runner.join();

    bool any_deltas = false;
    for (std::size_t i = 0; i < kNumShards; ++i) {
        // Reconstruct (symbol, side, price) -> current quantity purely
        // from the delta stream, applied in arrival order: a delta with
        // total_quantity == 0 removes that level, otherwise it's the
        // level's latest known quantity -- exactly LevelDelta's
        // 0-means-removed convention (orderbook/order_book.hpp).
        std::map<std::tuple<protocol::Symbol, protocol::Side, protocol::Price>, protocol::Quantity> reconstructed;
        for (const auto& delta : sinks[i]->deltas) {
            any_deltas = true;
            auto key = std::make_tuple(delta.symbol, delta.side, delta.price);
            if (delta.total_quantity == 0) {
                reconstructed.erase(key);
            } else {
                reconstructed[key] = delta.total_quantity;
            }
        }

        // Compare against the live book's actual final state, level by
        // level, both directions: every live level must match the
        // reconstruction (no missing/incorrect deltas), and the
        // reconstruction must have no extra levels the live book doesn't
        // (no stale/over-reported deltas).
        std::size_t live_level_count = 0;
        for (const auto& [symbol, book] : runner.pipeline().shard(i).books) {
            const auto snapshot = book.depth_snapshot(1'000'000);
            live_level_count += snapshot.bids.size() + snapshot.asks.size();
            for (const auto& level : snapshot.bids) {
                const auto key = std::make_tuple(symbol, protocol::Side::Buy, level.price);
                REQUIRE(reconstructed.contains(key));
                REQUIRE(reconstructed.at(key) == level.total_quantity);
            }
            for (const auto& level : snapshot.asks) {
                const auto key = std::make_tuple(symbol, protocol::Side::Sell, level.price);
                REQUIRE(reconstructed.contains(key));
                REQUIRE(reconstructed.at(key) == level.total_quantity);
            }
        }
        REQUIRE(reconstructed.size() == live_level_count);
    }

    REQUIRE(any_deltas);  // sanity: this actually exercised the publisher path, not a degenerate empty run
}

TEST_CASE(
    "FileDeltaSink downstream stub: a full pipeline run produces a non-empty, human-readable delta log per shard",
    "[pipeline_e2e][publisher]") {
    constexpr std::size_t kNumShards = 2;

    FeedGenerator::Config feed_config{
        .symbols          = eight_symbols(),
        .num_shards       = kNumShards,
        .message_count    = 5'000,
        .packet_loss_rate = 0.0,
        .seed             = 909,
    };

    PipelineRunner runner(feed_config, kNumShards);

    std::vector<std::filesystem::path> paths;
    for (std::size_t i = 0; i < kNumShards; ++i) {
        auto path = std::filesystem::temp_directory_path() / ("mdfh_test_pipeline_e2e_shard_" +
                                                                std::to_string(i) + ".deltas.log");
        std::filesystem::remove(path);
        paths.push_back(path);
        runner.set_delta_sink(i, std::make_unique<publisher::FileDeltaSink>(path));
    }

    runner.start();
    runner.join();

    bool any_output = false;
    for (const auto& path : paths) {
        std::ifstream in(path);
        REQUIRE(in.is_open());
        std::string line;
        std::size_t line_count = 0;
        while (std::getline(in, line)) {
            ++line_count;
            REQUIRE(line.find("seq=") != std::string::npos);
            REQUIRE(line.find("symbol=") != std::string::npos);
        }
        any_output = any_output || (line_count > 0);
        std::filesystem::remove(path);
    }
    REQUIRE(any_output);  // observed, live book delta output at the far end of the full pipeline
}

namespace {

// Wraps a RecordingSink with a small artificial per-publish delay. The
// natural-completion reconstruction test above proves zero delta loss, but
// only under whatever thread timing the OS happens to give a normal run --
// it doesn't specifically stress the shutdown handshake's actual race
// window (a decoder's LAST push to Shard::output_queue, concurrent with
// that decoder observing producer_done() and about to set
// shard_decoder_done_[i], concurrent with its publisher thread possibly
// already mid-drain). This delay deliberately biases the publisher thread
// to lag behind its decoder, making it far more likely that stop() below
// lands while the publisher genuinely still has undrained work queued --
// exercising the handshake's "one more drain after observing done" path
// on purpose, rather than hoping incidental scheduling happens to hit it.
struct SlowRecordingSink : publisher::DeltaSink {
    std::vector<publisher::BookDelta> deltas;
    void publish(const publisher::BookDelta& delta) override {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
        deltas.push_back(delta);
    }
};

}  // namespace

TEST_CASE(
    "Publisher shutdown handshake loses no deltas when stop() lands mid-flight, with the publisher "
    "thread deliberately lagging behind its decoder",
    "[pipeline_e2e][publisher]") {
    constexpr std::size_t kNumShards = 2;

    FeedGenerator::Config feed_config{
        .symbols          = eight_symbols(),
        .num_shards       = kNumShards,
        .message_count    = 1'000'000,  // deliberately huge -- stop() cuts this off long before exhaustion
        .packet_loss_rate = 0.0,        // isolate the shutdown-handshake question from gap-driven divergence
        .seed             = 2024,
    };

    PipelineRunner runner(feed_config, kNumShards);

    std::vector<SlowRecordingSink*> sinks;
    for (std::size_t i = 0; i < kNumShards; ++i) {
        auto sink = std::make_unique<SlowRecordingSink>();
        sinks.push_back(sink.get());
        runner.set_delta_sink(i, std::move(sink));
    }

    runner.start();

    // Deliberately short (2ms, not the liveness test's 20ms): with
    // SlowRecordingSink's 50us/delta cap, the publisher can process at
    // most ~40 deltas in this window, while the decoder can easily
    // generate far more -- guaranteeing genuine backlog exists when stop()
    // fires (the actual point of this test) -- but staying comfortably
    // under kDeltaQueueCapacity (4096, see publisher/book_delta.hpp)
    // regardless of exact decoder throughput, so the queue's OWN
    // drop-oldest backpressure (a real, separate, already-tested behavior)
    // can't fire and get confused with the shutdown-handshake question
    // this test isolates. Verified explicitly below via
    // dropped_updates_total(), not just assumed from the arithmetic.
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    REQUIRE_FALSE(runner.producer_done());  // sanity: genuinely still mid-flight

    runner.stop();  // triggers shutdown while decoders and the (artificially slow) publishers are both mid-work

    REQUIRE(runner.producer_done());
    REQUIRE(runner.feed().total_generated() < 1'000'000);  // confirms this really was cut short, not a full run

    // The actual claim: whatever the live book ended up with, every one of
    // those price levels is fully and correctly reflected in the deltas
    // this shard's (deliberately lagging) publisher thread received --
    // proving the final drain-after-done-flag path didn't drop anything
    // that was genuinely in flight when stop() landed. Identical technique
    // to the natural-completion reconstruction test above.
    bool any_deltas = false;
    for (std::size_t i = 0; i < kNumShards; ++i) {
        // Precondition, checked rather than assumed: no LEGITIMATE
        // drop-oldest loss happened on this shard's output queue. If this
        // ever fails, the test's premise (the only possible loss is a
        // shutdown-handshake bug, not ordinary backpressure) is violated,
        // and the timing constants above need retuning -- not a silent
        // false pass or a confusing failure below.
        REQUIRE(runner.pipeline().shard(i).output_queue.dropped_updates_total() == 0);

        std::map<std::tuple<protocol::Symbol, protocol::Side, protocol::Price>, protocol::Quantity> reconstructed;
        for (const auto& delta : sinks[i]->deltas) {
            any_deltas = true;
            auto key = std::make_tuple(delta.symbol, delta.side, delta.price);
            if (delta.total_quantity == 0) {
                reconstructed.erase(key);
            } else {
                reconstructed[key] = delta.total_quantity;
            }
        }

        std::size_t live_level_count = 0;
        for (const auto& [symbol, book] : runner.pipeline().shard(i).books) {
            const auto snapshot = book.depth_snapshot(1'000'000);
            live_level_count += snapshot.bids.size() + snapshot.asks.size();
            for (const auto& level : snapshot.bids) {
                const auto key = std::make_tuple(symbol, protocol::Side::Buy, level.price);
                REQUIRE(reconstructed.contains(key));
                REQUIRE(reconstructed.at(key) == level.total_quantity);
            }
            for (const auto& level : snapshot.asks) {
                const auto key = std::make_tuple(symbol, protocol::Side::Sell, level.price);
                REQUIRE(reconstructed.contains(key));
                REQUIRE(reconstructed.at(key) == level.total_quantity);
            }
        }
        REQUIRE(reconstructed.size() == live_level_count);
    }

    REQUIRE(any_deltas);  // sanity: the slow publisher actually processed something before/during shutdown
}
