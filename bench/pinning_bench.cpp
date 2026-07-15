// Phase 8 benchmark: does macOS affinity-tag hinting (see
// affinity/thread_affinity.hpp and io/pipeline_runner.cpp's tag-grouping
// comment) actually change full-pipeline latency?
//
// Runs the IDENTICAL live pipeline (Phase 7's PipelineRunner: coroutine
// network I/O thread -> demux -> per-shard SPSC queues -> per-shard decoder
// threads) with PipelineRunner::start(enable_affinity) toggled on and off,
// several times each, and reports wall-clock time from start() to the
// pipeline's natural completion (join()).
//
// What this measures: total time to fully process a fixed message count
// through the real, running, multi-threaded pipeline -- i.e. an
// end-to-end THROUGHPUT number, from which a mean per-message time is
// derived (total_time / message_count). This is NOT the same thing as a
// true tick-to-book-update latency percentile (that requires per-message
// timestamping at each stage boundary, which is Phase 10's job) -- it's
// the best latency-flavored signal available with what Phase 1-8 have
// actually built, and it's an honest one: if pinning helps or hurts, it
// will move this number.
//
// What this does NOT prove, given macOS's affinity model: THREAD_AFFINITY_
// POLICY is an advisory scheduler hint, not Linux's hard-pin guarantee
// (see thread_affinity.hpp's full explanation). A small or noisy
// before/after delta here is consistent with either "pinning genuinely
// doesn't help this workload" or "the kernel scheduler didn't act on the
// hint this run" -- macOS gives no API to distinguish the two from
// userspace. A consistent, repeatable improvement across many runs is
// still real evidence; a single run's number is not.

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "io/pipeline_runner.hpp"

using namespace mdfh::io;
namespace protocol = mdfh::protocol;

namespace {

constexpr std::size_t kNumShards = 8;

// FeedGenerator's message mix (io/feed_generator.cpp) is Add-heavy: 50% of
// messages Add a resting order, only 15% Cancel one, so resting-order
// count grows by roughly 0.35 * message_count over a run, concentrated in
// whichever shard's pool sees the most traffic (orderbook::
// kShardOrderPoolCapacity == 124,000, see order_book.hpp). With
// sixty_four_symbols()'s verified 1.25x worst-case skew (10-of-64 symbols
// on the busiest shard vs. an 8-symbol average), that shard sees up to
// ~15.6% of total messages: 0.35 * 1,000,000 * 0.156 ≈ 54,700, comfortably
// under 124,000 with real margin. An earlier version of this benchmark
// used 3,000,000 messages with a much more skewed (structurally
// correlated) symbol set and reliably crashed (uncaught std::bad_alloc,
// SIGABRT) once the busiest shard's pool filled.
constexpr std::size_t kMessageCount = 1'000'000;

// Kept well below Phase 7's E2E test's 1% (io/test_pipeline_e2e.cpp) on
// purpose: GapDetector::log_gap (concurrency/gap_detector.hpp) does a
// synchronous, mutex-guarded std::cerr write per gap. At 1% loss over a
// million messages that's ~10,000 blocking I/O calls competing across 8
// shard threads -- exactly the kind of cross-thread contention that would
// swamp any pinning signal this benchmark is trying to measure. 0.05%
// still exercises the gap-detection path under real concurrent load
// without turning the benchmark into an I/O-bound stderr test.
constexpr double kPacketLossRate = 0.0005;
constexpr int kRunsPerVariant     = 5;

// 64 pseudorandom 4-letter symbols. An earlier version of this generator
// built symbols from cyclic permutations of one base alphabet ("IZUH",
// "MUEF", ... via shared offsets) -- structurally correlated strings that
// FNV-1a (protocol::hash_symbol) hashed very unevenly: verified with a
// standalone probe, 13 of 32 such symbols landed on ONE shard (of 8) and 1
// shard got zero. That skew, combined with FeedGenerator's Add-heavy
// message mix (see kMessageCount's comment below), is what caused the
// std::bad_alloc crash this benchmark hit before this fix. Independently
// random characters hash far more evenly: verified with the same probe,
// seed 4242 gives a max/avg shard-load ratio of 1.25x (10 symbols on the
// busiest shard vs. an 8-symbol average across 8 shards), which is what
// kMessageCount's headroom below is sized against.
std::vector<protocol::Symbol> sixty_four_symbols() {
    std::mt19937_64 rng(4242);
    std::uniform_int_distribution<int> letter(0, 25);

    std::vector<protocol::Symbol> symbols;
    std::set<std::string> seen;
    symbols.reserve(64);
    while (symbols.size() < 64) {
        char buf[5];
        for (int i = 0; i < 4; ++i) {
            buf[i] = static_cast<char>('A' + letter(rng));
        }
        buf[4] = '\0';
        if (!seen.insert(buf).second) {
            continue;  // reject a repeat, keep symbols distinct
        }
        symbols.push_back(protocol::make_symbol(buf));
    }
    return symbols;
}

struct RunResult {
    std::chrono::nanoseconds elapsed;
    std::size_t affinity_applied;
    std::size_t affinity_attempted;
};

// One timed run of the full pipeline, start() to join(). `seed` varies per
// run so the exact interleaving/timing isn't degenerate-identical across
// repeats, while message_count/symbol set/loss rate stay fixed -- the
// workload SHAPE is held constant, only the affinity toggle and RNG seed
// vary.
RunResult run_once(bool enable_affinity, std::uint64_t seed) {
    FeedGenerator::Config feed_config{
        .symbols          = sixty_four_symbols(),
        .num_shards       = kNumShards,
        .message_count    = kMessageCount,
        .packet_loss_rate = kPacketLossRate,
        .seed             = seed,
    };

    PipelineRunner runner(feed_config, kNumShards);

    const auto start = std::chrono::steady_clock::now();
    runner.start(enable_affinity);
    runner.join();
    const auto end = std::chrono::steady_clock::now();

    return RunResult{
        .elapsed             = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start),
        .affinity_applied    = runner.affinity_hints_applied(),
        .affinity_attempted  = runner.affinity_hints_attempted(),
    };
}

struct Stats {
    double mean_ms;
    double stddev_ms;
    double min_ms;
    double max_ms;
    double mean_ns_per_msg;
};

Stats summarize(const std::vector<std::chrono::nanoseconds>& runs) {
    std::vector<double> ms;
    ms.reserve(runs.size());
    for (auto r : runs) {
        ms.push_back(std::chrono::duration<double, std::milli>(r).count());
    }

    const double sum  = std::accumulate(ms.begin(), ms.end(), 0.0);
    const double mean = sum / static_cast<double>(ms.size());

    double variance = 0.0;
    for (double v : ms) {
        variance += (v - mean) * (v - mean);
    }
    variance /= static_cast<double>(ms.size());

    return Stats{
        .mean_ms         = mean,
        .stddev_ms       = std::sqrt(variance),
        .min_ms          = *std::min_element(ms.begin(), ms.end()),
        .max_ms          = *std::max_element(ms.begin(), ms.end()),
        .mean_ns_per_msg = (mean * 1'000'000.0) / static_cast<double>(kMessageCount),
    };
}

void print_runs(const char* label, const std::vector<std::chrono::nanoseconds>& runs) {
    std::printf("%s runs (ms): ", label);
    for (auto r : runs) {
        std::printf("%.2f ", std::chrono::duration<double, std::milli>(r).count());
    }
    std::printf("\n");
}

}  // namespace

int main() {
    std::printf(
        "Phase 8 pinning benchmark: %zu shards, %zu messages/run, %.1f%% packet loss, %d runs per variant\n\n",
        kNumShards, kMessageCount, kPacketLossRate * 100.0, kRunsPerVariant);

    std::vector<std::chrono::nanoseconds> pinned_runs;
    std::vector<std::chrono::nanoseconds> unpinned_runs;
    std::size_t affinity_applied_total    = 0;
    std::size_t affinity_attempted_total  = 0;

    // Interleave pinned/unpinned rather than running all of one variant
    // then all of the other -- guards against systematic drift (thermal
    // throttling, background load creeping in over the benchmark's total
    // runtime) biasing whichever variant runs second.
    for (int i = 0; i < kRunsPerVariant; ++i) {
        const auto seed = static_cast<std::uint64_t>(1000 + i);

        const RunResult pinned_result = run_once(/*enable_affinity=*/true, seed);
        pinned_runs.push_back(pinned_result.elapsed);
        affinity_applied_total += pinned_result.affinity_applied;
        affinity_attempted_total += pinned_result.affinity_attempted;

        unpinned_runs.push_back(run_once(/*enable_affinity=*/false, seed).elapsed);
    }

    print_runs("Pinned  ", pinned_runs);
    print_runs("Unpinned", unpinned_runs);
    std::printf("\n");

    std::printf(
        "Affinity hints actually accepted by the kernel (KERN_SUCCESS): %zu / %zu attempted, across all %d pinned "
        "runs (network I/O thread + %zu shard threads each).\n",
        affinity_applied_total, affinity_attempted_total, kRunsPerVariant, kNumShards);
    if (affinity_applied_total == 0 && affinity_attempted_total > 0) {
        std::printf(
            "-> 0 accepted: on this machine, thread_policy_set(THREAD_AFFINITY_POLICY) is being rejected outright "
            "(KERN_NOT_SUPPORTED), not merely ignored as a soft hint -- see thread_affinity.hpp. Any timing delta "
            "below reflects code-path/scheduling noise from the enable_affinity branch itself, NOT an actual "
            "pinning effect, since no pinning ever took place.\n");
    }
    std::printf("\n");

    const Stats pinned   = summarize(pinned_runs);
    const Stats unpinned = summarize(unpinned_runs);

    std::printf("%-24s %12s %12s %12s %12s %16s\n", "Variant", "Mean (ms)", "Stddev(ms)", "Min (ms)", "Max (ms)",
                "ns/message");
    std::printf("%-24s %12.2f %12.2f %12.2f %12.2f %16.1f\n", "Pinned (affinity on)", pinned.mean_ms,
                pinned.stddev_ms, pinned.min_ms, pinned.max_ms, pinned.mean_ns_per_msg);
    std::printf("%-24s %12.2f %12.2f %12.2f %12.2f %16.1f\n", "Unpinned (affinity off)", unpinned.mean_ms,
                unpinned.stddev_ms, unpinned.min_ms, unpinned.max_ms, unpinned.mean_ns_per_msg);

    const double delta_pct = ((unpinned.mean_ms - pinned.mean_ms) / unpinned.mean_ms) * 100.0;
    std::printf("\nMean delta: pinned is %.2f%% %s than unpinned (%.2f ms vs %.2f ms).\n", std::fabs(delta_pct),
                delta_pct >= 0.0 ? "faster" : "slower", pinned.mean_ms, unpinned.mean_ms);

    std::printf(
        "\nCaveat (see thread_affinity.hpp): macOS's THREAD_AFFINITY_POLICY is an advisory\n"
        "scheduler hint, not Linux's pthread_setaffinity_np hard pin. A delta within roughly\n"
        "one run's stddev of zero is NOT strong evidence pinning helps or hurts on this\n"
        "platform -- it may simply mean the scheduler didn't act on the hint this run. Only a\n"
        "delta that's large and consistent relative to the stddev above should be read as a\n"
        "real effect.\n");

    return 0;
}
