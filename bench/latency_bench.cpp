// Phase 10: tick-to-book-update latency percentiles.
//
// Runs the full live pipeline (Phase 7's PipelineRunner) with a
// publisher::LatencyRecordingSink installed on every shard, wrapping a
// NullDeltaSink (no real downstream I/O -- this benchmark measures the
// PIPELINE's own latency, not a particular downstream sink's cost; see
// bench/pinning_bench.cpp for a benchmark that already exercises a
// FileDeltaSink-equivalent path under real threading). After the run,
// every shard's recorded publisher::LatencySample vector is merged and
// p50/p99/p99.9 computed per stage boundary -- see
// concurrency/latency_trace.hpp and publisher/publisher.hpp for exactly
// which timestamps bound which interval.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "io/pipeline_runner.hpp"
#include "publisher/publisher.hpp"

using namespace mdfh::io;
namespace protocol  = mdfh::protocol;
namespace publisher = mdfh::publisher;

namespace {

constexpr std::size_t kNumShards    = 8;
constexpr std::size_t kMessageCount = 1'000'000;
constexpr double kPacketLossRate    = 0.0005;

// Same generator, same verified skew (max/avg ratio ~1.25x with seed 4242),
// same capacity-safety reasoning as bench/pinning_bench.cpp -- see that
// file's kMessageCount comment for the full derivation. Kept as its own
// copy rather than a shared helper: these bench/ files are each meant to
// be a single, self-contained, readable artifact (matching
// allocation_bench.cpp's existing style), not sharing infrastructure that
// would make any one of them harder to read in isolation.
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
            continue;
        }
        symbols.push_back(protocol::make_symbol(buf));
    }
    return symbols;
}

double percentile(std::vector<std::int64_t>& sorted_ns, double p) {
    if (sorted_ns.empty()) {
        return 0.0;
    }
    // Nearest-rank method: index = ceil(p * n) - 1, clamped to a valid index.
    auto rank = static_cast<std::size_t>(std::ceil(p * static_cast<double>(sorted_ns.size())));
    rank      = std::clamp(rank, std::size_t{1}, sorted_ns.size());
    return static_cast<double>(sorted_ns[rank - 1]);
}

struct StageColumn {
    const char* name;
    std::vector<std::int64_t> ns;  // one entry per recorded sample, this stage's duration
};

void report_stage(const StageColumn& col) {
    if (col.ns.empty()) {
        std::printf("%-20s (no samples)\n", col.name);
        return;
    }
    std::vector<std::int64_t> sorted = col.ns;
    std::sort(sorted.begin(), sorted.end());

    const double p50   = percentile(sorted, 0.50);
    const double p99    = percentile(sorted, 0.99);
    const double p999   = percentile(sorted, 0.999);
    const double mean   = static_cast<double>(std::accumulate(sorted.begin(), sorted.end(), std::int64_t{0})) /
                         static_cast<double>(sorted.size());
    const double max_ns = static_cast<double>(sorted.back());

    std::printf("%-20s %12.0f %12.0f %12.0f %12.0f %12.0f %12zu\n", col.name, mean, p50, p99, p999, max_ns,
                sorted.size());
}

}  // namespace

int main() {
    std::printf("Phase 10 latency benchmark: %zu shards, %zu messages, %.2f%% packet loss\n\n", kNumShards,
                kMessageCount, kPacketLossRate * 100.0);

    FeedGenerator::Config feed_config{
        .symbols          = sixty_four_symbols(),
        .num_shards       = kNumShards,
        .message_count    = kMessageCount,
        .packet_loss_rate = kPacketLossRate,
        .seed             = 4242,
    };

    PipelineRunner runner(feed_config, kNumShards);

    std::vector<publisher::LatencyRecordingSink*> sinks;
    for (std::size_t i = 0; i < kNumShards; ++i) {
        auto sink = std::make_unique<publisher::LatencyRecordingSink>();
        sinks.push_back(sink.get());
        runner.set_delta_sink(i, std::move(sink));
    }

    runner.start();
    runner.join();

    // Merge every shard's samples into one set of columns, one per stage.
    StageColumn receive_to_decode{"receive_to_decode", {}};
    StageColumn decode_to_demux{"decode_to_demux", {}};
    StageColumn input_queue_wait{"input_queue_wait", {}};
    StageColumn book_update{"book_update", {}};
    StageColumn output_queue_wait{"output_queue_wait", {}};
    StageColumn publish{"publish", {}};
    StageColumn tick_to_book_update{"tick_to_book_update", {}};
    StageColumn end_to_end{"end_to_end", {}};

    std::size_t total_samples = 0;
    for (auto* sink : sinks) {
        total_samples += sink->samples().size();
    }
    receive_to_decode.ns.reserve(total_samples);
    decode_to_demux.ns.reserve(total_samples);
    input_queue_wait.ns.reserve(total_samples);
    book_update.ns.reserve(total_samples);
    output_queue_wait.ns.reserve(total_samples);
    publish.ns.reserve(total_samples);
    tick_to_book_update.ns.reserve(total_samples);
    end_to_end.ns.reserve(total_samples);

    for (auto* sink : sinks) {
        for (const auto& s : sink->samples()) {
            receive_to_decode.ns.push_back(s.receive_to_decode.count());
            decode_to_demux.ns.push_back(s.decode_to_demux.count());
            input_queue_wait.ns.push_back(s.input_queue_wait.count());
            book_update.ns.push_back(s.book_update.count());
            output_queue_wait.ns.push_back(s.output_queue_wait.count());
            publish.ns.push_back(s.publish.count());
            tick_to_book_update.ns.push_back(s.tick_to_book_update.count());
            end_to_end.ns.push_back(s.end_to_end.count());
        }
    }

    std::printf("%zu total delta samples recorded across %zu shards\n\n", total_samples, kNumShards);
    std::printf("%-20s %12s %12s %12s %12s %12s %12s\n", "Stage (ns)", "mean", "p50", "p99", "p99.9", "max",
                "n");
    report_stage(receive_to_decode);
    report_stage(decode_to_demux);
    report_stage(input_queue_wait);
    report_stage(book_update);
    report_stage(output_queue_wait);
    report_stage(publish);
    std::printf("%-20s %12s %12s %12s %12s %12s %12s\n", "---", "---", "---", "---", "---", "---", "---");
    report_stage(tick_to_book_update);
    report_stage(end_to_end);

    std::printf(
        "\ntick_to_book_update is the PRIMARY metric (t_received -> book apply() returned); end_to_end additionally "
        "includes the publish stage (this run's inner sink is a no-op NullDeltaSink, so 'publish' above measures "
        "the LatencyRecordingSink wrapper's own overhead, not real downstream I/O -- see bench/pinning_bench.cpp "
        "for latency under a real FileDeltaSink-equivalent path).\n");

    return 0;
}
