#pragma once

// Orchestrates a full running pipeline (build_guide.md Phase 7's "Done
// when"): a coroutine-driven network I/O thread pulling from a simulated
// feed and demux-ing into per-shard queues, plus one dedicated decoder
// thread per shard draining its own queue through its own gap detector and
// order books, plus (Phase 9) one dedicated publisher thread per shard
// draining that shard's book-delta output queue to a downstream sink --
// all genuinely concurrent, not simulated on one thread the way Phase 6's
// tests were.

#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "io/feed_generator.hpp"
#include "io/shard_demux.hpp"
#include "publisher/publisher.hpp"

namespace mdfh::io {

class PipelineRunner {
public:
    PipelineRunner(FeedGenerator::Config feed_config, std::size_t num_shards);

    // Stops (if still running) and joins, so a PipelineRunner going out of
    // scope -- including via an exception -- never leaves threads running,
    // hanging, or a coroutine frame orphaned. This is the answer to "what
    // happens if you want to stop the pipeline": RAII, not a separate
    // manual cleanup step callers have to remember.
    ~PipelineRunner();

    PipelineRunner(const PipelineRunner&)            = delete;
    PipelineRunner& operator=(const PipelineRunner&) = delete;

    // Installs a real downstream sink for one shard's book deltas (e.g. a
    // publisher::FileDeltaSink). Must be called before start() -- every
    // shard defaults to a publisher::NullDeltaSink otherwise, so every
    // Phase 7/8 call site that never heard of publishers keeps working
    // unchanged without having to construct one per shard it doesn't care
    // about.
    void set_delta_sink(std::size_t shard_index, std::unique_ptr<publisher::DeltaSink> sink);

    // Starts the network I/O thread, one decoder thread per shard, and
    // (Phase 9) one publisher thread per shard. Call once. `enable_affinity`
    // gates Phase 8's macOS affinity-tag hinting (see
    // affinity/thread_affinity.hpp and this .cpp's tag-grouping comment) --
    // defaulted on, but exposed so bench/pinning_bench.cpp can run the
    // identical pipeline with it on and off for a real before/after
    // comparison.
    void start(bool enable_affinity = true);

    // Cooperative shutdown: signals the network I/O coroutine to stop at
    // its next check (it may already be mid-flight -- see
    // async_network_reader.hpp's Reactor::run_until for how a suspended
    // coroutine still gets resumed promptly and destroyed cleanly rather
    // than orphaned), then blocks until every thread has actually exited.
    // Safe to call even after the feed has already exhausted itself.
    void stop();

    // Blocks until every thread has exited, whether that's because stop()
    // was called or the feed ran out on its own. Does not itself request a
    // stop -- use this alone to wait for natural completion.
    void join();

    // True once the network I/O thread has stopped generating/routing
    // messages (feed exhausted or stop_requested observed) -- the signal
    // shard decoder threads watch for before their final drain and exit.
    bool producer_done() const noexcept { return producer_done_.load(std::memory_order_acquire); }

    ShardedPipeline& pipeline() noexcept { return pipeline_; }
    const ShardedPipeline& pipeline() const noexcept { return pipeline_; }
    const FeedGenerator& feed() const noexcept { return feed_; }

    // Diagnostic for bench/pinning_bench.cpp: how many of this run's
    // affinity-tag hints (network I/O thread + one per shard decoder +
    // one per shard publisher) the kernel actually reported success for,
    // out of how many were attempted. Only meaningful when start() was
    // called with enable_affinity=true -- both stay 0 otherwise. See
    // affinity/thread_affinity.hpp: on this project's Apple Silicon dev
    // machine, attempted > 0 with applied == 0 is the expected, verified
    // result, not a bug.
    std::size_t affinity_hints_applied() const noexcept { return affinity_hints_applied_; }
    std::size_t affinity_hints_attempted() const noexcept { return affinity_hints_attempted_; }

private:
    FeedGenerator feed_;
    ShardedPipeline pipeline_;

    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> producer_done_{false};

    // Set by each shard's decoder thread just before it exits (mirroring
    // producer_done_'s role one stage upstream) -- the signal that shard's
    // publisher thread watches for before its own final drain and exit.
    // Fixed-size (orderbook::kMaxShards, the same ceiling shard indices are
    // already bounded by) rather than a resizable container: std::atomic
    // isn't movable/copyable, and a fixed array sidesteps needing either.
    std::array<std::atomic<bool>, orderbook::kMaxShards> shard_decoder_done_{};

    std::thread network_io_thread_;
    std::vector<std::thread> shard_threads_;      // decoders
    std::vector<std::thread> publisher_threads_;  // Phase 9

    // One sink per shard, indexed like shard_threads_/publisher_threads_.
    // Defaults to NullDeltaSink for every shard at construction; set_delta_sink()
    // replaces individual entries before start().
    std::vector<std::unique_ptr<publisher::DeltaSink>> delta_sinks_;

    std::size_t affinity_hints_applied_   = 0;
    std::size_t affinity_hints_attempted_ = 0;

    bool started_ = false;
    bool joined_  = false;
};

}  // namespace mdfh::io
