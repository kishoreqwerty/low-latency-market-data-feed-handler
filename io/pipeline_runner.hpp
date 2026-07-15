#pragma once

// Orchestrates a full running pipeline (build_guide.md Phase 7's "Done
// when"): a coroutine-driven network I/O thread pulling from a simulated
// feed and demux-ing into per-shard queues, plus one dedicated decoder
// thread per shard draining its own queue through its own gap detector and
// order books -- all genuinely concurrent, not simulated on one thread the
// way Phase 6's tests were.

#include <atomic>
#include <thread>
#include <vector>

#include "io/feed_generator.hpp"
#include "io/shard_demux.hpp"

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

    // Starts the network I/O thread and one decoder thread per shard. Call
    // once. `enable_affinity` gates Phase 8's macOS affinity-tag hinting
    // (see affinity/thread_affinity.hpp and this .cpp's tag-grouping
    // comment) -- defaulted on, but exposed so bench/pinning_bench.cpp can
    // run the identical pipeline with it on and off for a real before/after
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
    // affinity-tag hints (network I/O thread + one per shard) the kernel
    // actually reported KERN_SUCCESS for, out of how many were attempted.
    // Only meaningful when start() was called with enable_affinity=true --
    // both stay 0 otherwise. See affinity/thread_affinity.hpp: on this
    // project's Apple Silicon dev machine, attempted > 0 with applied == 0
    // is the expected, verified result, not a bug.
    std::size_t affinity_hints_applied() const noexcept { return affinity_hints_applied_; }
    std::size_t affinity_hints_attempted() const noexcept { return affinity_hints_attempted_; }

private:
    FeedGenerator feed_;
    ShardedPipeline pipeline_;

    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> producer_done_{false};

    std::thread network_io_thread_;
    std::vector<std::thread> shard_threads_;

    std::size_t affinity_hints_applied_   = 0;
    std::size_t affinity_hints_attempted_ = 0;

    bool started_ = false;
    bool joined_  = false;
};

}  // namespace mdfh::io
