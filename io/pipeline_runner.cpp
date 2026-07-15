#include "io/pipeline_runner.hpp"

#include <chrono>

#include "affinity/thread_affinity.hpp"
#include "io/async_network_reader.hpp"

namespace mdfh::io {

namespace {

// Phase 8 affinity-tag grouping decision.
//
// Every shard is a fully independent pipeline: its own SPSC ring buffer
// (Shard::queue), its own gap detector, its own symbol->OrderBook map, and
// (as of the Phase 7 pool redesign) its own independent PoolAllocator
// instances, touched only by that shard's own decoder thread -- see
// object_pool.hpp and shard_demux.hpp. No two shards' threads ever read or
// write the same memory. Grouping two different shards' threads under one
// affinity tag would therefore buy nothing (there's no shared data to gain
// locality on) while creating pure L2 contention between two otherwise
// unrelated workloads -- exactly the "grouping unrelated shards'
// communicating stages together" mistake to avoid. So: each shard gets its
// own dedicated tag, unshared with any other shard.
//
// The network I/O thread is the one thread that DOES cross shard
// boundaries -- it's the sole producer for every shard's ring buffer (see
// ShardedPipeline::demux). But precisely because it feeds ALL shards, not
// one, it has no single natural partner to share a tag with: grouping it
// with shard 0's tag, say, would arbitrarily privilege shard 0's cache
// locality over every other shard's for no principled reason. It gets its
// own dedicated tag instead, distinct from every shard tag.
//
// Once Phase 9 adds a per-shard publisher thread, that thread SHOULD share
// its shard's decoder tag: within one shard, the decoder thread is the
// sole producer for that shard's book-delta output queue and the
// publisher thread is its sole consumer -- a genuine producer/consumer
// data-sharing relationship, unlike the cross-shard case above, so L2
// locality between exactly those two threads is a real potential win.
constexpr affinity::AffinityTag kNetworkIoAffinityTag = 1;
constexpr affinity::AffinityTag kShardAffinityTagBase = 2;  // shard i -> kShardAffinityTagBase + i

}  // namespace

PipelineRunner::PipelineRunner(FeedGenerator::Config feed_config, std::size_t num_shards)
    : feed_(std::move(feed_config)), pipeline_(num_shards) {}

PipelineRunner::~PipelineRunner() {
    if (started_ && !joined_) {
        stop();
    }
}

void PipelineRunner::start(bool enable_affinity) {
    started_ = true;

    network_io_thread_ = std::thread([this] {
        Reactor reactor;
        // Task's initial_suspend is suspend_never, so this call runs the
        // coroutine body eagerly on THIS thread up to its first
        // `co_await WaitFor{...}` before returning here. Task is
        // fire-and-forget (no handle retained) -- run_until below is what
        // drives all of its subsequent resumes, on this same thread.
        Task task = read_packets_async(feed_, pipeline_, reactor, stop_requested_, producer_done_);
        (void)task;
        reactor.run_until([this] { return stop_requested_.load(std::memory_order_acquire); });
    });
    if (enable_affinity) {
        // Safe to tag right after construction, before the thread has done
        // any work -- set_thread_affinity_tag() looks up the thread's Mach
        // port by pthread_t, it doesn't need to run ON that thread (see
        // thread_affinity.cpp).
        ++affinity_hints_attempted_;
        if (affinity::set_thread_affinity_tag(network_io_thread_, kNetworkIoAffinityTag)) {
            ++affinity_hints_applied_;
        }
    }

    shard_threads_.reserve(pipeline_.num_shards());
    for (std::size_t i = 0; i < pipeline_.num_shards(); ++i) {
        shard_threads_.emplace_back([this, i] {
            // Must happen before this thread constructs any OrderBook (the
            // very first process_shard() call below can do exactly that,
            // via Shard::books[symbol]): every PoolAllocator this thread
            // touches from here on resolves to shard i's own independent
            // pools (see object_pool.hpp), which is what makes per-shard
            // pool access lock-free -- no other thread ever touches slot i.
            orderbook::set_current_shard_index(i);
            for (;;) {
                if (pipeline_.process_shard(i)) {
                    continue;  // more may already be queued; keep draining before checking anything else
                }
                if (producer_done()) {
                    // Producer is done; one more check in case a message
                    // was pushed between our last empty check and
                    // observing this flag -- same drain-to-completion
                    // pattern proven correct by Phase 5's stress test.
                    if (!pipeline_.process_shard(i)) {
                        break;
                    }
                } else {
                    std::this_thread::sleep_for(std::chrono::microseconds(50));
                }
            }
        });
        if (enable_affinity) {
            ++affinity_hints_attempted_;
            if (affinity::set_thread_affinity_tag(shard_threads_.back(),
                                                   kShardAffinityTagBase + static_cast<affinity::AffinityTag>(i))) {
                ++affinity_hints_applied_;
            }
        }
    }
}

void PipelineRunner::stop() {
    stop_requested_.store(true, std::memory_order_release);
    join();
}

void PipelineRunner::join() {
    if (joined_) {
        return;
    }
    // Join the network I/O thread first: it sets producer_done_ as its
    // very last act, so by the time it's joined, every shard thread's
    // "producer is done, do a final drain and exit" check is guaranteed
    // to see it on their next iteration -- no separate signaling needed.
    if (network_io_thread_.joinable()) {
        network_io_thread_.join();
    }
    for (auto& t : shard_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    joined_ = true;
}

}  // namespace mdfh::io
