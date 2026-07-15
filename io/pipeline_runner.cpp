#include "io/pipeline_runner.hpp"

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
// Phase 9's publisher thread shares its shard's decoder tag, per the plan
// left here in Phase 8: within one shard, the decoder thread is the sole
// producer for that shard's book-delta output queue (Shard::output_queue)
// and the publisher thread is its sole consumer -- a genuine
// producer/consumer data-sharing relationship, unlike the cross-shard case
// above, so L2 locality between exactly those two threads is a real
// potential win.
constexpr affinity::AffinityTag kNetworkIoAffinityTag = 1;
constexpr affinity::AffinityTag kShardAffinityTagBase = 2;  // shard i's decoder AND publisher -> this tag + i

}  // namespace

PipelineRunner::PipelineRunner(FeedGenerator::Config feed_config, std::size_t num_shards)
    : feed_(std::move(feed_config)), pipeline_(num_shards) {
    delta_sinks_.reserve(pipeline_.num_shards());
    for (std::size_t i = 0; i < pipeline_.num_shards(); ++i) {
        delta_sinks_.push_back(std::make_unique<publisher::NullDeltaSink>());
    }
}

PipelineRunner::~PipelineRunner() {
    if (started_ && !joined_) {
        stop();
    }
}

void PipelineRunner::set_delta_sink(std::size_t shard_index, std::unique_ptr<publisher::DeltaSink> sink) {
    delta_sinks_.at(shard_index) = std::move(sink);
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
    publisher_threads_.reserve(pipeline_.num_shards());
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
                    // Phase 12: a real OS-level block (futex/ulock via
                    // std::atomic::wait), not the old fixed sleep_for(50us)
                    // poll -- see concurrency/spsc_ring_buffer.hpp. Woken by
                    // either the network I/O thread's next push() to this
                    // shard's queue, or its notify_waiters() call once
                    // producer_done_ is set (see async_network_reader.cpp).
                    pipeline_.shard(i).queue.wait_for_data();
                }
            }
            // Last act, mirroring producer_done_ one stage upstream: tells
            // this shard's publisher thread it's seen everything this
            // decoder will ever push to Shard::output_queue, so its own
            // final drain-and-exit check is safe to trust.
            shard_decoder_done_[i].store(true, std::memory_order_release);
            // This shard's publisher thread may be genuinely blocked in
            // output_queue.wait_for_data() with nothing left coming --
            // wake it so it promptly re-checks shard_decoder_done_[i]
            // instead of waiting for a push() that will never happen.
            pipeline_.shard(i).output_queue.notify_waiters();
        });
        if (enable_affinity) {
            ++affinity_hints_attempted_;
            if (affinity::set_thread_affinity_tag(shard_threads_.back(),
                                                   kShardAffinityTagBase + static_cast<affinity::AffinityTag>(i))) {
                ++affinity_hints_applied_;
            }
        }

        publisher_threads_.emplace_back([this, i] {
            publisher::DeltaSink& sink = *delta_sinks_[i];
            for (;;) {
                if (publisher::drain_publisher_queue(pipeline_.shard(i).output_queue, sink)) {
                    continue;  // more may already be queued; keep draining before checking anything else
                }
                if (shard_decoder_done_[i].load(std::memory_order_acquire)) {
                    // Same "one more check, in case something was pushed
                    // between our last empty check and observing the done
                    // flag" pattern as the decoder loop above.
                    if (!publisher::drain_publisher_queue(pipeline_.shard(i).output_queue, sink)) {
                        break;
                    }
                } else {
                    // Phase 12: real OS-level block, same mechanism and
                    // reasoning as the decoder loop above -- woken by this
                    // shard's decoder thread's next delta push(), or its
                    // notify_waiters() call once shard_decoder_done_[i] is
                    // set.
                    pipeline_.shard(i).output_queue.wait_for_data();
                }
            }
        });
        if (enable_affinity) {
            // Shares its decoder's tag, not a new one -- see the
            // tag-grouping comment above.
            ++affinity_hints_attempted_;
            if (affinity::set_thread_affinity_tag(publisher_threads_.back(),
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
    // Join order matches the data flow (network I/O -> decoder ->
    // publisher), each stage's "I'm done" flag set as its last act before
    // it exits: network_io_thread_ sets producer_done_, so every decoder's
    // "producer is done, final drain and exit" check is guaranteed to see
    // it by the time we even start waiting on shard_threads_; each decoder
    // sets shard_decoder_done_[i], so every publisher's equivalent check is
    // likewise guaranteed to see it by the time we start waiting on
    // publisher_threads_. No separate cross-thread signaling needed beyond
    // these flags.
    if (network_io_thread_.joinable()) {
        network_io_thread_.join();
    }
    for (auto& t : shard_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    for (auto& t : publisher_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    joined_ = true;
}

}  // namespace mdfh::io
