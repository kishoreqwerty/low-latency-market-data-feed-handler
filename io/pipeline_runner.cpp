#include "io/pipeline_runner.hpp"

#include <chrono>

#include "io/async_network_reader.hpp"

namespace mdfh::io {

PipelineRunner::PipelineRunner(FeedGenerator::Config feed_config, std::size_t num_shards)
    : feed_(std::move(feed_config)), pipeline_(num_shards) {}

PipelineRunner::~PipelineRunner() {
    if (started_ && !joined_) {
        stop();
    }
}

void PipelineRunner::start() {
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
