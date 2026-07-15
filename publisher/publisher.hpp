#pragma once

// Phase 9: downstream consumer for a shard's book-delta output queue.
//
// Two pieces, deliberately kept generic (no dependency on io::Shard /
// io::ShardedPipeline -- see book_delta.hpp's header comment for why):
//   - DeltaSink: where a delta goes once it's dequeued. FileDeltaSink is
//     the "simple downstream consumer stub" build_guide.md Phase 9 asks
//     for -- proves the pipeline's output is observable end to end.
//     NullDeltaSink is the default so every earlier-phase test/benchmark
//     that builds a PipelineRunner without caring about publisher output
//     keeps working unchanged (see io/pipeline_runner.hpp's
//     set_delta_sink()).
//   - drain_publisher_queue(): pops everything currently queued and hands
//     each delta to the sink, mirroring ShardedPipeline::process_shard()'s
//     "drain everything currently available, return whether it did work"
//     shape so io/pipeline_runner.cpp's publisher-thread loop can reuse
//     the exact same drain-until-empty + upstream-done handshake pattern
//     already proven correct for decoder threads.

#include <filesystem>
#include <fstream>

#include "publisher/book_delta.hpp"

namespace mdfh::publisher {

class DeltaSink {
public:
    virtual ~DeltaSink() = default;
    virtual void publish(const BookDelta& delta) = 0;
};

// Discards everything. The default sink for every shard unless
// PipelineRunner::set_delta_sink() installs a real one -- keeps existing
// Phase 7/8 call sites (which never heard of publishers) working
// unchanged, and keeps the publish() call on the hot path cheap (a single
// no-op virtual dispatch) when nobody's listening.
class NullDeltaSink : public DeltaSink {
public:
    void publish(const BookDelta&) override {}
};

// Writes one formatted line per delta to a file -- deliberately one
// INSTANCE PER SHARD (see io/pipeline_runner.cpp), not one shared sink
// behind a mutex: a shared stream would reintroduce exactly the
// cross-shard contention this project has repeatedly designed away
// (object_pool.hpp's per-shard pools, the per-shard ring buffers
// themselves). Each shard's publisher thread is the sole writer to its
// own FileDeltaSink, so no synchronization is needed here either.
class FileDeltaSink : public DeltaSink {
public:
    explicit FileDeltaSink(const std::filesystem::path& path);
    void publish(const BookDelta& delta) override;

private:
    std::ofstream out_;
};

// Formats one delta as a single line of text (used by FileDeltaSink;
// exposed separately so a test can check formatting without touching the
// filesystem). total_quantity == 0 is rendered distinctly ("REMOVED") so a
// human tailing the file can tell "level updated" from "level gone" at a
// glance, matching what LevelDelta's 0-means-removed convention already
// encodes structurally (orderbook/order_book.hpp).
std::string format_delta_line(const BookDelta& delta);

// Pops everything currently available on `queue` and publishes each to
// `sink`, in order. Returns true if it processed at least one delta.
// Meant to be called repeatedly by that shard's own dedicated publisher
// thread (see io/pipeline_runner.cpp) -- like ShardedPipeline::
// process_shard(), calling this for the same queue from more than one
// thread concurrently is not safe (SpscRingBuffer::try_pop requires a
// single consumer).
bool drain_publisher_queue(DeltaQueue& queue, DeltaSink& sink);

}  // namespace mdfh::publisher
