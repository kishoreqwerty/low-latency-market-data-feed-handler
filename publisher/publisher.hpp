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

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <vector>

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

// Phase 10: one delta's full stage-boundary breakdown, in nanoseconds.
// Field names spell out exactly which two LatencyTrace timestamps (plus
// this sink's own t_delta_dequeued/t_published) each one spans, rather
// than trying to force each interval into a single stage label -- e.g.
// receive_to_decode necessarily covers BOTH "however long between bytes
// being ready and decode() actually being invoked" (~0, same function
// call in this pipeline) AND decode()'s own running time, since
// t_received and t_decoded bracket both at once.
struct LatencySample {
    std::chrono::nanoseconds receive_to_decode;    // t_decoded - t_received (network I/O readiness -> decode done)
    std::chrono::nanoseconds decode_to_demux;      // t_demuxed - t_decoded (routing + enqueue cost)
    std::chrono::nanoseconds input_queue_wait;     // t_dequeued - t_demuxed (queued before the decoder got to it)
    std::chrono::nanoseconds book_update;          // t_book_updated - t_dequeued (apply() cost itself)
    std::chrono::nanoseconds output_queue_wait;    // t_delta_dequeued - t_book_updated
    std::chrono::nanoseconds publish;              // t_published - t_delta_dequeued (downstream I/O cost)

    std::chrono::nanoseconds tick_to_book_update;  // t_book_updated - t_received -- THE primary metric
    std::chrono::nanoseconds end_to_end;           // t_published - t_received
};

// Decorator: forwards every delta to `inner` (defaults to a NullDeltaSink,
// so a pure latency-measurement run doesn't need a real downstream target)
// and additionally records a LatencySample built from the delta's
// LatencyTrace plus two more timestamps captured right here -- the
// t_delta_dequeued/t_published boundaries the trace itself doesn't carry
// (see concurrency/latency_trace.hpp's header comment for why those two
// live here instead of being threaded further).
//
// One instance per shard (see io/pipeline_runner.cpp / bench/latency_bench.cpp),
// same as FileDeltaSink -- samples_ is only ever appended to by that one
// shard's publisher thread, so no synchronization is needed. Deliberately
// NOT capacity-bounded like the production hot-path containers
// (object_pool.hpp et al.): this is an opt-in measurement tool, not part
// of the default pipeline, so letting `samples_` grow (reallocating like
// any normal std::vector if it must) is an accepted, scoped exception to
// the zero-allocation rule -- see latency_trace.hpp's header comment for
// the same reasoning applied to why timestamps are captured at all.
class LatencyRecordingSink : public DeltaSink {
public:
    explicit LatencyRecordingSink(std::unique_ptr<DeltaSink> inner = std::make_unique<NullDeltaSink>());

    void publish(const BookDelta& delta) override;

    const std::vector<LatencySample>& samples() const noexcept { return samples_; }

private:
    std::unique_ptr<DeltaSink> inner_;
    std::vector<LatencySample> samples_;
};

}  // namespace mdfh::publisher
