#include "publisher/publisher.hpp"

#include <sstream>
#include <stdexcept>

namespace mdfh::publisher {

namespace {

// Symbol is a fixed, zero-padded char array (protocol::make_symbol) -- the
// trailing '\0' bytes aren't part of the ticker, so a formatted line
// shouldn't include them.
std::string_view symbol_view(const protocol::Symbol& symbol) {
    std::size_t len = 0;
    while (len < symbol.size() && symbol[len] != '\0') {
        ++len;
    }
    return std::string_view(symbol.data(), len);
}

}  // namespace

FileDeltaSink::FileDeltaSink(const std::filesystem::path& path) : out_(path, std::ios::out | std::ios::trunc) {
    if (!out_.is_open()) {
        throw std::runtime_error("FileDeltaSink: failed to open " + path.string());
    }
}

void FileDeltaSink::publish(const BookDelta& delta) {
    // Flushed per line, not left buffered until close: a "simple downstream
    // consumer stub" (build_guide.md Phase 9) is only useful as proof the
    // pipeline's output is observable if a reader tailing the file (or a
    // test reading it back right after PipelineRunner::join(), before this
    // sink is necessarily destroyed) actually sees what was written. This
    // is the publisher's own I/O stage, not the order book's hot path --
    // CLAUDE.md's zero-allocation rule applies to apply(), not to this.
    out_ << format_delta_line(delta) << '\n' << std::flush;
}

std::string format_delta_line(const BookDelta& delta) {
    std::ostringstream line;
    line << "seq=" << delta.seq_num << " ts=" << delta.timestamp << " symbol=" << symbol_view(delta.symbol)
         << " side=" << (delta.side == protocol::Side::Buy ? "BUY" : "SELL") << " price=" << delta.price
         << " qty=" << (delta.total_quantity == 0 ? "REMOVED" : std::to_string(delta.total_quantity));
    return line.str();
}

bool drain_publisher_queue(DeltaQueue& queue, DeltaSink& sink) {
    bool did_work = false;
    BookDelta delta;
    while (queue.try_pop(delta)) {
        did_work = true;
        sink.publish(delta);
    }
    return did_work;
}

LatencyRecordingSink::LatencyRecordingSink(std::unique_ptr<DeltaSink> inner) : inner_(std::move(inner)) {}

void LatencyRecordingSink::publish(const BookDelta& delta) {
    // t_delta_dequeued: approximated as "right now" -- this method is
    // called immediately after drain_publisher_queue() pops the delta off
    // Shard::output_queue (see publisher.hpp's drain_publisher_queue()
    // above), so the gap between the real pop and this line is a few
    // instructions, not a queue wait. Captured BEFORE calling inner_ so
    // the inner sink's own I/O cost (e.g. FileDeltaSink's file write)
    // doesn't get folded into output_queue_wait.
    const auto t_delta_dequeued = concurrency::LatencyClock::now();

    inner_->publish(delta);

    const auto t_published = concurrency::LatencyClock::now();

    const concurrency::LatencyTrace& trace = delta.trace;
    samples_.push_back(LatencySample{
        .receive_to_decode   = trace.t_decoded - trace.t_received,
        .decode_to_demux     = trace.t_demuxed - trace.t_decoded,
        .input_queue_wait    = trace.t_dequeued - trace.t_demuxed,
        .book_update         = trace.t_book_updated - trace.t_dequeued,
        .output_queue_wait   = t_delta_dequeued - trace.t_book_updated,
        .publish             = t_published - t_delta_dequeued,
        .tick_to_book_update = trace.t_book_updated - trace.t_received,
        .end_to_end          = t_published - trace.t_received,
    });
}

}  // namespace mdfh::publisher
