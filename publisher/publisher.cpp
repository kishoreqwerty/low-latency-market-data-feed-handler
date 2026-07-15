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

}  // namespace mdfh::publisher
