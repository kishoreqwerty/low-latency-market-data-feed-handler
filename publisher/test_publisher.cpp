#include <filesystem>
#include <fstream>
#include <sstream>

#include <catch2/catch_test_macros.hpp>

#include "publisher/publisher.hpp"

using namespace mdfh::publisher;
namespace protocol = mdfh::protocol;

namespace {

BookDelta make_delta(protocol::Quantity qty) {
    return BookDelta{
        .symbol         = protocol::make_symbol("AAPL"),
        .side           = protocol::Side::Buy,
        .price          = 10'000,
        .total_quantity = qty,
        .seq_num        = 42,
        .timestamp      = 12345,
    };
}

}  // namespace

TEST_CASE("format_delta_line renders a live level with its quantity", "[publisher]") {
    const std::string line = format_delta_line(make_delta(150));
    REQUIRE(line.find("AAPL") != std::string::npos);
    REQUIRE(line.find("BUY") != std::string::npos);
    REQUIRE(line.find("qty=150") != std::string::npos);
    // Symbol is a fixed, zero-padded array (protocol::make_symbol) --
    // formatting must trim the padding, not print embedded NUL bytes.
    REQUIRE(line.find('\0') == std::string::npos);
}

TEST_CASE("format_delta_line renders a removed level distinctly from a zero quantity looking like data",
          "[publisher]") {
    const std::string line = format_delta_line(make_delta(0));
    REQUIRE(line.find("REMOVED") != std::string::npos);
    REQUIRE(line.find("qty=0") == std::string::npos);  // must not read as "still resting, just empty"
}

TEST_CASE("NullDeltaSink discards without crashing", "[publisher]") {
    NullDeltaSink sink;
    sink.publish(make_delta(10));
    sink.publish(make_delta(0));
    SUCCEED("no crash");
}

TEST_CASE("drain_publisher_queue pops everything currently queued, in order, and reports whether it did work",
          "[publisher]") {
    DeltaQueue queue;
    std::vector<protocol::Quantity> seen;

    struct RecordingSink : DeltaSink {
        explicit RecordingSink(std::vector<protocol::Quantity>* seen) : seen(seen) {}
        std::vector<protocol::Quantity>* seen;
        void publish(const BookDelta& delta) override { seen->push_back(delta.total_quantity); }
    } sink(&seen);

    REQUIRE_FALSE(drain_publisher_queue(queue, sink));  // empty queue -> no work

    queue.push(make_delta(10));
    queue.push(make_delta(20));
    queue.push(make_delta(0));  // a "removed" delta mixed in

    REQUIRE(drain_publisher_queue(queue, sink));
    REQUIRE(seen == std::vector<protocol::Quantity>{10, 20, 0});

    REQUIRE_FALSE(drain_publisher_queue(queue, sink));  // drained -> nothing left
}

TEST_CASE("FileDeltaSink writes one formatted line per delta to disk", "[publisher]") {
    const auto path = std::filesystem::temp_directory_path() / "mdfh_test_publisher_output.log";
    std::filesystem::remove(path);

    {
        FileDeltaSink sink(path);
        sink.publish(make_delta(100));
        sink.publish(make_delta(0));
        // FileDeltaSink's destructor closes/flushes the ofstream here.
    }

    std::ifstream in(path);
    REQUIRE(in.is_open());
    std::string line1, line2;
    REQUIRE(std::getline(in, line1));
    REQUIRE(std::getline(in, line2));
    REQUIRE(line1.find("qty=100") != std::string::npos);
    REQUIRE(line2.find("REMOVED") != std::string::npos);

    std::filesystem::remove(path);
}
