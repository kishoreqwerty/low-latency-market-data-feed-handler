#include <cstddef>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "io/feed_generator.hpp"
#include "io/shard_demux.hpp"
#include "protocol/decoder.hpp"
#include "protocol/message_types.hpp"

using namespace mdfh::io;
namespace protocol = mdfh::protocol;

namespace {

FeedGenerator::Config base_config() {
    return FeedGenerator::Config{
        .symbols          = {protocol::make_symbol("AAPL"), protocol::make_symbol("MSFT"),
                              protocol::make_symbol("GOOG")},
        .num_shards       = 4,
        .message_count    = 5'000,
        .packet_loss_rate = 0.0,
        .seed             = 123,
    };
}

}  // namespace

TEST_CASE("FeedGenerator with zero packet loss produces exactly message_count valid, decodable messages",
          "[feed_generator]") {
    FeedGenerator feed(base_config());

    std::size_t received = 0;
    for (;;) {
        auto packet = feed.next();
        if (packet.exhausted) {
            break;
        }
        REQUIRE(packet.bytes.has_value());  // zero loss rate: nothing should ever be dropped
        auto decoded = protocol::decode(*packet.bytes);
        REQUIRE(decoded.has_value());  // every generated packet must be independently well-formed
        ++received;
    }

    REQUIRE(received == 5'000);
    REQUIRE(feed.total_generated() == 5'000);
    REQUIRE(feed.total_dropped() == 0);
}

TEST_CASE("FeedGenerator is exhausted after message_count and stays exhausted", "[feed_generator]") {
    FeedGenerator::Config cfg = base_config();
    cfg.message_count         = 10;
    FeedGenerator feed(cfg);

    for (int i = 0; i < 10; ++i) {
        REQUIRE_FALSE(feed.next().exhausted);
    }
    for (int i = 0; i < 5; ++i) {
        REQUIRE(feed.next().exhausted);  // stays exhausted on repeated calls, doesn't wrap around
    }
}

TEST_CASE("FeedGenerator's packet loss rate roughly matches configuration over a large run", "[feed_generator]") {
    FeedGenerator::Config cfg = base_config();
    cfg.message_count         = 50'000;
    cfg.packet_loss_rate      = 0.10;
    FeedGenerator feed(cfg);

    std::size_t seen = 0;
    for (;;) {
        auto packet = feed.next();
        if (packet.exhausted) break;
        if (packet.bytes.has_value()) ++seen;
    }

    REQUIRE(feed.total_generated() == 50'000);
    // seen + dropped must account for everything generated -- no packet is
    // both delivered and dropped, or neither.
    REQUIRE(seen + feed.total_dropped() == 50'000);

    // Loose statistical bound (not an exact check -- this is a random
    // process): with 50,000 draws at a true rate of 10%, the observed rate
    // should land comfortably within +-2% almost always.
    const double observed_rate = static_cast<double>(feed.total_dropped()) / 50'000.0;
    REQUIRE(observed_rate > 0.08);
    REQUIRE(observed_rate < 0.12);
}

TEST_CASE("FeedGenerator is deterministic for a fixed seed", "[feed_generator]") {
    FeedGenerator::Config cfg = base_config();
    cfg.message_count         = 500;
    cfg.packet_loss_rate      = 0.05;

    FeedGenerator feed_a(cfg);
    FeedGenerator feed_b(cfg);  // same config, same seed

    for (int i = 0; i < 500; ++i) {
        auto a = feed_a.next();
        auto b = feed_b.next();
        REQUIRE(a.exhausted == b.exhausted);
        REQUIRE(a.bytes.has_value() == b.bytes.has_value());
        if (a.bytes.has_value()) {
            REQUIRE(*a.bytes == *b.bytes);
        }
    }
}

TEST_CASE("FeedGenerator assigns per-shard sequence numbers that are exactly contiguous absent loss",
          "[feed_generator]") {
    FeedGenerator::Config cfg = base_config();
    cfg.message_count         = 2'000;
    cfg.packet_loss_rate      = 0.0;
    FeedGenerator feed(cfg);

    // Mirrors exactly what a real demux's routing table does (see
    // io/shard_demux.cpp): only AddOrder carries a symbol on the wire, so
    // Cancel/Execute/Replace are routed by remembering where their
    // originating order went.
    std::unordered_map<protocol::OrderId, std::size_t> order_to_shard;
    std::unordered_map<std::size_t, protocol::SeqNum> last_seq_per_shard;

    for (;;) {
        auto packet = feed.next();
        if (packet.exhausted) break;
        REQUIRE(packet.bytes.has_value());

        auto decoded = protocol::decode(*packet.bytes);
        REQUIRE(decoded.has_value());

        std::visit(
            [&](auto&& msg) {
                using Msg = std::decay_t<decltype(msg)>;
                std::size_t shard_index;
                if constexpr (std::is_same_v<Msg, protocol::AddOrder>) {
                    shard_index                        = shard_for_symbol(msg.symbol, cfg.num_shards);
                    order_to_shard[msg.order_id]        = shard_index;
                } else if constexpr (std::is_same_v<Msg, protocol::ReplaceOrder>) {
                    shard_index                          = order_to_shard.at(msg.old_order_id);
                    order_to_shard[msg.new_order_id]     = shard_index;
                } else {
                    shard_index = order_to_shard.at(msg.order_id);
                }

                auto it = last_seq_per_shard.find(shard_index);
                if (it == last_seq_per_shard.end()) {
                    last_seq_per_shard[shard_index] = msg.seq_num;
                } else {
                    REQUIRE(msg.seq_num == it->second + 1);  // exactly contiguous: zero loss rate, so no gaps
                    it->second = msg.seq_num;
                }
            },
            *decoded);
    }

    REQUIRE(last_seq_per_shard.size() <= cfg.num_shards);
}
