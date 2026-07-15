#pragma once

// Simulated multicast market data feed generator (build_guide.md Phase 7).
// Produces a deterministic (seeded), realistic Add/Cancel/Execute/Replace
// message stream across a configured symbol set, with per-shard sequence
// numbers (matching architecture_spec.md Section 3 -- real exchange
// multicast channels are typically split the same way we split shards,
// and this models that by hashing symbols the SAME way shard_demux.hpp
// does) and a configurable fraction of packets silently lost in transit --
// exactly what real UDP multicast exhibits, and exactly what Phase 4's gap
// detector exists to handle.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <unordered_map>
#include <vector>

#include "protocol/message_types.hpp"

namespace mdfh::io {

class FeedGenerator {
public:
    struct Config {
        std::vector<protocol::Symbol> symbols;
        std::size_t num_shards;
        std::size_t message_count;
        double packet_loss_rate = 0.0;  // [0.0, 1.0)
        std::uint64_t seed      = 42;
    };

    explicit FeedGenerator(Config config);

    struct Packet {
        bool exhausted = false;                        // true: no more messages, ever
        std::optional<std::vector<std::byte>> bytes;    // nullopt: this one was lost in transit
    };

    // Generates (and possibly drops) the next packet. Not thread-safe --
    // meant to be driven by a single reader (see async_network_reader.hpp).
    Packet next();

    std::size_t total_generated() const noexcept { return emitted_count_; }
    std::size_t total_dropped() const noexcept { return total_dropped_; }

private:
    std::vector<std::byte> generate_next_message();

    Config config_;
    std::mt19937_64 rng_;
    std::size_t emitted_count_ = 0;
    std::size_t total_dropped_ = 0;
    protocol::OrderId next_order_id_ = 1;  // globally unique across all symbols, matching the routing table's assumption
    std::unordered_map<std::size_t, protocol::SeqNum> next_seq_per_shard_;
    std::vector<std::vector<protocol::OrderId>> resting_orders_per_symbol_;  // indexed like config_.symbols
};

}  // namespace mdfh::io
