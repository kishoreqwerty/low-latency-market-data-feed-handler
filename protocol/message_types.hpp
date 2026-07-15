#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace mdfh::protocol {

enum class MessageType : uint8_t {
    AddOrder     = 1,
    CancelOrder  = 2,
    ExecuteOrder = 3,
    ReplaceOrder = 4,
};

enum class Side : uint8_t {
    Buy  = 0,
    Sell = 1,
};

using SeqNum    = uint64_t;
using OrderId   = uint64_t;
using Price     = int64_t;   // ticks, scale defined by the feed generator
using Quantity  = uint32_t;
using Timestamp = uint64_t;  // nanoseconds since epoch

inline constexpr std::size_t kSymbolLen = 8;
using Symbol = std::array<char, kSymbolLen>;

constexpr Symbol make_symbol(std::string_view s) {
    Symbol sym{};
    for (std::size_t i = 0; i < sym.size() && i < s.size(); ++i) {
        sym[i] = s[i];
    }
    return sym;
}

// Hashes a Symbol's raw bytes (FNV-1a). std::array has no standard library
// hash specialization, and this needs to be deterministic and
// platform-independent (not implementation-defined, the way std::hash's
// output is) so shard assignment (io/shard_demux.hpp) is stable across
// builds and platforms.
constexpr std::size_t hash_symbol(const Symbol& symbol) noexcept {
    std::uint64_t hash = 1469598103934665603ull;  // FNV-1a offset basis
    for (char c : symbol) {
        hash ^= static_cast<unsigned char>(c);
        hash *= 1099511628211ull;  // FNV-1a prime
    }
    return static_cast<std::size_t>(hash);
}

struct SymbolHash {
    std::size_t operator()(const Symbol& symbol) const noexcept { return hash_symbol(symbol); }
};

// Every wire message is [type: u8][length: u16][payload...]; length covers
// the whole message (header included) so the decoder can validate it.
inline constexpr std::size_t kHeaderSize = 3;

struct AddOrder {
    static constexpr MessageType kType        = MessageType::AddOrder;
    static constexpr std::size_t kPayloadSize = 8 + 8 + kSymbolLen + 1 + 8 + 4 + 8;  // 45
    static constexpr std::size_t kWireSize    = kHeaderSize + kPayloadSize;

    SeqNum seq_num;
    OrderId order_id;
    Symbol symbol;
    Side side;
    Price price;
    Quantity quantity;
    Timestamp timestamp;

    friend bool operator==(const AddOrder&, const AddOrder&) = default;
};

struct CancelOrder {
    static constexpr MessageType kType        = MessageType::CancelOrder;
    static constexpr std::size_t kPayloadSize = 8 + 8 + 8;  // 24
    static constexpr std::size_t kWireSize    = kHeaderSize + kPayloadSize;

    SeqNum seq_num;
    OrderId order_id;
    Timestamp timestamp;

    friend bool operator==(const CancelOrder&, const CancelOrder&) = default;
};

struct ExecuteOrder {
    static constexpr MessageType kType        = MessageType::ExecuteOrder;
    static constexpr std::size_t kPayloadSize = 8 + 8 + 4 + 8;  // 28
    static constexpr std::size_t kWireSize    = kHeaderSize + kPayloadSize;

    SeqNum seq_num;
    OrderId order_id;
    Quantity executed_quantity;
    Timestamp timestamp;

    friend bool operator==(const ExecuteOrder&, const ExecuteOrder&) = default;
};

struct ReplaceOrder {
    static constexpr MessageType kType        = MessageType::ReplaceOrder;
    static constexpr std::size_t kPayloadSize = 8 + 8 + 8 + 8 + 4 + 8;  // 44
    static constexpr std::size_t kWireSize    = kHeaderSize + kPayloadSize;

    SeqNum seq_num;
    OrderId old_order_id;
    OrderId new_order_id;
    Price price;
    Quantity quantity;
    Timestamp timestamp;

    friend bool operator==(const ReplaceOrder&, const ReplaceOrder&) = default;
};

}  // namespace mdfh::protocol
