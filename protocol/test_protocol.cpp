#include <array>
#include <cstddef>
#include <span>

#include <catch2/catch_test_macros.hpp>

#include "protocol/decoder.hpp"
#include "protocol/encoder.hpp"

using namespace mdfh::protocol;

namespace {

AddOrder sample_add_order() {
    return AddOrder{
        .seq_num   = 42,
        .order_id  = 1001,
        .symbol    = make_symbol("AAPL"),
        .side      = Side::Buy,
        .price     = 1'500'000,
        .quantity  = 100,
        .timestamp = 1'720'000'000'000ULL,
    };
}

CancelOrder sample_cancel_order() {
    return CancelOrder{
        .seq_num   = 43,
        .order_id  = 1001,
        .timestamp = 1'720'000'000'100ULL,
    };
}

ExecuteOrder sample_execute_order() {
    return ExecuteOrder{
        .seq_num           = 44,
        .order_id          = 1001,
        .executed_quantity = 50,
        .timestamp         = 1'720'000'000'200ULL,
    };
}

ReplaceOrder sample_replace_order() {
    return ReplaceOrder{
        .seq_num      = 45,
        .old_order_id = 1001,
        .new_order_id = 1002,
        .price        = 1'510'000,
        .quantity     = 75,
        .timestamp    = 1'720'000'000'300ULL,
    };
}

}  // namespace

TEST_CASE("AddOrder round-trips through encode/decode", "[protocol]") {
    auto original = sample_add_order();
    std::array<std::byte, AddOrder::kWireSize> buffer{};

    auto encoded_size = encode(original, buffer);
    REQUIRE(encoded_size.has_value());
    REQUIRE(*encoded_size == AddOrder::kWireSize);

    auto decoded = decode(std::span<const std::byte>(buffer));
    REQUIRE(decoded.has_value());
    REQUIRE(std::holds_alternative<AddOrder>(*decoded));
    REQUIRE(std::get<AddOrder>(*decoded) == original);
}

TEST_CASE("CancelOrder round-trips through encode/decode", "[protocol]") {
    auto original = sample_cancel_order();
    std::array<std::byte, CancelOrder::kWireSize> buffer{};

    REQUIRE(encode(original, buffer).has_value());

    auto decoded = decode(std::span<const std::byte>(buffer));
    REQUIRE(decoded.has_value());
    REQUIRE(std::get<CancelOrder>(*decoded) == original);
}

TEST_CASE("ExecuteOrder round-trips through encode/decode", "[protocol]") {
    auto original = sample_execute_order();
    std::array<std::byte, ExecuteOrder::kWireSize> buffer{};

    REQUIRE(encode(original, buffer).has_value());

    auto decoded = decode(std::span<const std::byte>(buffer));
    REQUIRE(decoded.has_value());
    REQUIRE(std::get<ExecuteOrder>(*decoded) == original);
}

TEST_CASE("ReplaceOrder round-trips through encode/decode", "[protocol]") {
    auto original = sample_replace_order();
    std::array<std::byte, ReplaceOrder::kWireSize> buffer{};

    REQUIRE(encode(original, buffer).has_value());

    auto decoded = decode(std::span<const std::byte>(buffer));
    REQUIRE(decoded.has_value());
    REQUIRE(std::get<ReplaceOrder>(*decoded) == original);
}

TEST_CASE("encode reports BufferTooSmall instead of overflowing the caller's buffer", "[protocol]") {
    auto original = sample_add_order();
    std::array<std::byte, AddOrder::kWireSize - 1> buffer{};

    auto result = encode(original, buffer);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == EncodeError::BufferTooSmall);
}

TEST_CASE("decode rejects a message truncated mid-payload", "[protocol]") {
    auto original = sample_add_order();
    std::array<std::byte, AddOrder::kWireSize> buffer{};
    REQUIRE(encode(original, buffer).has_value());

    // Simulate a packet cut short on the wire.
    auto truncated = std::span<const std::byte>(buffer).first(AddOrder::kWireSize - 5);
    auto decoded   = decode(truncated);

    REQUIRE_FALSE(decoded.has_value());
    REQUIRE(decoded.error() == DecodeError::TruncatedMessage);
}

TEST_CASE("decode rejects a message shorter than the header itself", "[protocol]") {
    std::array<std::byte, 2> buffer{};  // header is 3 bytes
    auto decoded = decode(std::span<const std::byte>(buffer));

    REQUIRE_FALSE(decoded.has_value());
    REQUIRE(decoded.error() == DecodeError::TruncatedMessage);
}

TEST_CASE("decode rejects an invalid message type byte", "[protocol]") {
    std::array<std::byte, AddOrder::kWireSize> buffer{};
    buffer[0] = std::byte{0xFF};  // no MessageType is defined as 0xFF

    auto decoded = decode(std::span<const std::byte>(buffer));

    REQUIRE_FALSE(decoded.has_value());
    REQUIRE(decoded.error() == DecodeError::InvalidMessageType);
}

TEST_CASE("decode rejects a length field inconsistent with its message type", "[protocol]") {
    auto original = sample_add_order();
    std::array<std::byte, AddOrder::kWireSize> buffer{};
    REQUIRE(encode(original, buffer).has_value());

    // Corrupt the length field (bytes 1-2) to claim a bogus size.
    buffer[1] = std::byte{0xFF};
    buffer[2] = std::byte{0xFF};

    auto decoded = decode(std::span<const std::byte>(buffer));

    REQUIRE_FALSE(decoded.has_value());
    REQUIRE(decoded.error() == DecodeError::LengthMismatch);
}
