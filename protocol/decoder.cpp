#include "protocol/decoder.hpp"

#include <utility>

#include "protocol/byte_order.hpp"

namespace mdfh::protocol {

namespace {

// nullopt means the raw byte doesn't correspond to any known MessageType.
std::expected<std::size_t, DecodeError> wire_size_for(uint8_t raw_type) {
    switch (static_cast<MessageType>(raw_type)) {
        case MessageType::AddOrder:
            return AddOrder::kWireSize;
        case MessageType::CancelOrder:
            return CancelOrder::kWireSize;
        case MessageType::ExecuteOrder:
            return ExecuteOrder::kWireSize;
        case MessageType::ReplaceOrder:
            return ReplaceOrder::kWireSize;
    }
    return std::unexpected(DecodeError::InvalidMessageType);
}

AddOrder decode_add_order(const std::byte* p) {
    AddOrder msg{};
    msg.seq_num = detail::get_u64(p);
    p += 8;
    msg.order_id = detail::get_u64(p);
    p += 8;
    for (char& c : msg.symbol) {
        c = static_cast<char>(detail::get_u8(p));
        p += 1;
    }
    msg.side = static_cast<Side>(detail::get_u8(p));
    p += 1;
    msg.price = detail::get_i64(p);
    p += 8;
    msg.quantity = detail::get_u32(p);
    p += 4;
    msg.timestamp = detail::get_u64(p);
    return msg;
}

CancelOrder decode_cancel_order(const std::byte* p) {
    CancelOrder msg{};
    msg.seq_num = detail::get_u64(p);
    p += 8;
    msg.order_id = detail::get_u64(p);
    p += 8;
    msg.timestamp = detail::get_u64(p);
    return msg;
}

ExecuteOrder decode_execute_order(const std::byte* p) {
    ExecuteOrder msg{};
    msg.seq_num = detail::get_u64(p);
    p += 8;
    msg.order_id = detail::get_u64(p);
    p += 8;
    msg.executed_quantity = detail::get_u32(p);
    p += 4;
    msg.timestamp = detail::get_u64(p);
    return msg;
}

ReplaceOrder decode_replace_order(const std::byte* p) {
    ReplaceOrder msg{};
    msg.seq_num = detail::get_u64(p);
    p += 8;
    msg.old_order_id = detail::get_u64(p);
    p += 8;
    msg.new_order_id = detail::get_u64(p);
    p += 8;
    msg.price = detail::get_i64(p);
    p += 8;
    msg.quantity = detail::get_u32(p);
    p += 4;
    msg.timestamp = detail::get_u64(p);
    return msg;
}

}  // namespace

std::expected<DecodedMessage, DecodeError> decode(std::span<const std::byte> in) {
    if (in.size() < kHeaderSize) {
        return std::unexpected(DecodeError::TruncatedMessage);
    }

    const std::byte* p       = in.data();
    const uint8_t raw_type   = detail::get_u8(p);
    const uint16_t length    = detail::get_u16(p + 1);

    auto expected_size = wire_size_for(raw_type);
    if (!expected_size) {
        return std::unexpected(expected_size.error());
    }
    if (length != *expected_size) {
        return std::unexpected(DecodeError::LengthMismatch);
    }
    if (in.size() < length) {
        return std::unexpected(DecodeError::TruncatedMessage);
    }

    const std::byte* payload = p + kHeaderSize;
    switch (static_cast<MessageType>(raw_type)) {
        case MessageType::AddOrder:
            return DecodedMessage{decode_add_order(payload)};
        case MessageType::CancelOrder:
            return DecodedMessage{decode_cancel_order(payload)};
        case MessageType::ExecuteOrder:
            return DecodedMessage{decode_execute_order(payload)};
        case MessageType::ReplaceOrder:
            return DecodedMessage{decode_replace_order(payload)};
    }

    // Every case of this exhaustive switch returns. Any raw_type that isn't
    // one of the four MessageType enumerators was already rejected above by
    // wire_size_for(), which fails closed (its own switch has no default,
    // so an unmatched byte falls through to its InvalidMessageType return).
    std::unreachable();
}

}  // namespace mdfh::protocol
