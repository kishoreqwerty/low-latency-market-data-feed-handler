#include "protocol/encoder.hpp"

#include "protocol/byte_order.hpp"

namespace mdfh::protocol {

namespace {

std::byte* put_header(std::byte* p, MessageType type, uint16_t length) {
    detail::put_u8(p, static_cast<uint8_t>(type));
    detail::put_u16(p + 1, length);
    return p + kHeaderSize;
}

}  // namespace

std::expected<std::size_t, EncodeError> encode(const AddOrder& msg, std::span<std::byte> out) {
    if (out.size() < AddOrder::kWireSize) {
        return std::unexpected(EncodeError::BufferTooSmall);
    }

    std::byte* p = put_header(out.data(), AddOrder::kType, static_cast<uint16_t>(AddOrder::kWireSize));

    detail::put_u64(p, msg.seq_num);
    p += 8;
    detail::put_u64(p, msg.order_id);
    p += 8;
    for (char c : msg.symbol) {
        detail::put_u8(p, static_cast<uint8_t>(c));
        p += 1;
    }
    detail::put_u8(p, static_cast<uint8_t>(msg.side));
    p += 1;
    detail::put_i64(p, msg.price);
    p += 8;
    detail::put_u32(p, msg.quantity);
    p += 4;
    detail::put_u64(p, msg.timestamp);
    p += 8;

    return AddOrder::kWireSize;
}

std::expected<std::size_t, EncodeError> encode(const CancelOrder& msg, std::span<std::byte> out) {
    if (out.size() < CancelOrder::kWireSize) {
        return std::unexpected(EncodeError::BufferTooSmall);
    }

    std::byte* p = put_header(out.data(), CancelOrder::kType, static_cast<uint16_t>(CancelOrder::kWireSize));

    detail::put_u64(p, msg.seq_num);
    p += 8;
    detail::put_u64(p, msg.order_id);
    p += 8;
    detail::put_u64(p, msg.timestamp);
    p += 8;

    return CancelOrder::kWireSize;
}

std::expected<std::size_t, EncodeError> encode(const ExecuteOrder& msg, std::span<std::byte> out) {
    if (out.size() < ExecuteOrder::kWireSize) {
        return std::unexpected(EncodeError::BufferTooSmall);
    }

    std::byte* p = put_header(out.data(), ExecuteOrder::kType, static_cast<uint16_t>(ExecuteOrder::kWireSize));

    detail::put_u64(p, msg.seq_num);
    p += 8;
    detail::put_u64(p, msg.order_id);
    p += 8;
    detail::put_u32(p, msg.executed_quantity);
    p += 4;
    detail::put_u64(p, msg.timestamp);
    p += 8;

    return ExecuteOrder::kWireSize;
}

std::expected<std::size_t, EncodeError> encode(const ReplaceOrder& msg, std::span<std::byte> out) {
    if (out.size() < ReplaceOrder::kWireSize) {
        return std::unexpected(EncodeError::BufferTooSmall);
    }

    std::byte* p = put_header(out.data(), ReplaceOrder::kType, static_cast<uint16_t>(ReplaceOrder::kWireSize));

    detail::put_u64(p, msg.seq_num);
    p += 8;
    detail::put_u64(p, msg.old_order_id);
    p += 8;
    detail::put_u64(p, msg.new_order_id);
    p += 8;
    detail::put_i64(p, msg.price);
    p += 8;
    detail::put_u32(p, msg.quantity);
    p += 4;
    detail::put_u64(p, msg.timestamp);
    p += 8;

    return ReplaceOrder::kWireSize;
}

}  // namespace mdfh::protocol
