#pragma once

#include <cstddef>
#include <expected>
#include <span>

#include "protocol/message_types.hpp"

namespace mdfh::protocol {

enum class EncodeError {
    BufferTooSmall,
};

// Each overload packs `msg` into `out` as [type][length][payload] and
// returns the number of bytes written (always the type's kWireSize).
std::expected<std::size_t, EncodeError> encode(const AddOrder& msg, std::span<std::byte> out);
std::expected<std::size_t, EncodeError> encode(const CancelOrder& msg, std::span<std::byte> out);
std::expected<std::size_t, EncodeError> encode(const ExecuteOrder& msg, std::span<std::byte> out);
std::expected<std::size_t, EncodeError> encode(const ReplaceOrder& msg, std::span<std::byte> out);

}  // namespace mdfh::protocol
