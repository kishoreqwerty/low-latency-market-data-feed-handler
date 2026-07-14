#pragma once

#include <cstddef>
#include <expected>
#include <span>
#include <variant>

#include "protocol/message_types.hpp"

namespace mdfh::protocol {

enum class DecodeError {
    TruncatedMessage,     // fewer bytes available than the header or declared length require
    InvalidMessageType,   // type byte doesn't match any known MessageType
    LengthMismatch,       // declared length doesn't match the fixed size for that type
};

using DecodedMessage = std::variant<AddOrder, CancelOrder, ExecuteOrder, ReplaceOrder>;

// Parses [type][length][payload...] out of `in`. Never reads past `in.size()`
// and never throws on malformed input -- callers get a DecodeError instead.
std::expected<DecodedMessage, DecodeError> decode(std::span<const std::byte> in);

}  // namespace mdfh::protocol
