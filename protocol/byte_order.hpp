#pragma once

// Explicit little-endian field packing, used internally by encoder.cpp and
// decoder.cpp. Written by hand instead of memcpy'ing structs directly so the
// wire layout never depends on compiler padding/alignment or host endianness.

#include <cstddef>
#include <cstdint>

namespace mdfh::protocol::detail {

inline void put_u8(std::byte* p, uint8_t v) {
    p[0] = static_cast<std::byte>(v);
}

inline uint8_t get_u8(const std::byte* p) {
    return std::to_integer<uint8_t>(p[0]);
}

inline void put_u16(std::byte* p, uint16_t v) {
    p[0] = static_cast<std::byte>(v & 0xFF);
    p[1] = static_cast<std::byte>((v >> 8) & 0xFF);
}

inline uint16_t get_u16(const std::byte* p) {
    return static_cast<uint16_t>(static_cast<uint16_t>(get_u8(p)) |
                                  (static_cast<uint16_t>(get_u8(p + 1)) << 8));
}

inline void put_u32(std::byte* p, uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        p[i] = static_cast<std::byte>((v >> (8 * i)) & 0xFF);
    }
}

inline uint32_t get_u32(const std::byte* p) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<uint32_t>(get_u8(p + i)) << (8 * i);
    }
    return v;
}

inline void put_u64(std::byte* p, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        p[i] = static_cast<std::byte>((v >> (8 * i)) & 0xFF);
    }
}

inline uint64_t get_u64(const std::byte* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(get_u8(p + i)) << (8 * i);
    }
    return v;
}

inline void put_i64(std::byte* p, int64_t v) {
    put_u64(p, static_cast<uint64_t>(v));
}

inline int64_t get_i64(const std::byte* p) {
    return static_cast<int64_t>(get_u64(p));
}

}  // namespace mdfh::protocol::detail
