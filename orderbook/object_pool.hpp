#pragma once

// Fixed-capacity pool allocator for order-book node objects (list nodes, map
// nodes, hash-table nodes). All Capacity blocks live in a single static
// array threaded into an intrusive free list; allocate()/deallocate() after
// that never touch the heap. This is deliberately usable as a real C++
// Allocator (see PoolAllocator below), so std::list/std::map/std::unordered_map
// can be handed it directly as their allocator template argument -- the
// containers then rebind it to whatever internal node type they actually
// allocate, and the pool sizes itself to that rebound type automatically.

#include <array>
#include <cstddef>
#include <cstring>
#include <memory>
#include <new>

namespace mdfh::orderbook {

template <std::size_t BlockSize, std::size_t Alignment, std::size_t Capacity>
class FixedBlockPool {
    static_assert(Capacity > 0, "FixedBlockPool requires a nonzero capacity");

    // The free list is threaded through the blocks themselves (each free
    // block's first bytes hold a pointer to the next free block), so every
    // block must be at least pointer-sized/aligned.
    static constexpr std::size_t kStride    = BlockSize > sizeof(void*) ? BlockSize : sizeof(void*);
    static constexpr std::size_t kAlignment = Alignment > alignof(void*) ? Alignment : alignof(void*);

public:
    FixedBlockPool() noexcept {
        for (std::size_t i = 0; i + 1 < Capacity; ++i) {
            write_next(slot(i), slot(i + 1));
        }
        write_next(slot(Capacity - 1), nullptr);
        free_list_ = slot(0);
    }

    FixedBlockPool(const FixedBlockPool&)            = delete;
    FixedBlockPool& operator=(const FixedBlockPool&) = delete;

    void* allocate() {
        if (free_list_ == nullptr) {
            throw std::bad_alloc();
        }
        void* block = free_list_;
        free_list_  = read_next(block);
        ++in_use_;
        return block;
    }

    void deallocate(void* p) noexcept {
        write_next(p, free_list_);
        free_list_ = p;
        --in_use_;
    }

    static constexpr std::size_t capacity() noexcept { return Capacity; }
    std::size_t in_use() const noexcept { return in_use_; }

private:
    void* slot(std::size_t index) noexcept { return storage_.data() + index * kStride; }

    // Reading/writing the free-list pointer through memcpy (rather than a
    // reinterpret_cast<void**> dereference) avoids relying on the strict
    // aliasing / effective-type rules for raw storage that hasn't had an
    // object constructed in it yet.
    static void write_next(void* block, void* next) noexcept { std::memcpy(block, &next, sizeof(void*)); }
    static void* read_next(void* block) noexcept {
        void* next;
        std::memcpy(&next, block, sizeof(void*));
        return next;
    }

    alignas(kAlignment) std::array<std::byte, kStride * Capacity> storage_;
    void* free_list_    = nullptr;
    std::size_t in_use_ = 0;
};

// A minimal C++ Allocator backed by one FixedBlockPool per (rebound type,
// Capacity) pair. The pool is a function-local static, so it's constructed
// (and its Capacity blocks reserved) the first time a container actually
// allocates that type -- not eagerly at process start, but still well
// before any steady-state message traffic, and never again afterward.
//
// Known limitation: the pool is a process-wide singleton keyed purely by
// type, not by which OrderBook instance is using it. With a single book (as
// through Phase 5), that's equivalent to a per-book cap. Once Phase 6 adds
// multiple concurrent per-shard books, they will all draw from the *same*
// shared pool of a given node type -- fine if Capacity is sized for the
// system total, but worth revisiting (e.g. injecting a pool reference per
// book) if per-shard isolation turns out to matter.
template <typename T, std::size_t Capacity>
class PoolAllocator {
public:
    using value_type = T;

    // libc++ (unlike libstdc++) requires allocators to declare rebind
    // explicitly rather than inferring it from the Alloc<T, Args...> shape,
    // since map/list/unordered_map all need to allocate their own internal
    // node type, not T itself.
    template <typename U>
    struct rebind {
        using other = PoolAllocator<U, Capacity>;
    };

    PoolAllocator() noexcept = default;

    template <typename U>
    PoolAllocator(const PoolAllocator<U, Capacity>&) noexcept {}

    T* allocate(std::size_t n) {
        if (n == 1) {
            return static_cast<T*>(pool().allocate());
        }
        // Node-based containers (list/map/unordered_map's per-element
        // nodes) only ever request one node at a time. A request for more
        // than one element is a bulk allocation -- e.g. unordered_map's
        // bucket array growing on rehash -- which this fixed pool isn't
        // sized for. That path only fires when a container structurally
        // grows, not per message, so falling back to the global allocator
        // here is a deliberate, documented exception, not a silent gap.
        return std::allocator<T>{}.allocate(n);
    }

    void deallocate(T* p, std::size_t n) noexcept {
        if (n == 1) {
            pool().deallocate(p);
            return;
        }
        std::allocator<T>{}.deallocate(p, n);
    }

    template <typename U>
    bool operator==(const PoolAllocator<U, Capacity>&) const noexcept {
        return true;  // stateless: matches std::allocator's own equality contract
    }

    // Diagnostics only -- not used on the hot path.
    static std::size_t capacity() noexcept { return pool().capacity(); }
    static std::size_t in_use() noexcept { return pool().in_use(); }

private:
    static FixedBlockPool<sizeof(T), alignof(T), Capacity>& pool() {
        static FixedBlockPool<sizeof(T), alignof(T), Capacity> instance;
        return instance;
    }
};

}  // namespace mdfh::orderbook
