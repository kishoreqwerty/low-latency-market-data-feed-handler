#pragma once

// Fixed-capacity pool allocator for order-book node objects (list nodes, map
// nodes, hash-table nodes). All Capacity blocks live in a single static
// array threaded into an intrusive free list; allocate()/deallocate() after
// that never touch the heap. This is deliberately usable as a real C++
// Allocator (see PoolAllocator below), so std::list/std::map/std::unordered_map
// can be handed it directly as their allocator template argument -- the
// containers then rebind it to whatever internal node type they actually
// allocate, and the pool sizes itself to that rebound type automatically.
//
// Per-shard pools, no synchronization (revised after Phase 7): the
// original design (Phase 3) had exactly one pool per node type, shared as
// a process-wide singleton across every OrderBook instance, protected by a
// mutex once Phase 7 introduced genuine concurrent shard threads. That
// mutex is gone. Sharding by symbol at ingestion exists specifically to
// give each shard's decoder thread an independent, contention-free
// pipeline -- a shared pool behind a mutex silently reintroduced the exact
// cross-shard lock contention that design was meant to eliminate, just one
// level lower than the ring buffers. The fix is a genuinely separate pool
// per shard, so each shard's decoder thread only ever touches memory no
// other thread touches, and no lock is needed at all.
//
// Mechanically: PoolAllocator<T, Capacity>::pool() now returns one of up to
// kMaxShards independent FixedBlockPool instances for a given node type T,
// selected by a thread_local "which shard am I" index (see
// current_shard_index / set_current_shard_index below) rather than a
// single global instance. A thread_local index, not a constructor
// parameter threaded through PoolAllocator, is the mechanism because
// std::map::operator[] (used for e.g. bids_[price] to insert a new price
// Level) default-constructs the mapped Level -- including its nested
// OrderList's own allocator -- with no way to pass extra construction
// context through. Each shard decoder thread calls
// set_current_shard_index() exactly once, at thread start (see
// io/pipeline_runner.cpp); from then on every PoolAllocator on that thread
// transparently resolves to that shard's own pools. This is safe without
// synchronization only because of that one-thread-per-index invariant:
// pools[i] is only ever touched by whichever single thread called
// set_current_shard_index(i), so two different shards' pools for the same
// node type never share memory, and the lazy first-use construction of
// pools[i] is itself only ever performed by that one thread.

#include <array>
#include <cstddef>
#include <cstring>
#include <memory>
#include <new>

namespace mdfh::orderbook {

// Ceiling on how many independent per-shard pool slots exist. Must match
// (or exceed) orderbook::kMaxShards -- kept as its own constant here rather
// than including order_book.hpp, since object_pool.hpp is the
// lower-level, more general building block.
inline constexpr std::size_t kMaxPoolShards = 16;

// Set once per shard decoder thread, at thread start, before that thread
// constructs any OrderBook. Defaults to 0, which is fine for
// single-threaded use (tests, benchmarks) that never call this at all --
// they all simply share pool slot 0, which is safe precisely because
// they're single-threaded (no concurrent access to that slot).
inline thread_local std::size_t current_shard_index = 0;

inline void set_current_shard_index(std::size_t index) noexcept { current_shard_index = index; }

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

    // No locking: a given FixedBlockPool instance is only ever reached via
    // PoolAllocator::pool(), which selects it by current_shard_index --
    // exactly one thread ever calls allocate()/deallocate() on any one
    // instance, for the lifetime of the process.
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
// Capacity, current_shard_index) combination. Each per-shard pool is a
// function-local static, lazily constructed (its Capacity blocks reserved)
// the first time that shard's thread actually allocates that type -- not
// eagerly at process start, but still well before steady-state message
// traffic on that shard, and never again afterward.
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

    // Diagnostics only -- not used on the hot path. Reflect whichever
    // shard's pool the calling thread currently resolves to.
    static std::size_t capacity() noexcept { return pool().capacity(); }
    static std::size_t in_use() noexcept { return pool().in_use(); }

private:
    using Pool = FixedBlockPool<sizeof(T), alignof(T), Capacity>;

    static Pool& pool() {
        // One lazily-constructed pool per shard slot. Safe without a lock
        // only because slot i is, by construction (see
        // set_current_shard_index's call sites), ever touched by one
        // specific thread -- see the file-level comment for the full
        // argument.
        static std::array<std::unique_ptr<Pool>, kMaxPoolShards> pools;
        std::unique_ptr<Pool>& slot = pools.at(current_shard_index);
        if (!slot) {
            slot = std::make_unique<Pool>();
        }
        return *slot;
    }
};

}  // namespace mdfh::orderbook
