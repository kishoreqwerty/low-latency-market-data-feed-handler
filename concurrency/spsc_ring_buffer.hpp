#pragma once

// Bounded single-producer/single-consumer ring buffer with a drop-oldest
// backpressure policy (architecture_spec.md Section 6).
//
// Why this isn't the textbook Folly/Boost SPSC queue: those queues are
// genuinely lock-free because neither side ever writes the other's index --
// producer owns tail_, consumer owns head_, full stop, and push() simply
// fails when the buffer is full. That does not extend to drop-oldest.
//
// When the buffer is full, the slot the producer would overwrite
// (tail_ % Capacity) is mathematically the SAME slot the consumer is
// currently reading (head_ % Capacity) -- they're congruent mod Capacity
// exactly when full. A producer that overwrites that slot based on a
// slightly-stale read of head_ can race with the consumer's in-flight,
// unsynchronized read of that same memory. Compare-and-swapping head_ alone
// does not fix this: CAS-ing the index doesn't stop an already-in-flight
// plain read of the slot's data.
//
// The fix has three parts, all found by running the concurrent stress test
// under sustained overwhelming load -- not by inspection alone, and each
// one was a distinct bug the previous fix didn't cover:
//
// 1. compare_exchange on head_ is the FIRST action either side takes for a
//    given transition (head -> head+1) -- not a "check a flag, then CAS"
//    two-step. A check-then-CAS leaves a gap between the check and the CAS
//    during which the other side can start AND fully complete a data-safe
//    read before the CAS executes, so the CAS still succeeds even though
//    that item was already delivered -- double-counting it as both
//    delivered and dropped. Because compare_exchange on a given expected
//    value can only ever succeed for one caller, making it the first move
//    means "did a read happen" and "who claims this transition" can never
//    disagree.
// 2. try_pop() sets its per-slot `reader_active` flag BEFORE attempting the
//    CAS, not after winning it. Setting it only after winning leaves its
//    own gap: a producer that just lost its CAS for this transition checks
//    reader_active immediately afterward, and if the winner hadn't set it
//    yet, the producer sees it false and overwrites the slot before the
//    winner actually reads it -- handing the consumer data for a much
//    later item while it believes it's reading the one it claimed.
// 3. push() waits on reader_active for the slot it's about to write
//    UNCONDITIONALLY, not only in the branch where it lost the eviction
//    race for the *current* transition. Slots are reused every Capacity
//    transitions: winning the CAS proves nothing about whether a much
//    OLDER, still-in-flight read (from a full lap ago -- a slow consumer's
//    read takes real time even after it already won that transition's CAS)
//    has finished with this exact physical slot. Checking the flag only in
//    the "I lost this specific transition" branch missed that case
//    entirely.
//
// This makes push() NOT strictly lock-free in the formal sense: a producer
// can spin on a flag the consumer thread owns, so a pathologically
// descheduled consumer could stall a push. In practice the guarded critical
// section is a single plain struct copy (nanoseconds), so this is a
// bounded-spin, seqlock-style pattern -- the same family of technique used
// for similar overwrite-on-full scenarios elsewhere (e.g. the Linux kernel) --
// not a general-purpose lock. It is lock-free in the common (no in-flight
// collision) case; only that rare collision window spins.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace mdfh::concurrency {

template <typename T, std::size_t Capacity>
class SpscRingBuffer {
    static_assert(Capacity > 0, "SpscRingBuffer requires a nonzero capacity");

    struct Slot {
        T value{};
        std::atomic<bool> reader_active{false};
    };

public:
    SpscRingBuffer() = default;

    SpscRingBuffer(const SpscRingBuffer&)            = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;

    // Producer-side. Never blocks on an empty/non-full buffer and never
    // rejects an item: on overflow it evicts the oldest not-yet-consumed
    // entry (drop-oldest), so this always succeeds from the caller's view.
    void push(const T& item) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);  // own value, sole normal writer
        const std::size_t head = head_.load(std::memory_order_acquire);  // need a fresh view to detect overflow

        if (tail - head >= Capacity) {
            // Overflow: buffer[tail % Capacity] == buffer[head % Capacity]
            // (congruent mod Capacity exactly when full) -- about to
            // overwrite the oldest unconsumed entry.
            //
            // The CAS below -- not a "check reader_active, then CAS" -- is
            // what makes this correct. An earlier version checked
            // reader_active first and only CAS'd afterward; that left a gap
            // between the check and the CAS during which the consumer could
            // start AND fully complete its own read (data-safe, since
            // nothing had overwritten it yet) before this CAS executed --
            // so the producer's CAS would still succeed (head_ was still
            // unchanged) even though the consumer had already delivered
            // that exact item, double-counting it as both delivered and
            // dropped. Found empirically by the concurrent stress test
            // (see test_spsc_ring_buffer_stress.cpp), not by inspection.
            //
            // Making the CAS the FIRST action on both sides closes that
            // gap: compare_exchange on the same (expected == head) can only
            // ever succeed for one caller, so "did a read happen" and "who
            // claims this transition" can no longer disagree.
            std::size_t expected = head;
            if (head_.compare_exchange_strong(expected, head + 1, std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) {
                // Won: try_pop() cannot also win this exact transition, so
                // it will never read this slot -- a genuine drop.
                dropped_updates_total_.fetch_add(1, std::memory_order_relaxed);
            }
            // If lost, the consumer's own CAS (see try_pop) already claimed
            // this transition and may be reading right now -- handled by
            // the unconditional reader_active wait below, not here.
        }

        // Wait for any in-flight read of this physical slot before writing
        // -- unconditionally, not just when this push() lost an eviction
        // race. Slots are reused every Capacity transitions: a consumer can
        // legitimately still be reading a slot from a full lap ago (it won
        // its CAS, but the actual data read takes real time to execute)
        // while THIS push() wins a completely different, much later
        // transition that happens to map to the same physical slot. That
        // winning CAS says nothing about whether the old lap's read has
        // finished, so checking reader_active only in the "lost" branch
        // above missed this case entirely -- found empirically (delivered
        // values arriving out of order) after fixing the first two bugs.
        Slot& target = buffer_[tail % Capacity];
        while (target.reader_active.load(std::memory_order_acquire)) {
            // spin
        }

        target.value = item;
        tail_.store(tail + 1, std::memory_order_release);
    }

    // Consumer-side. Returns false if the buffer is currently empty (or if
    // a producer eviction won the race for the slot this call would have
    // read -- the caller's next call will see the new head and proceed).
    bool try_pop(T& out) {
        const std::size_t head = head_.load(std::memory_order_relaxed);  // best-effort starting guess
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        if (tail == head) {
            return false;  // empty relative to this guess
        }

        // Announce BEFORE attempting the CAS, not after winning it. A
        // producer that loses its own CAS for this same transition checks
        // reader_active immediately afterward; if we set the flag only
        // after winning, there's a gap where that producer can observe
        // reader_active still false (we hadn't set it yet) and proceed to
        // overwrite the slot before we actually read it -- handing us data
        // for a much later item under the belief we're reading `head`'s.
        // (Also found empirically, by this same stress test, after fixing
        // the first double-counting bug -- it manifested as delivered
        // values going backwards/out of order instead.)
        Slot& slot = buffer_[head % Capacity];
        slot.reader_active.store(true, std::memory_order_release);

        // Claim this transition (head -> head+1) via CAS BEFORE touching
        // any data. Exactly one of {this call, a concurrent producer
        // eviction in push()} can ever win a CAS with expected == head, so
        // winning here is proof no eviction can also claim -- and therefore
        // read the data -- for this exact item.
        std::size_t expected = head;
        if (!head_.compare_exchange_strong(expected, head + 1, std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
            slot.reader_active.store(false, std::memory_order_release);  // didn't win; retract the claim
            return false;  // a producer eviction already claimed this transition
        }

        // Won: safe to read. Any producer that lost the CAS above (racing
        // for this same transition) will see reader_active already true
        // (set before we even attempted the CAS) and wait for us.
        out = slot.value;
        slot.reader_active.store(false, std::memory_order_release);
        return true;
    }

    std::uint64_t dropped_updates_total() const noexcept {
        return dropped_updates_total_.load(std::memory_order_relaxed);
    }

    static constexpr std::size_t capacity() noexcept { return Capacity; }

private:
    std::array<Slot, Capacity> buffer_;  // fixed-size, no heap allocation

    // Each on its own cache line: tail_ is written every push, head_ every
    // pop (plus occasionally by push() on eviction) -- sharing a line
    // between producer- and consumer-touched atomics causes cache-line
    // ping-pong between cores on every single operation even though there's
    // no logical contention, which is the single most common SPSC
    // performance pitfall.
    alignas(64) std::atomic<std::size_t> tail_{0};
    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::uint64_t> dropped_updates_total_{0};
};

}  // namespace mdfh::concurrency
