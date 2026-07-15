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
//
// Phase 12: blocking consumer wait via std::atomic::wait/notify (C++20).
//
// Phase 10's latency instrumentation found that this pipeline's actual
// tick-to-book-update latency floor (~30-70us) was set almost entirely by
// the decoder/publisher threads' idle-wait poll (`sleep_for(50us)` in
// io/pipeline_runner.cpp when a queue was empty), not by anything in this
// ring buffer or the order book -- confirmed three independent ways (see
// docs/BENCHMARKS.md). wait_for_data()/notify_waiters() below replace that
// fixed-interval poll with a real OS-level block (futex on Linux, ulock on
// macOS, both reachable via std::atomic::wait/notify), so a consumer with
// nothing to do actually sleeps until woken instead of waking up to check
// every 50us regardless.
//
// Deliberately kept OUT of try_pop()/push()'s core contract: wait_for_data()
// is a separate, optional consumer-side call, and notify_waiters() is a
// separate producer-side (or external) call -- neither changes what
// try_pop() or push() themselves return, so every existing single-threaded
// test and the drop-oldest race-safety reasoning above is untouched. push()
// unconditionally calls tail_.notify_one() after its final store; that is
// the ONLY change to push() itself.
//
// Why waiting/notifying on tail_ ITSELF doesn't work, and wake_generation_
// exists instead -- found empirically, not by inspection, by a dedicated
// 2-thread test that deliberately forced the shutdown-notify path
// (test_spsc_ring_buffer_stress.cpp's "slowed consumer relies on the
// producer-done notify_waiters() wake" test hung indefinitely before this
// fix):
//
// std::atomic<T>::wait(old) is specified as a LOOP: compare the atomic's
// current value to `old`; if different, return; if the same, block until
// notified or spuriously woken, THEN GO BACK AND COMPARE AGAIN. That
// recheck is the whole point (it's what makes the check-then-block
// sequence race-free against a concurrent push()) -- but it also means a
// notify_one()/notify_all() call that does NOT change the watched value
// accomplishes nothing: the woken waiter re-compares, finds the value
// still equal to `old`, and goes right back to sleep. notify_waiters()
// exists specifically for the case where nothing was pushed (e.g. "the
// producer just finished, wake up and check your own shutdown flag") --
// tail_ genuinely does NOT change in that case, so waiting/notifying on
// tail_ itself cannot ever wake that call.
//
// wake_generation_ fixes this by being a value that is UNCONDITIONALLY
// incremented on every event that should wake a waiter, whether or not
// tail_ changed: both push() (real data) and notify_waiters() (an
// external/shutdown signal) increment it before notifying, so `wait_for_
// data()`'s recheck-after-wake always sees a genuinely different value
// and correctly returns instead of re-sleeping. tail_/head_ themselves are
// untouched by any of this -- they remain exactly the producer/consumer
// index pair the drop-oldest reasoning above depends on; wake_generation_
// is purely a wake signal layered on top, never consulted for queue logic.

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

        // Wake a consumer blocked in wait_for_data(), if any. Unconditional
        // (not gated on "is anyone actually waiting") -- notify_one() on an
        // atomic nobody is waiting on is a cheap, non-blocking check on both
        // target platforms, and SPSC means at most one thread could ever be
        // waiting here, so there's no "wake the wrong one" cost to avoid
        // either. Signaled via wake_generation_, not tail_ -- see this
        // file's header comment for why waiting/notifying on tail_ itself
        // doesn't work for the notify_waiters() (no-new-data) case, which
        // is why this increments a value that ALWAYS changes rather than
        // notifying on tail_ directly.
        wake_generation_.fetch_add(1, std::memory_order_release);
        wake_generation_.notify_one();
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

    // Blocks (a real OS-level wait, not a spin or a timed poll) until
    // either an item becomes available or notify_waiters() is called
    // externally. Does not itself pop anything, and does not know or care
    // about any shutdown condition -- purely a wake primitive. The caller
    // decides what to do after waking: check try_pop() again, check its own
    // shutdown flag, or both (see io/pipeline_runner.cpp's decoder/publisher
    // loops for the actual protocol built on top of this).
    //
    // Waits on wake_generation_, NOT tail_ -- see this file's header
    // comment for why: push() and notify_waiters() both unconditionally
    // increment wake_generation_ before notifying, so it's guaranteed to
    // differ from any previously-observed snapshot on every wake-worthy
    // event, including the "nothing was pushed, just wake up" case tail_
    // alone cannot signal. Race-free by the same construction as before
    // (re-checking immediately before the wait() call, then passing that
    // SAME snapshot to wait(), so a concurrent increment can never be
    // missed) -- just applied to a value that always changes when it should,
    // rather than one (tail_) that sometimes doesn't.
    void wait_for_data() const noexcept {
        if (tail_.load(std::memory_order_acquire) != head_.load(std::memory_order_relaxed)) {
            return;  // already something to read -- don't block at all
        }
        const std::uint64_t observed_generation = wake_generation_.load(std::memory_order_acquire);
        // Re-check emptiness against the CURRENT queue state after
        // snapshotting the generation, in case a push() completed (and
        // bumped the generation) between the check above and this
        // snapshot -- otherwise this could snapshot a generation that's
        // already stale relative to data that's already sitting there.
        if (tail_.load(std::memory_order_acquire) != head_.load(std::memory_order_relaxed)) {
            return;
        }
        wake_generation_.wait(observed_generation, std::memory_order_acquire);
    }

    // Wakes any thread currently blocked in wait_for_data() on this queue,
    // even though nothing was actually pushed -- needed so a consumer can
    // observe an externally-signaled condition (e.g. "the producer just
    // finished, stop waiting and check your own shutdown flag") promptly,
    // rather than only on the next real push(). Safe to call whether or not
    // anything is currently waiting. Must increment wake_generation_ (not
    // just notify) -- see this file's header comment for why a notify with
    // no corresponding value change is silently ineffective against
    // atomic::wait's recheck-on-wake semantics.
    void notify_waiters() noexcept {
        wake_generation_.fetch_add(1, std::memory_order_release);
        wake_generation_.notify_one();
    }

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

    // Phase 12: the actual value wait_for_data()/notify_waiters() wait and
    // notify on -- see this file's header comment for why it can't just be
    // tail_. On its own cache line for the same reason as the others: it's
    // written by every push() (producer) and every notify_waiters() call
    // (whichever thread signals shutdown, typically the far-side producer
    // of the NEXT stage), read/waited-on by the consumer.
    alignas(64) std::atomic<std::uint64_t> wake_generation_{0};
};

}  // namespace mdfh::concurrency
