#pragma once

// Per-shard sequence gap detector (architecture_spec.md Section 3). Phase 6
// gave each shard its own instance. Not wired to OrderBook directly --
// that wiring (gap detector + ring buffer + book, one set per shard) is
// io/shard_demux.hpp's job.
//
// Thread-safety contract (added in Phase 7, when a shard's own decoder
// thread starts calling observe() while other threads -- e.g. a test or a
// future monitoring/publisher thread -- read state() concurrently): all
// fields are atomic, so any number of readers can safely call
// state()/is_stale()/sequence_gaps_total()/last_seq_num() while a single
// writer thread calls observe() or resync(). What this does NOT make safe
// is observe() and resync() being called concurrently by two DIFFERENT
// threads at the same time -- each individually updates multiple related
// fields (e.g. observe() updates last_seq_num_ then state_), and an
// interleaved resync() from another thread could observe or clobber an
// in-between state. Every caller in this codebase only ever has one thread
// calling observe() (that shard's decoder thread) and calls resync() only
// after that thread has stopped (see io/pipeline_runner.hpp), so this
// doesn't arise in practice; a concurrent-writers use case would need a
// stronger primitive than this class provides.

#include <atomic>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <optional>

#include "protocol/message_types.hpp"

namespace mdfh::concurrency {

enum class BookState : std::uint8_t {
    Live,
    Stale,
};

enum class SequenceObservation {
    InOrder,    // seq_num continues directly from the last one seen
    Gap,        // seq_num skipped ahead -- one or more messages were missed
    Duplicate,  // seq_num at or behind the last one seen (replay/out-of-order)
};

class GapDetector {
public:
    // Feed the next message's seq_num through the detector. Updates
    // sequence_gaps_total() and state() as a side effect when a gap is found.
    SequenceObservation observe(protocol::SeqNum seq_num) {
        if (!has_last_seq_num_.load(std::memory_order_relaxed)) {
            last_seq_num_.store(seq_num, std::memory_order_relaxed);
            has_last_seq_num_.store(true, std::memory_order_release);
            return SequenceObservation::InOrder;
        }

        const protocol::SeqNum last     = last_seq_num_.load(std::memory_order_relaxed);
        const protocol::SeqNum expected = last + 1;

        if (seq_num == expected) {
            last_seq_num_.store(seq_num, std::memory_order_relaxed);
            return SequenceObservation::InOrder;
        }

        if (seq_num < expected) {
            return SequenceObservation::Duplicate;
        }

        // seq_num > expected: at least one message between expected and
        // seq_num never arrived.
        sequence_gaps_total_.fetch_add(1, std::memory_order_relaxed);
        state_.store(BookState::Stale, std::memory_order_release);
        log_gap(expected, seq_num);
        last_seq_num_.store(seq_num, std::memory_order_relaxed);
        return SequenceObservation::Gap;
    }

    // Simulates a fulfilled replay request or full snapshot resync: the feed
    // is now known-good as of resync_seq_num, so tracking resumes from there
    // and STALE clears back to LIVE. Does not reset sequence_gaps_total(),
    // which is a lifetime counter, not a "currently stale" flag.
    void resync(protocol::SeqNum resync_seq_num) {
        last_seq_num_.store(resync_seq_num, std::memory_order_relaxed);
        has_last_seq_num_.store(true, std::memory_order_relaxed);
        state_.store(BookState::Live, std::memory_order_release);
    }

    BookState state() const noexcept { return state_.load(std::memory_order_acquire); }
    bool is_stale() const noexcept { return state() == BookState::Stale; }
    std::uint64_t sequence_gaps_total() const noexcept {
        return sequence_gaps_total_.load(std::memory_order_relaxed);
    }
    std::optional<protocol::SeqNum> last_seq_num() const noexcept {
        if (!has_last_seq_num_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }
        return last_seq_num_.load(std::memory_order_relaxed);
    }

private:
    // Serializes the diagnostic log line across every GapDetector instance
    // (one per shard, each with its own decoder thread as of Phase 7).
    // Without this, concurrent std::cerr writes from different shards'
    // threads don't corrupt any state -- state_/sequence_gaps_total_/
    // last_seq_num_ are already correctly atomic -- but interleave at the
    // character level into unreadable garbled output, which is exactly
    // what running the Phase 7 pipeline with real gaps on multiple shards
    // showed. A log message being merely cosmetic doesn't mean a garbled
    // one is a fine outcome: it hid the real bug (see object_pool.hpp)
    // behind confusing noise while debugging this phase.
    static void log_gap(protocol::SeqNum expected, protocol::SeqNum seq_num) {
        static std::mutex log_mutex;
        std::lock_guard<std::mutex> lock(log_mutex);
        std::cerr << "[gap_detector] sequence gap: expected " << expected << ", got " << seq_num << " ("
                   << (seq_num - expected) << " message(s) missing) -- marking STALE\n";
    }

    std::atomic<bool> has_last_seq_num_{false};
    std::atomic<protocol::SeqNum> last_seq_num_{0};
    std::atomic<BookState> state_{BookState::Live};
    std::atomic<std::uint64_t> sequence_gaps_total_{0};
};

}  // namespace mdfh::concurrency
