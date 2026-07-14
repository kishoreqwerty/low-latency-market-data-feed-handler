#pragma once

// Per-shard sequence gap detector (architecture_spec.md Section 3). Phase 6
// will give each shard its own instance; for now there is exactly one
// shard, so a single instance is the whole story. Not wired to OrderBook
// yet -- that wiring (gap detector + ring buffer + book, one set per shard)
// is explicitly Phase 6's job, not this one's.

#include <cstdint>
#include <iostream>
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
        if (!last_seq_num_.has_value()) {
            last_seq_num_ = seq_num;
            return SequenceObservation::InOrder;
        }

        const protocol::SeqNum expected = *last_seq_num_ + 1;

        if (seq_num == expected) {
            last_seq_num_ = seq_num;
            return SequenceObservation::InOrder;
        }

        if (seq_num < expected) {
            return SequenceObservation::Duplicate;
        }

        // seq_num > expected: at least one message between expected and
        // seq_num never arrived.
        ++sequence_gaps_total_;
        state_ = BookState::Stale;
        std::cerr << "[gap_detector] sequence gap: expected " << expected << ", got " << seq_num << " ("
                   << (seq_num - expected) << " message(s) missing) -- marking STALE\n";
        last_seq_num_ = seq_num;
        return SequenceObservation::Gap;
    }

    // Simulates a fulfilled replay request or full snapshot resync: the feed
    // is now known-good as of resync_seq_num, so tracking resumes from there
    // and STALE clears back to LIVE. Does not reset sequence_gaps_total(),
    // which is a lifetime counter, not a "currently stale" flag.
    void resync(protocol::SeqNum resync_seq_num) {
        last_seq_num_ = resync_seq_num;
        state_        = BookState::Live;
    }

    BookState state() const noexcept { return state_; }
    bool is_stale() const noexcept { return state_ == BookState::Stale; }
    std::uint64_t sequence_gaps_total() const noexcept { return sequence_gaps_total_; }
    std::optional<protocol::SeqNum> last_seq_num() const noexcept { return last_seq_num_; }

private:
    std::optional<protocol::SeqNum> last_seq_num_;
    BookState state_                   = BookState::Live;
    std::uint64_t sequence_gaps_total_ = 0;
};

}  // namespace mdfh::concurrency
