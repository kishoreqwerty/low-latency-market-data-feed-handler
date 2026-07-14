#include <catch2/catch_test_macros.hpp>

#include "concurrency/gap_detector.hpp"

using namespace mdfh::concurrency;

TEST_CASE("GapDetector treats the first observed seq_num as in-order", "[gap_detector]") {
    GapDetector detector;

    REQUIRE(detector.observe(1) == SequenceObservation::InOrder);
    REQUIRE(detector.state() == BookState::Live);
    REQUIRE_FALSE(detector.is_stale());
    REQUIRE(detector.sequence_gaps_total() == 0);
    REQUIRE(detector.last_seq_num() == 1);
}

TEST_CASE("GapDetector stays LIVE through a contiguous sequence", "[gap_detector]") {
    GapDetector detector;

    for (mdfh::protocol::SeqNum seq = 1; seq <= 5; ++seq) {
        REQUIRE(detector.observe(seq) == SequenceObservation::InOrder);
    }

    REQUIRE(detector.state() == BookState::Live);
    REQUIRE(detector.sequence_gaps_total() == 0);
    REQUIRE(detector.last_seq_num() == 5);
}

TEST_CASE("GapDetector flags a gap, marks STALE, and a resync clears it back to LIVE", "[gap_detector]") {
    GapDetector detector;

    // Feed seq 1, 2, 3 -- all in order.
    REQUIRE(detector.observe(1) == SequenceObservation::InOrder);
    REQUIRE(detector.observe(2) == SequenceObservation::InOrder);
    REQUIRE(detector.observe(3) == SequenceObservation::InOrder);
    REQUIRE(detector.state() == BookState::Live);

    // Intentional gap: seq 4 never arrives, seq 5 shows up next.
    REQUIRE(detector.observe(5) == SequenceObservation::Gap);
    REQUIRE(detector.state() == BookState::Stale);
    REQUIRE(detector.is_stale());
    REQUIRE(detector.sequence_gaps_total() == 1);
    REQUIRE(detector.last_seq_num() == 5);  // the detector still advances to the newest seq_num seen

    // Further in-order messages after the gap do NOT clear STALE on their own --
    // only an explicit resync does.
    REQUIRE(detector.observe(6) == SequenceObservation::InOrder);
    REQUIRE(detector.state() == BookState::Stale);
    REQUIRE(detector.sequence_gaps_total() == 1);

    // Resync (simulated snapshot/replay fulfillment) establishes a new
    // baseline and clears STALE, but the lifetime gap counter is untouched.
    detector.resync(100);
    REQUIRE(detector.state() == BookState::Live);
    REQUIRE_FALSE(detector.is_stale());
    REQUIRE(detector.sequence_gaps_total() == 1);
    REQUIRE(detector.last_seq_num() == 100);

    // Tracking resumes from the resync point.
    REQUIRE(detector.observe(101) == SequenceObservation::InOrder);
    REQUIRE(detector.state() == BookState::Live);
}

TEST_CASE("GapDetector counts multiple distinct gaps", "[gap_detector]") {
    GapDetector detector;

    REQUIRE(detector.observe(1) == SequenceObservation::InOrder);
    REQUIRE(detector.observe(3) == SequenceObservation::Gap);   // missing 2
    REQUIRE(detector.sequence_gaps_total() == 1);

    detector.resync(10);
    REQUIRE(detector.observe(12) == SequenceObservation::Gap);  // missing 11
    REQUIRE(detector.sequence_gaps_total() == 2);
    REQUIRE(detector.state() == BookState::Stale);
}

TEST_CASE("GapDetector registers a large gap the same way as a single missing message", "[gap_detector]") {
    GapDetector detector;

    REQUIRE(detector.observe(1) == SequenceObservation::InOrder);
    REQUIRE(detector.observe(1000) == SequenceObservation::Gap);  // 998 messages missing

    REQUIRE(detector.state() == BookState::Stale);
    REQUIRE(detector.sequence_gaps_total() == 1);
    REQUIRE(detector.last_seq_num() == 1000);
}

TEST_CASE("GapDetector reports duplicate/out-of-order-behind seq_nums without flagging a gap", "[gap_detector]") {
    GapDetector detector;

    REQUIRE(detector.observe(1) == SequenceObservation::InOrder);
    REQUIRE(detector.observe(2) == SequenceObservation::InOrder);
    REQUIRE(detector.observe(3) == SequenceObservation::InOrder);

    // A message we've already passed (replay/duplicate) is neither in-order
    // nor a gap -- and must not perturb state.
    REQUIRE(detector.observe(2) == SequenceObservation::Duplicate);
    REQUIRE(detector.state() == BookState::Live);
    REQUIRE(detector.sequence_gaps_total() == 0);
    REQUIRE(detector.last_seq_num() == 3);  // unchanged by the duplicate
}
