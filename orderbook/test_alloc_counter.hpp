#pragma once

// Replaces the process-wide operator new/delete (in test_alloc_counter.cpp)
// with counting wrappers, so tests/benchmarks in a binary that links this
// can measure real heap traffic instead of asserting it by inspection.

#include <cstddef>

namespace mdfh::test_util {

// Call right before the code path you want to audit.
void reset_alloc_counters() noexcept;

std::size_t new_call_count() noexcept;
std::size_t delete_call_count() noexcept;

}  // namespace mdfh::test_util
