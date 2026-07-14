#include "orderbook/test_alloc_counter.hpp"

#include <atomic>
#include <cstdlib>
#include <new>

namespace {

std::atomic<std::size_t> g_new_calls{0};
std::atomic<std::size_t> g_delete_calls{0};

}  // namespace

// Replaceable per the standard ([new.delete]). Delegating to malloc/free
// (rather than reimplementing allocation) keeps this compatible with
// AddressSanitizer, which instruments at the malloc/free layer.
void* operator new(std::size_t size) {
    g_new_calls.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(size == 0 ? 1 : size)) {
        return p;
    }
    throw std::bad_alloc();
}

void operator delete(void* ptr) noexcept {
    g_delete_calls.fetch_add(1, std::memory_order_relaxed);
    std::free(ptr);
}

void operator delete(void* ptr, std::size_t) noexcept {
    g_delete_calls.fetch_add(1, std::memory_order_relaxed);
    std::free(ptr);
}

namespace mdfh::test_util {

void reset_alloc_counters() noexcept {
    g_new_calls.store(0, std::memory_order_relaxed);
    g_delete_calls.store(0, std::memory_order_relaxed);
}

std::size_t new_call_count() noexcept {
    return g_new_calls.load(std::memory_order_relaxed);
}

std::size_t delete_call_count() noexcept {
    return g_delete_calls.load(std::memory_order_relaxed);
}

}  // namespace mdfh::test_util
