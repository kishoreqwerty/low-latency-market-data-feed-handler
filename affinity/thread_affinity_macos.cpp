#include "affinity/thread_affinity.hpp"

#include <mach/mach.h>
#include <mach/thread_policy.h>
#include <pthread.h>

namespace mdfh::affinity {

bool set_thread_affinity_tag(std::thread& thread, AffinityTag tag) {
    thread_affinity_policy_data_t policy{.affinity_tag = static_cast<integer_t>(tag)};

    // pthread_mach_thread_np() resolves a pthread_t to its Mach port; it's
    // a lookup, not a "run this on that thread" operation, so it's safe to
    // call from outside the target thread (e.g. from the thread that
    // spawned it, right after construction).
    thread_port_t port = pthread_mach_thread_np(thread.native_handle());

    kern_return_t result = thread_policy_set(port, THREAD_AFFINITY_POLICY,
                                              reinterpret_cast<thread_policy_t>(&policy),
                                              THREAD_AFFINITY_POLICY_COUNT);
    return result == KERN_SUCCESS;
}

}  // namespace mdfh::affinity
