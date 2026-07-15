#include "affinity/thread_affinity.hpp"

#include <pthread.h>
#include <sched.h>

#include <vector>

namespace mdfh::affinity {

bool set_thread_affinity_tag(std::thread& thread, AffinityTag tag) {
    // Query the CPUs this PROCESS is actually allowed to run on, rather
    // than assuming a contiguous [0, hardware_concurrency()) range. Under
    // a container's cgroup cpuset (Docker's --cpuset-cpus, Kubernetes
    // resource limits, etc.), the allowed core IDs can be a non-contiguous
    // subset -- e.g. only cores 4-7 -- so building the candidate list from
    // sched_getaffinity() is what makes this correct there, not just on
    // bare metal.
    cpu_set_t available;
    CPU_ZERO(&available);
    if (sched_getaffinity(0, sizeof(available), &available) != 0) {
        return false;
    }

    std::vector<int> usable_cpus;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &available)) {
            usable_cpus.push_back(cpu);
        }
    }
    if (usable_cpus.empty()) {
        return false;
    }

    // Unlike macOS's THREAD_AFFINITY_POLICY (an opaque grouping hint --
    // see thread_affinity.hpp), Linux's cpu_set_t names one specific core.
    // AffinityTag is reinterpreted here as a target-core selector: tag
    // modulo however many cores this process may actually use, so
    // distinct tags land on distinct cores whenever enough are available
    // (wrapping only if there are more distinct tags than usable cores).
    const int target_cpu = usable_cpus[tag % usable_cpus.size()];

    cpu_set_t target_set;
    CPU_ZERO(&target_set);
    CPU_SET(target_cpu, &target_set);

    return pthread_setaffinity_np(thread.native_handle(), sizeof(target_set), &target_set) == 0;
}

}  // namespace mdfh::affinity
