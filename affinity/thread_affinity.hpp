#pragma once

// Phase 8: CPU affinity / thread pinning.
//
// This project is primarily developed on macOS, so the original
// implementation used Mach's thread_policy_set() with
// THREAD_AFFINITY_POLICY. To get genuine hard-pinning evidence -- not just
// the macOS advisory hint -- there's also a real Linux implementation
// backed by pthread_setaffinity_np(), built and run inside a Docker
// container (see docker/README.md and bench/pinning_bench.cpp for the
// actual before/after numbers on both platforms). Same API
// (set_thread_affinity_tag), two platform-specific .cpp files
// (thread_affinity_macos.cpp / thread_affinity_linux.cpp) selected by
// affinity/CMakeLists.txt via CMAKE_SYSTEM_NAME -- because the two OS APIs
// make fundamentally different guarantees, not just different syntax for
// the same thing:
//
//   - Linux's pthread_setaffinity_np() takes a cpu_set_t naming specific,
//     numbered logical cores and is a HARD constraint enforced by the
//     scheduler: the thread will only ever run on those cores, full stop.
//     This is what real production HFT systems rely on (paired with
//     isolcpus/nohz_full kernel boot parameters to keep the OS scheduler
//     off those cores entirely). thread_affinity_linux.cpp reinterprets
//     AffinityTag as a target-core selector (tag modulo however many CPUs
//     this process may actually use, per sched_getaffinity() -- important
//     inside a container, where the cgroup-visible cpuset may be a
//     non-contiguous subset of the host's cores) and hard-pins to exactly
//     that one core.
//   - macOS's THREAD_AFFINITY_POLICY takes an opaque integer TAG, not a
//     core number -- there is no API on macOS to request a specific
//     numbered core at all. Threads sharing the same nonzero tag are only
//     a SCHEDULING HINT: the kernel is told "these threads would like to
//     share an L2 cache" and remains completely free to ignore it. Apple's
//     own documentation describes this as advisory. On Apple Silicon's
//     heterogeneous P-core/E-core topology in particular, the scheduler's
//     power/thermal/QoS decisions routinely take priority over the
//     affinity hint.
//
// Net effect: a "before/after" benchmark run on macOS measures the effect
// of *asking* the scheduler for cache locality, not the effect of a
// guaranteed pin -- while the SAME benchmark run on Linux (in the
// container) measures the effect of an actual, kernel-enforced hard pin.
// See bench/pinning_bench.cpp for the measurements on both and an
// explicit discussion of what each does and doesn't prove.
//
// VERIFIED ON THIS PROJECT'S macOS DEV MACHINE (Apple Silicon, M5 Pro):
// thread_policy_set(THREAD_AFFINITY_POLICY) does not merely get silently
// ignored -- it returns KERN_NOT_SUPPORTED (errno 46), confirmed with a
// standalone probe outside Catch2/TSan to rule out either as the cause.
// That is a stronger statement than Apple's documented Intel-era
// "advisory hint" behavior: on this machine there appears to be no
// working userspace affinity mechanism via this API at all, not just a
// weak one. set_thread_affinity_tag() below still returns false in that
// case (it never throws or aborts), so callers that don't check the
// return value -- like io/pipeline_runner.cpp, which treats this purely
// as a best-effort hint -- keep working correctly either way. See
// affinity/test_thread_affinity.cpp for how this was discovered and why
// the tests don't assert success, and bench/pinning_bench.cpp for what it
// means for the actual before/after timing numbers on macOS specifically.

#include <cstdint>
#include <thread>

namespace mdfh::affinity {

// An affinity tag. On macOS: threads sharing the same nonzero tag are
// hinted to the scheduler as wanting to be co-scheduled to share an L2
// cache (THREAD_AFFINITY_TAG_NULL == 0 is the "no hint" default every
// thread starts with). On Linux: reinterpreted as a target-core selector
// (see thread_affinity_linux.cpp) -- distinct tags land on distinct cores
// whenever enough are available. Either way, tags are opaque integers
// scoped to this process; this project's specific tag assignments (which
// thread gets which tag, and why) are documented in io/pipeline_runner.cpp,
// where the pipeline's actual thread topology is known.
using AffinityTag = std::uint32_t;

// Applies an affinity tag/target-core hint to `thread`. Safe to call from
// any thread once `thread` has been started -- neither platform's
// implementation needs to run ON the target thread itself. Returns true
// if the underlying OS call reported success: on macOS that means only
// "the hint was accepted by the kernel", not "the thread is now pinned to
// anything in particular" (see the caveats above); on Linux it means the
// thread is now genuinely, unconditionally hard-pinned to one specific
// core.
bool set_thread_affinity_tag(std::thread& thread, AffinityTag tag);

}  // namespace mdfh::affinity
