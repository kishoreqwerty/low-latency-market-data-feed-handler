# Linux container: real `pthread_setaffinity_np` hard-pin evidence

`affinity/thread_affinity.hpp` documents that macOS's `THREAD_AFFINITY_POLICY`
is an advisory scheduler hint (and, on this project's Apple Silicon dev
machine, verified to be outright rejected with `KERN_NOT_SUPPORTED`). To get
genuine hard-pinning evidence — not just the macOS null result — this
container builds and runs the identical project against a real Linux
`pthread_setaffinity_np` implementation (`affinity/thread_affinity_linux.cpp`),
with no source changes anywhere else.

## Build and run

```sh
docker build -f docker/Dockerfile -t mdfh-linux-bench .
docker run --rm mdfh-linux-bench                       # runs bench/mdfh_pinning_bench
docker run --rm --entrypoint ctest mdfh-linux-bench --test-dir build/benchmark   # full suite
```

## Results (Apple Silicon M5 Pro host, Docker Desktop, native arm64 — no emulation)

Same benchmark, same workload (8 shards, 1,000,000 messages/run, 5 runs per
variant) as the macOS run documented in the Phase 8 report:

| | macOS (native) | Linux (this container) |
|---|---|---|
| Affinity calls accepted | 0 / 45 (`KERN_NOT_SUPPORTED`) | **45 / 45** (`pthread_setaffinity_np` succeeded) |
| Pinned mean | 5221.09 ms | 74745.26 ms |
| Unpinned mean | 5226.43 ms | 74765.98 ms |
| Delta | 0.10% (noise) | 0.03–0.06% (noise) |

Two separate findings here, not one:

1. **The mechanism genuinely works on Linux.** All 45 `pthread_setaffinity_np`
   calls (network I/O thread + 8 shard threads, across 5 pinned runs)
   returned success — a real, kernel-enforced hard pin, unlike macOS's
   outright rejection. This is the contrast the earlier macOS-only report
   couldn't demonstrate.
2. **The timing delta is still noise-level, for a different reason than on
   macOS.** Two confounds specific to this containerized environment, ruled
   in/out explicitly rather than assumed:
   - **Not I/O buffering**: rerunning with the gap-detector's stderr logging
     redirected to `/dev/null` *inside* the container (ruling out any
     Docker-log-driver-crosses-the-VM-boundary effect) gave the same ~74.7s
     runtime.
   - **Is timer granularity**: a standalone probe (1,000 calls to
     `sleep_for(50us)`) measured ~69.5us/call average on the macOS host vs.
     ~132.8us/call in this container — Docker Desktop's Linux VM (Apple
     Virtualization.framework, not bare metal) has roughly 2x coarser sleep
     granularity than native macOS. `io/pipeline_runner.cpp`'s shard threads
     poll via `sleep_for(50us)` when a queue is momentarily empty; coarser
     per-call granularity compounds through that polling loop and plausibly
     accounts for most of the ~14x total wall-clock difference between the
     two environments. This is a virtualization artifact, not a pinning
     effect — it shows up identically in both the pinned and unpinned runs.

**Honest limit of this evidence**: Docker Desktop's Linux is a VM (Apple's
Virtualization.framework), not bare-metal Linux. `pthread_setaffinity_np`
genuinely succeeds and is enforced by the *guest* kernel, but a guest hard
pin is a pin to a vCPU — the host hypervisor remains free to schedule that
vCPU across physical cores. True bare-metal-grade pinning evidence (the kind
real production HFT systems rely on, typically paired with `isolcpus`/
`nohz_full` kernel boot parameters) would require running this same image on
physical Linux hardware, not inside a virtualized container on macOS. What
this container does verify, concretely: the Linux affinity API itself works
as documented and is a fundamentally different (stronger) guarantee than
macOS's, even though neither environment's timing run shows a measurable
speedup for this particular I/O/poll-heavy workload.
