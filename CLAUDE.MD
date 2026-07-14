# Project: Low-Latency Market Data Feed Handler

See architecture_spec.md, HLD.md, and build_guide.md in this repo for full design.

## Build order
Follow build_guide.md phase by phase. Do not skip ahead — each phase has a
"Done when" criterion that must be met before starting the next.

## Non-negotiable design decisions (do not change without discussion)
- Shard by symbol at ingestion; every queue is genuinely SPSC, not MPSC
- Zero dynamic allocation in the hot path (object pools only)
- Drop-oldest backpressure policy on all bounded queues
- CPU-pinned threads per pipeline stage
- Sequence gap detection with explicit STALE marking

## Stack
C++20/23, CMake, Catch2 + GMock, perf/vtune for profiling