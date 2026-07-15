# Benchmark results

Every number below was measured in this repo's own benchmark/test binaries, not
estimated. Reproduce any of them with the commands cited inline. Platform:
macOS numbers are native, on this project's Apple Silicon (M5 Pro) dev
machine; Linux numbers are from the Docker container built for Phase 8
(`docker/Dockerfile`) — see `docker/README.md` for why a container is needed
at all (genuine `pthread_setaffinity_np`, `perf`, Valgrind — none of the three
has real Apple Silicon macOS support).

## Phase 3 — allocation benchmark

`bench/mdfh_allocation_bench`: 2,000,000 Add+remove cycles (4,000,000
messages), 20,000 concurrently resting orders, 50 price levels/side. Compares
the production pool-backed `OrderBook` against `NaiveOrderBook` (identical
logic, plain `std::allocator`) on the same workload.

| Platform | Variant | Time | ns/message | `new()`/`delete()` calls |
|---|---|---|---|---|
| macOS (native) | Pool-backed | 66.59 ms | 16.6 | 0 / 0 |
| macOS (native) | Naive | 119.15 ms | 29.8 | 4,000,000 / 4,000,000 |
| Linux (Docker) | Pool-backed | 70.42 ms | 17.6 | 0 / 0 |
| Linux (Docker) | Naive | 83.58 ms | 20.9 | 4,000,000 / 4,000,000 |

**Zero heap traffic for the pool-backed book, confirmed on both platforms** —
the `new()`/`delete()` call counts come from a real global-operator-override
counter (`orderbook/test_alloc_counter.cpp`), not an assumption. The relative
speedup differs by platform (1.79x on macOS vs. 1.19x on Linux/glibc) — glibc's
allocator is evidently faster than macOS's for this alloc/free-heavy pattern,
narrowing (not eliminating) the naive path's disadvantage. The zero-allocation
property itself is platform-independent.

**Note on historical numbers**: the Phase 3 commit message (`27437cd`) cites
"23% faster than std::allocator," measured against a *different* pool
implementation than the one above — at that point the pool was a single
process-wide static singleton with no synchronization (the system was still
single-threaded; Phase 7 hadn't introduced concurrent shard threads yet). A
mutex was later added to that same global pool during Phase 7 development to
make it safe for real concurrency, then removed entirely in favor of the
current per-shard, `thread_local`-indexed design once sharding made a
per-shard pool possible (see `orderbook/object_pool.hpp`'s header comment for
the full history) — a shared pool behind a mutex reintroduced the exact
cross-shard contention sharding exists to eliminate, one layer below the ring
buffers. `bench/allocation_bench.cpp`'s workload itself hasn't changed since
Phase 3 (confirmed via `git log`); every number in the table above reflects
the current, mutex-free, per-shard pool — not the original singleton the
Phase 3 commit message describes. If you see the two figures cited
differently elsewhere (e.g. in git history), that's why.

## Phase 8 — CPU affinity / pinning benchmark

`bench/mdfh_pinning_bench`: full live pipeline, 8 shards, 1,000,000 messages,
5 runs per variant (pinned/unpinned), interleaved to avoid drift bias.

| Platform | Affinity API | Hints accepted | Pinned mean | Unpinned mean | Delta |
|---|---|---|---|---|---|
| macOS (native) | `thread_policy_set`/`THREAD_AFFINITY_POLICY` | **0 / 45** (`KERN_NOT_SUPPORTED`) | 5221.09 ms | 5226.43 ms | 0.10% (noise) |
| Linux (Docker) | `pthread_setaffinity_np` | **45 / 45** (genuine hard pin) | 74745.26 ms | 74765.98 ms | 0.03% (noise) |

Two separate findings, not one:
1. **The Linux hard-pin mechanism genuinely works** — a real, kernel-enforced
   pin, unlike macOS's outright rejection (confirmed via a standalone probe:
   `thread_policy_set` returns `KERN_NOT_SUPPORTED` on this Apple Silicon
   machine, not a silently-ignored soft hint).
2. **Neither platform shows a measurable speedup** for this poll-heavy
   workload, for different reasons: macOS because the OS rejected pinning
   outright; Linux because — verified, not assumed — a standalone probe found
   `sleep_for(50µs)` takes ~133µs actual in the Docker container vs. ~70µs
   natively on macOS (Docker Desktop's Linux runs in a VM, not bare metal),
   which explains most of the ~14x absolute-runtime gap between the two rows
   above and swamps any affinity-driven signal either way.

See `docker/README.md` for the full writeup, including the sleep-granularity
probe.

## Phase 10 — tick-to-book-update latency percentiles

`bench/mdfh_latency_bench`: full live pipeline, 8 shards, 1,000,000 messages,
0.05% packet loss, ~1.15M recorded deltas. Every value in nanoseconds.
Stage boundaries: network I/O received → decode → demux (route+enqueue) →
[wait in input ring buffer] → book update (`apply()`) → [wait in output ring
buffer] → publish. See `concurrency/latency_trace.hpp` and
`publisher/publisher.hpp` for exactly which timestamps bound which interval.

### macOS (native)

| Stage | mean | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| receive → decode | 16 | 0 | 42 | 125 | 25,083 |
| decode → demux | 350 | 291 | 1,166 | 4,667 | 3,982,541 |
| input queue wait | 32,635 | 31,708 | 67,375 | 85,916 | 3,816,291 |
| book update | 419 | 333 | 1,834 | 9,208 | 3,770,708 |
| output queue wait | 32,598 | 32,125 | 68,416 | 80,334 | 961,417 |
| publish (wrapper only) | 14 | 0 | 42 | 83 | 28,208 |
| **tick → book update** | **33,420** | **32,417** | **68,250** | **92,500** | 4,009,042 |
| end to end | 66,032 | 65,042 | 124,125 | 162,250 | 4,078,542 |

### Linux (Docker)

| Stage | mean | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| receive → decode | 35 | 42 | 84 | 250 | 64,375 |
| decode → demux | 426 | 375 | 1,209 | 7,167 | 4,107,375 |
| input queue wait | 68,830 | 66,209 | 136,000 | 171,333 | 14,713,292 |
| book update | 1,303 | 792 | 3,709 | 95,584 | 14,770,250 |
| output queue wait | 71,027 | 117,000 | 144,916 | 178,084 | 6,339,709 |
| publish (wrapper only) | 24 | 41 | 42 | 167 | 74,958 |
| **tick → book update** | **70,595** | **67,625** | **137,916** | **325,750** | 14,880,708 |
| end to end | 141,646 | 140,750 | 272,000 | 408,709 | 15,009,083 |

**Reading these numbers**: `book update` (the actual `OrderBookT::apply()`
call — the thing Phase 3's zero-allocation design was for) is sub-microsecond
at p50 on both platforms (333ns / 792ns). `input_queue_wait` and
`output_queue_wait` — both roughly 32µs mean on macOS, roughly matching the
pipeline's own `sleep_for(50µs)` idle-poll interval in
`io/pipeline_runner.cpp` almost exactly — are the dominant cost, not the
algorithm. The p99.9/max tails (hundreds of microseconds to several
milliseconds) are real OS scheduling jitter, not hidden.

## Profiling: perf record + flame graph (Linux/Docker) and native macOS `sample`

Full writeup and the interactive flame graph: **`docs/perf/latency_report.html`**
(also published as a Claude Artifact) and `docs/perf/README.md`.

Headline finding, corroborated three independent ways — Linux CPU sampling
(`perf record -F 999 -g`), the latency percentiles above, and a native macOS
`sample` capture (`docs/perf/macos_sample_native.txt`) — all point to the same
two things:

1. **33.46%** of all Linux perf samples are in `clock_nanosleep`/kernel-side
   sleep: the decoder/publisher polling loop's `sleep_for(50µs)` dominates CPU
   time whenever the pipeline is running under-saturated, exactly matching the
   ~32-70µs queue-wait means above.
2. **~10%** of samples are in `clock_gettime`/`steady_clock::now()` — the
   latency instrumentation this very benchmark relies on. A real, measured
   cost of measuring, not a free lunch (documented as a deliberate tradeoff in
   `concurrency/latency_trace.hpp`).
3. The actual order-book logic (`apply`/`insert_resting_order`/
   `remove_resting_order` across all four message types) is a modest ~13-15%
   of total CPU time combined — confirming the bottleneck is the surrounding
   polling architecture and measurement overhead, not the algorithm.

**perf needed a workaround to run at all**: Ubuntu's `linux-tools-generic`
package's `perf` refuses to start ("perf not found for kernel 6.12.76") since
Docker Desktop containers share the host's LinuxKit kernel rather than
running their own. The actual binary underneath
(`/usr/lib/linux-tools/6.8.0-134-generic/perf`) works correctly with real
symbol resolution when invoked directly, bypassing the version-check wrapper
— see `docs/perf/README.md`.

**macOS-native equivalent**: Instruments' Time Profiler is the closest match
but is GUI/`.trace`-driven with no clean non-interactive path; the CLI-adjacent
`sample <pid>` utility (`/usr/bin/sample`) was actually run against this
project's own `mdfh_latency_bench` natively — not just proposed — and its
output (`docs/perf/macos_sample_native.txt`) independently shows the same
`clock_gettime`/`nanosleep` dominance, on real (non-virtualized) Apple Silicon
hardware, which the Docker/Linux numbers can't rule out as a VM artifact on
their own.

## Sanitizers: ASan/UBSan, TSan, Valgrind

| Tool | Platform | Result |
|---|---|---|
| ASan + UBSan (`debug` preset) | macOS | 71/71 tests, 100% pass, 3 consecutive clean runs |
| TSan (`debug-tsan` preset) | macOS | 71/71 tests, 100% pass, 5+ consecutive clean runs (plus dozens of additional targeted reruns across this project's history — pool concurrency, pipeline shutdown handshake under deliberately adversarial timing, the Phase 12 blocking-wait stress tests, full e2e suite — all clean) |
| Valgrind (`--leak-check=full --track-origins=yes`) | Linux (Docker) | 69/71 tests pass (re-run after Phase 12); **0 memory defects detected anywhere** |

Valgrind can't coexist with ASan/UBSan instrumentation, so it runs against a
separate sanitizer-free debug build (`-DMDFH_SANITIZER=none`), Linux-only (no
meaningful Apple Silicon Valgrind support). Full detail, including why the 2
non-passing tests are unrelated to memory correctness, is in
`docs/perf/valgrind/README.md`, alongside full logs showing explicit
`ERROR SUMMARY: 0 errors from 0 contexts` for every binary checked. Which 2
tests fail is itself timing-sensitive, not fixed: before Phase 12 it was the
gap-detection e2e test (a 10-second real-time deadline for observing a live
event) and the naive-allocator counter test; after Phase 12's blocking-wait
change, the gap-detection test now passes under Valgrind (the new scheduling
characteristics happen to fit its window better) but the adversarial
publisher-shutdown-handshake test's 2ms real-time window now fails instead.
Same underlying category both times — a hardcoded short real-time window
meeting Valgrind's ~20-50x slowdown — not a new correctness bug, and zero
memory defects were reported in either version.

## Phase 12 — blocking wait replaces the polling floor: before/after

Phase 10's headline finding was that `io/pipeline_runner.cpp`'s decoder and
publisher threads idle-waited via a fixed `sleep_for(50us)` poll whenever a
queue was empty, and that poll — not the order book — set the pipeline's
actual latency floor. Phase 12 replaced it with a real OS-level blocking
wait: `concurrency::SpscRingBuffer` gained `wait_for_data()` (blocks via
`std::atomic::wait`, C++20) and `notify_waiters()` (producer-side
`std::atomic::notify_one`, called from every `push()` and explicitly from
the shutdown path), with the same `bench/mdfh_latency_bench` workload
(8 shards, 1,000,000 messages, 0.05% packet loss) re-run unchanged.

**A real bug found before this ever reached the pipeline**: the first
implementation waited/notified on the ring buffer's existing `tail_` atomic
directly. That's correct for the "new data arrived" case, but
`std::atomic::wait(old)` only returns when the *watched value itself*
changes — a `notify_waiters()` call for the shutdown case (nothing pushed,
`tail_` unchanged) gets treated as a spurious wake: the waiter rechecks,
sees no change, and goes back to sleep forever. A dedicated 2-thread test
built specifically to exercise that exact path (an artificially slowed
consumer relying on the producer-done `notify_waiters()` wake, not a
subsequent push) hung indefinitely and caught this before any pipeline
code depended on it. Fixed with a dedicated `wake_generation_` counter that
*every* wake-worthy event — real data or an external notify — unconditionally
increments, so the recheck can never see "no change." See
`concurrency/spsc_ring_buffer.hpp`'s header comment for the full mechanism.

### Before (Phase 10, polling) vs. after (Phase 12, blocking), `tick_to_book_update`

| Platform | Metric | Before (poll) | After (blocking) | Improvement |
|---|---|---|---|---|
| macOS | mean | 33,420 ns | 5,306 ns | **6.3x** |
| macOS | p50 | 32,417 ns | 4,000 ns | **8.1x** |
| macOS | p99 | 68,250 ns | 8,542 ns | **8.0x** |
| macOS | p99.9 | 92,500 ns | 32,834 ns | **2.8x** |
| Linux (Docker) | mean | 70,595 ns | 31,031 ns | **2.3x** |
| Linux (Docker) | p50 | 67,625 ns | 29,208 ns | **2.3x** |
| Linux (Docker) | p99 | 137,916 ns | 62,292 ns | **2.2x** |
| Linux (Docker) | p99.9 | 325,750 ns | 80,500 ns | **4.0x** |

Both platforms improved substantially and consistently (3+ repeated runs
each, numbers stable within a few percent) — but **not by the same amount,
and the reason why is itself a real, measured finding, not a footnote**:

- **macOS**: a fresh native `sample` capture after this change shows *zero*
  occurrences of `nanosleep` anywhere in the profile — the poll is
  completely gone, replaced by real business logic (`OrderBookT::apply`,
  `insert_resting_order`, `ShardedPipeline::process_shard`) as the dominant
  symbols. This is consistent with a genuinely efficient, near-zero-CPU
  block while idle.
- **Linux (Docker)**: `perf report` also shows `clock_nanosleep` gone, but
  it's replaced by substantial self-time in `__sched_yield` (7.35%) and
  `std::__atomic_wait_address_v` (0.41% self, 82.43% total) — i.e.,
  glibc/libstdc++'s `atomic::wait` implementation on this platform spin-
  yields for a period before committing to a real futex block, rather than
  blocking immediately. That's a real implementation-level difference
  between platforms' C++ standard libraries, not a bug in this project's
  code, and it plausibly explains why the Linux improvement (2.2-4x) is
  smaller than macOS's (6-8x): part of the old poll's cost was simply
  replaced by a different (shorter, but nonzero) form of active waiting,
  rather than eliminated entirely. Full artifacts: `docs/perf/phase12/`.

**What didn't change**: `book_update` (the actual `OrderBookT::apply()` cost)
stayed sub-microsecond at p50 on both platforms throughout (it was never the
bottleneck, before or after) — this change targeted exactly the thing Phase
10 identified as dominant, and moved exactly that number, nothing else.

## Reproducing everything above

```sh
# Phase 3 + Phase 8 + Phase 10 benchmarks (native, whichever platform you're on)
cmake --preset benchmark && cmake --build build/benchmark -j
./build/benchmark/bench/mdfh_allocation_bench
./build/benchmark/bench/mdfh_pinning_bench
./build/benchmark/bench/mdfh_latency_bench

# Linux-only: perf/flame graph, Valgrind, and a genuine hard-pin run
docker build -f docker/Dockerfile -t mdfh-linux-bench .
docker run --rm mdfh-linux-bench                                    # pinning benchmark
docker run --rm --entrypoint bash mdfh-linux-bench                  # drop in for perf/valgrind (see docker/README.md, docs/perf/README.md)

# Sanitizers
cmake --preset debug && cmake --build build/debug -j && ctest --preset debug
cmake --preset debug-tsan && cmake --build build/debug-tsan -j && ctest --preset debug-tsan
```
