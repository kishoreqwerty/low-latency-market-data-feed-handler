# Low-Latency Market Data Feed Handler

A from-scratch C++20/23 pipeline that ingests a simulated binary exchange market
data feed (multicast-style, sequence-numbered, with injected packet loss),
decodes it, reconstructs a live limit order book per symbol, and publishes
book-delta updates downstream — built from low-level primitives (lock-free
per-shard queues, coroutine-based async I/O, pre-allocated memory pools,
CPU-pinned threads) rather than high-level language abstractions.

Every performance number in this document was measured in this repo's own
benchmark binaries, on real hardware (native Apple Silicon macOS and a Linux
Docker container), not estimated. Full detail and reproduction commands:
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md). Interactive reports:

- [profiling & flame graph writeup](docs/perf/latency_report.html) — GitHub renders this as source, download or clone locally to view interactively
- [consolidated benchmark report](docs/benchmarks_report.html) — GitHub renders this as source, download or clone locally to view interactively

---

## What this is

Real exchange feeds (mostly UDP multicast) drop packets, and a market-making
or pricing engine downstream needs the freshest, correct book state with
predictable — not just low-average — latency. This project builds the full
pipeline that problem implies:

- Ingest a binary feed, decode Add/Cancel/Execute/Replace order events
- Maintain an accurate, price-time-priority limit order book per symbol
- Detect and recover from dropped/out-of-order messages (sequence gaps)
- Publish book-delta updates (not full snapshots) to a downstream consumer
- Do all of the above with instrumented, measured, per-stage latency —
  not a black box

## Architecture

```
Feed generator (multicast, sequence numbers, simulated loss)
        │
        ▼
Pinned network I/O + demux (coroutines, buffer pool, split by shard)
        │  per-shard SPSC ring buffer (bounded, drop-oldest)
        ▼
Pinned decoder + order book (per shard: gap detection, object-pool alloc)
        │  per-shard SPSC output queue (bounded, same policy)
        ▼
Pinned publisher threads (per shard)

Observability: perf/flame graphs, latency percentiles, gap + drop counters,
ASan/TSan/Valgrind-verified builds (separate from the optimized benchmark build)
```

**Why this is genuinely SPSC throughout, not mislabeled MPSC:** sharding by
symbol happens immediately, in the network I/O stage — one dedicated decoder
thread owns each shard end-to-end, and that same shard's own publisher thread
owns its output. That means every queue in the pipeline has exactly one
producer and one consumer, which is what makes the simpler, faster SPSC
lock-free ring buffer correct everywhere, instead of needing a heavier MPSC
structure. Every stage after the initial demux touches only its own shard's
memory — no cross-shard locking anywhere in the steady-state path.

---

## Headline finding: the polling loop set the latency floor — found, then fixed

**The order book itself was always fast** — sub-microsecond at p50 on both
platforms it was measured on. Phase 10's instrumentation found that what
actually showed up in tick-to-book-update latency was something else
entirely: the pipeline's own idle-wait polling loop. Phase 12 then replaced
that loop with a real blocking wait and re-measured, confirming the
diagnosis by fixing it and watching the number move.

**The diagnosis** (Phase 10), confirmed **three independent ways**, not
asserted from one measurement:

1. **Stage-boundary latency instrumentation** (`bench/mdfh_latency_bench`):
   `input_queue_wait`/`output_queue_wait` — time a message spent sitting in a
   shard's ring buffer before its consumer thread got to it — averaged
   **~32µs on macOS / ~70µs on Linux**, closely tracking the decoder and
   publisher threads' `sleep_for(50µs)` idle-poll interval almost exactly.
   Meanwhile `book_update` (the actual `OrderBookT::apply()` call) averaged
   **333ns / 792ns at p50**.
2. **Linux CPU sampling** (`perf record -F 999 -g`): **33.46%** of all
   samples landed in `clock_nanosleep` — the single largest line item in the
   entire profile, bigger than every order-book function combined.
3. **Native macOS CPU sampling** (`/usr/bin/sample`, no container or VM
   involved): independently showed the same `nanosleep` dominance on real
   hardware, ruling out "this is just a Docker artifact."

**The fix** (Phase 12): `concurrency/SpscRingBuffer` gained a real OS-level
blocking wait (`wait_for_data()`/`notify_waiters()`, built on C++20's
`std::atomic::wait`/`notify` — futex on Linux, `ulock` on macOS), replacing
`io/pipeline_runner.cpp`'s `sleep_for(50µs)` poll entirely. Applying the same
Phase 5-level rigor to new synchronization surface caught a real bug before
it ever reached the pipeline: `std::atomic::wait(old)` only returns when the
*watched value itself* changes, so notifying a waiter for the shutdown case
(nothing pushed, the ring buffer's tail index unchanged) was silently
ineffective — a dedicated 2-thread test built to exercise exactly that path
hung indefinitely, caught by TSan-preset testing before it ever touched
`PipelineRunner`. Fixed with a dedicated generation counter that every
wake-worthy event unconditionally increments (see
`concurrency/spsc_ring_buffer.hpp`'s header comment for the full mechanism).

**The result**, same workload, same both platforms:

| Platform | `tick_to_book_update` | Before (poll) | After (blocking) | Improvement |
|---|---|---|---|---|
| macOS | mean | 33,420 ns | 5,306 ns | **6.3x** |
| macOS | p50 | 32,417 ns | 4,000 ns | **8.1x** |
| Linux (Docker) | mean | 70,595 ns | 31,031 ns | **2.3x** |
| Linux (Docker) | p50 | 67,625 ns | 29,208 ns | **2.3x** |

Both platforms improved substantially, but by different amounts — itself a
real, measured finding: a post-fix macOS `sample` capture shows **zero**
`nanosleep` anywhere in the profile (genuinely near-zero-CPU blocking), while
Linux's `perf` profile shows the poll replaced by meaningful time in
`__sched_yield`/`std::__atomic_wait_address_v` — glibc's `atomic::wait`
spin-yields for a period before committing to a real futex block, unlike
macOS's implementation. A platform difference in the C++ standard library,
not a bug in this project.

See [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) (Phase 10 and Phase 12
sections) and [`docs/perf/`](docs/perf/) for the full breakdown and artifacts.

---

## Key design decisions, and what the numbers say about each one

Every row below is a real decision with a real alternative that was
consciously not taken. Where this project has a measured number bearing on
the decision, it's included — this section is meant to be argued from, not
just read.

| Decision | Alternative considered | Why this choice | What was measured |
|---|---|---|---|
| **Shard by symbol at ingestion, not later** | Single shared book with locking | Avoids lock contention entirely; every shard's decoder/publisher/pool is touched by exactly one thread, so no queue or book anywhere needs a lock | The Phase 7→9 pool redesign made this concrete: a process-wide pool behind a mutex (an earlier iteration of this project) reintroduced the exact cross-shard contention sharding exists to remove. Fixed by giving each shard its own pool instances, verified race-free under repeated TSan runs |
| **Lock-free SPSC ring buffers per shard** | Mutex-protected queues, general MPSC | SPSC lock-free is simpler and faster than MPSC, and is *correct* here because sharding guarantees single-producer/single-consumer on every queue | Getting the drop-oldest overwrite path race-free took three distinct, empirically-found fixes (see `concurrency/spsc_ring_buffer.hpp`'s header comment) — found by a dedicated stress test under TSan, not by inspection |
| **Drop-oldest backpressure policy** | Block producer / grow buffer unbounded | Current book state matters more than a backlog of stale state for live market data; blocking risks cascading latency, unbounded growth risks memory blowup under a stuck consumer | Exercised directly by the adversarial shutdown-handshake test (`io/test_pipeline_e2e.cpp`), which deliberately makes a publisher thread lag its decoder and confirms zero *incorrect* loss — only genuine backpressure drops, never a dropped-but-uncounted delta |
| **Pool-allocated objects, no hot-path `new`** | Standard heap allocation per message | Heap allocation has unpredictable latency (allocator locks, page faults, arena contention); pools trade a fixed startup cost and a memory ceiling for predictable steady-state cost | **Zero heap calls** for 4,000,000 messages, both platforms, confirmed via a real global-operator-override counter, not assumed. 1.79x (macOS) / 1.19x (Linux) wall-clock speedup over the identical naive-allocator logic — see `docs/BENCHMARKS.md` Phase 3 |
| **Explicit STALE marking on gap detection** | Silently continue with best-effort state | A silently wrong book is worse than a visibly stale one — a downstream consumer can pause trading on that symbol rather than act on bad data unknowingly | Exercised live, under real concurrent load, by the Phase 7 e2e test: a gap is observed and marked STALE *while other shards are still actively processing*, not only after the fact |
| **CPU-pinned threads per stage** | Let the OS scheduler place threads | Reduces cache-line bouncing and context-switch jitter for producer/consumer thread pairs that share data | Measured, not assumed, on both platforms — see next paragraph, this is the one decision whose *measured* payoff was smaller than expected |
| **Coroutines for network I/O** | Thread-per-connection or blocking I/O | Non-blocking I/O avoids a thread idling on a blocking read; coroutines keep the suspend/resume mechanism readable, at the cost of some C++20 learning curve | Verified clean shutdown under every tested scenario — stop mid-flight, stop before a coroutine starts, destructor-only cleanup — with no orphaned coroutine frames, under ASan across all of them |
| **Blocking consumer wait via `std::atomic::wait`/`notify`** (Phase 12) | Keep the fixed `sleep_for(50µs)` poll | A poll bounds best-case latency to roughly the poll interval regardless of how fast the rest of the pipeline is; a real OS-level block wakes as soon as data (or a shutdown signal) actually arrives | tick-to-book-update p50 improved **8.1x on macOS, 2.3x on Linux** for the identical workload — see the headline finding above. Also surfaced a real missed-wakeup bug (notifying on a value that hadn't changed) via a dedicated 2-thread test *before* it reached the pipeline, not after |

**On CPU pinning specifically** — this is the one row where the honest answer
is "the mechanism differs by platform, and neither showed a measurable win
for this workload":

- **macOS**: `thread_policy_set`/`THREAD_AFFINITY_POLICY` returned
  `KERN_NOT_SUPPORTED` for **every one of 45 pin attempts** across 5 runs —
  not a silently-ignored soft hint, an outright kernel rejection, verified
  with a standalone probe outside the benchmark itself.
- **Linux (Docker)**: `pthread_setaffinity_np` succeeded for **all 45**
  attempts — a genuine, kernel-enforced hard pin. But the pinned/unpinned
  delta was still noise-level (0.03%), because Docker Desktop's Linux runs in
  a VM, not bare metal — a probe found `sleep_for(50µs)` takes ~133µs actual
  in-container vs. ~70µs natively, which (per the headline finding above)
  swamps anything pinning could contribute for this poll-bound workload.

Full pinning results: `docs/BENCHMARKS.md` → Phase 8.

---

## Benchmark results (summary)

Full tables, both platforms, every stage: [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

| | macOS (native) | Linux (Docker) |
|---|---|---|
| Pool-backed allocation, 4M messages | 66.59 ms, **0** heap calls | 70.42 ms, **0** heap calls |
| Naive allocation, same workload | 119.15 ms, 4,000,000 heap calls | 83.58 ms, 4,000,000 heap calls |
| Affinity hints accepted | 0 / 45 (`KERN_NOT_SUPPORTED`) | 45 / 45 (genuine hard pin) |
| tick → book update, p50 (poll, Phase 10) | 32,417 ns | 67,625 ns |
| tick → book update, p50 (blocking, Phase 12) | **4,000 ns** (8.1x) | **29,208 ns** (2.3x) |
| book update alone, p50 | 333 ns | 792 ns |
| ASan + UBSan | 71/71 tests, 3 clean runs | — |
| TSan | 71/71 tests, 3 clean runs | — |
| Valgrind (`--leak-check=full`) | — | see `docs/BENCHMARKS.md` |

---

## Sequence gap detection & recovery

Every message carries a monotonically increasing per-shard sequence number.
The decoder thread tracks the last-seen sequence number per shard; on a gap
(received seq > expected seq + 1) it logs the gap, increments a
`sequence_gaps_total` counter, and marks that shard's book state `STALE`
until a resync lands. This is exercised under real concurrent load, not
simulated single-threaded: the end-to-end test confirms a gap is observed
and marked *while other shards are still actively processing*, and that
resync (`GapDetector::resync()`) correctly clears the STALE flag afterward.

---

## Production considerations (explicitly out of scope)

Stated for honesty, not omitted quietly. None of these were built, and
building them would blow the timeline of a portfolio project without adding
proportional value over what's already demonstrated:

- Kernel bypass networking (DPDK, Solarflare) for true wire-to-wire microsecond latency
- FPGA/hardware acceleration for the hottest paths
- Real exchange colocation and physical network topology
- Full ITCH/OUCH protocol compliance rather than a simplified fixed-size subset
- Production-grade failover across redundant feed sources (most real exchanges provide A/B multicast feeds; this design assumes one)
- Process crash / restart recovery — a real system would need book state recovery from an exchange snapshot service; this project's gap/resync mechanism handles *in-stream* loss, not a full process restart

One thing *not* on the original out-of-scope list that turned out to matter
in practice: the polling-loop latency floor described above. That's a real
production consideration this build surfaced empirically rather than one
that was anticipated up front.

---

## Demo: run the feed generator, run the pipeline, observe output

Build first (see [Build & test](#build--test) below), then:

**1. Run the feed generator + full live pipeline, see it work:**

```sh
./build/benchmark/bench/mdfh_latency_bench
```

This builds a `FeedGenerator` (the simulated multicast feed), spins up a real
`PipelineRunner` — network I/O thread, per-shard decoder threads, per-shard
publisher threads, all genuinely concurrent — runs 1,000,000 messages through
it end to end, and prints the tick-to-book-update latency percentiles shown
above. `./build/benchmark/bench/mdfh_pinning_bench` and
`./build/benchmark/bench/mdfh_allocation_bench` do the same for the pinning
and allocation numbers respectively.

**2. Observe actual book-delta output**, not just performance numbers — save
this as a scratch `.cpp`, compile it against the library (or drop it in
`bench/` and add an `add_executable` line, same pattern as the files above),
and run it:

```cpp
#include <iostream>
#include "io/pipeline_runner.hpp"
#include "publisher/publisher.hpp"

using namespace mdfh::io;
namespace protocol  = mdfh::protocol;
namespace publisher = mdfh::publisher;

struct StdoutSink : publisher::DeltaSink {
    void publish(const publisher::BookDelta& d) override {
        std::cout << publisher::format_delta_line(d) << "\n";
    }
};

int main() {
    FeedGenerator::Config feed_config{
        .symbols          = {protocol::make_symbol("AAPL"), protocol::make_symbol("MSFT")},
        .num_shards       = 2,
        .message_count    = 2000,
        .packet_loss_rate = 0.02,  // 2% simulated loss -- watch for STALE markings in stderr
        .seed             = 1,
    };

    PipelineRunner runner(feed_config, 2);
    runner.set_delta_sink(0, std::make_unique<StdoutSink>());
    runner.set_delta_sink(1, std::make_unique<StdoutSink>());

    runner.start();
    runner.join();

    std::cout << "\ngenerated=" << runner.feed().total_generated()
              << " dropped=" << runner.feed().total_dropped() << "\n";
}
```

Each printed line is one real `BookDelta` — symbol, side, price, and either
the level's new aggregate quantity or `REMOVED` if that price level just
emptied out. Expect stderr gap-detector logs (`[gap_detector] sequence
gap...`) to interleave with the stdout delta lines — both shards' threads
write concurrently, so that's real concurrency showing through, not a bug;
redirect stderr elsewhere (`2>/dev/null`) if you just want the clean delta
stream. `publisher::FileDeltaSink` (used the same way,
`std::make_unique<publisher::FileDeltaSink>(path)`) writes the identical
lines to a file per shard instead, if you'd rather `tail -f` it — see
`publisher/publisher.hpp`.

**3. Run the full test suite** (correctness, not performance):

```sh
ctest --preset benchmark   # 71 tests, optimized build, ~14s
```

---

## Build & test

Three CMake presets, matching what each is for:

```sh
cmake --preset benchmark && cmake --build build/benchmark -j && ctest --preset benchmark   # optimized, no sanitizers
cmake --preset debug      && cmake --build build/debug -j      && ctest --preset debug       # ASan + UBSan
cmake --preset debug-tsan && cmake --build build/debug-tsan -j && ctest --preset debug-tsan   # TSan
```

Linux-only tooling (real hard-pin affinity, `perf`, Valgrind — none has
meaningful Apple Silicon macOS support) runs in the Docker container built
for that purpose:

```sh
docker build -f docker/Dockerfile -t mdfh-linux-bench .
docker run --rm mdfh-linux-bench                             # pinning benchmark
docker run --rm --entrypoint bash mdfh-linux-bench            # drop in for perf/Valgrind, see docker/README.md
```

## Project structure

```
protocol/      wire format: encoder/decoder, message types
orderbook/     limit order book engine + per-shard pool allocator
concurrency/   SPSC ring buffer, gap detector, latency-trace timestamps
io/            feed generator, coroutine network I/O, shard demux, pipeline orchestration
affinity/      CPU affinity (macOS thread_policy_set / Linux pthread_setaffinity_np)
publisher/     book-delta type, downstream sinks (file/null/latency-recording)
bench/         allocation, pinning, and latency benchmarks
docker/        Linux container for perf/Valgrind/genuine hard-pin affinity
docs/          consolidated benchmark numbers, profiling reports, Valgrind logs
```

Stack: C++20/23, CMake, Catch2, perf + Brendan Gregg's FlameGraph, Valgrind,
ASan/UBSan/TSan.
