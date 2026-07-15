# Low-Latency Market Data Feed Handler — High-Level Design (HLD)

---

## 1. Problem Statement

Design a system that ingests a real-time exchange market data feed (order add/cancel/execute/replace events), reconstructs an accurate, low-latency view of the limit order book per symbol, and publishes book state updates to downstream consumers (e.g. a pricing/strategy engine) — with minimal and predictable tick-to-publish latency.

---

## 2. Functional Requirements

- Ingest a binary market data feed over the network (multicast-style, simulated)
- Decode messages: Add, Cancel, Execute, Replace order events
- Maintain an accurate limit order book per symbol (price-time priority)
- Detect and recover from dropped/out-of-order messages (sequence gaps)
- Publish best bid/ask and depth snapshots to downstream consumers
- Expose latency and health metrics (gaps, drops, per-stage timing)

## 3. Non-Functional Requirements

| Requirement | Target (stated, not guaranteed — see Section 9) |
|---|---|
| Tick-to-book-update latency | Sub-millisecond p99, documented via benchmark |
| Throughput | Handle feed rates comparable to a busy single-exchange symbol universe (thousands of msgs/sec/symbol, tens of thousands aggregate) |
| Correctness | Order book state must never silently diverge from the true exchange state — prefer explicit STALE marking over silent incorrectness |
| Predictability | Low variance (tight p99/p99.9), not just low average latency — this matters more than raw average in trading contexts |
| Resource discipline | Zero dynamic allocation in the steady-state hot path |

---

## 4. High-Level Components

| Component | Responsibility |
|---|---|
| **Feed Generator** (test harness, not production) | Emits a simulated binary feed with sequence numbers and injected packet loss, to exercise the system realistically |
| **Network I/O + Shard Demux** | Reads raw packets asynchronously (coroutine-driven), routes each message to its symbol's shard with zero-copy where possible |
| **Per-Shard Ring Buffer** | Lock-free, bounded, single-producer single-consumer handoff between I/O and decode stages |
| **Gap Detector** | Tracks per-shard sequence continuity; flags gaps, marks shard state STALE, triggers resync |
| **Decoder + Order Book Engine** | Parses binary messages, applies them to in-memory book state using pool-allocated objects |
| **Publisher** | Emits book state changes (best bid/ask, depth) to downstream consumers per shard |
| **Observability Layer** | Cross-cutting: latency histograms, gap/drop counters, profiling hooks |

---

## 5. High-Level Data Flow

```
Feed Generator → Network I/O (async, pinned) → Shard Demux
    → [per-shard] Ring Buffer → Gap Detector → Decoder → Order Book
    → [per-shard] Output Queue → Publisher → Downstream Consumer
```

Sharding by symbol happens once, early (at demux), and every component downstream of that point owns its shard exclusively — this is the design choice that keeps every internal queue genuinely single-producer/single-consumer rather than needing more complex multi-producer coordination.

---

## 6. Key Design Decisions & Tradeoffs

This is the section most useful for walking through out loud in an interview — every row is a real decision with a real alternative that was consciously not taken.

| Decision | Alternative considered | Why this choice | What was measured (before/after) |
|---|---|---|---|
| Shard by symbol at ingestion, not later | Single shared book with locking | Avoids lock contention entirely; trades a small amount of demux complexity for eliminating the biggest latency risk (lock contention under load) | The Phase 7→9 pool redesign made this concrete: a process-wide pool behind a mutex (an earlier iteration) reintroduced the exact cross-shard contention sharding exists to remove; fixed by giving each shard its own pool instances, verified race-free under repeated TSan runs |
| Lock-free SPSC ring buffers per shard | Mutex-protected queues | SPSC lock-free is simpler and faster than general-purpose lock-free MPSC or mutex-based queues, and is correct here because sharding guarantees single-producer/single-consumer | Getting the drop-oldest overwrite path race-free took three distinct, empirically-found fixes (see `concurrency/spsc_ring_buffer.hpp`'s header comment), found by a dedicated stress test under TSan, not by inspection |
| Drop-oldest backpressure policy | Block producer / grow buffer unbounded | For live market data, current state matters more than a backlog of stale state; blocking risks cascading latency, unbounded growth risks memory blowup under a sustained slow-consumer scenario | Exercised directly by the adversarial shutdown-handshake test (`io/test_pipeline_e2e.cpp`), which deliberately makes a publisher thread lag its decoder and confirms zero *incorrect* loss — only genuine backpressure drops, never a dropped-but-uncounted delta |
| Pool-allocated objects, no hot-path `new` | Standard heap allocation per message | Heap allocation has unpredictable latency (allocator locks, page faults); pools trade a fixed startup cost and memory ceiling for predictable steady-state latency | **Zero heap calls** for 4,000,000 messages, both platforms, confirmed via a real global-operator-override counter. 1.79x (macOS) / 1.19x (Linux) wall-clock speedup over identical naive-allocator logic — see `docs/BENCHMARKS.md` Phase 3 |
| Explicit STALE marking on gap detection | Silently continue with best-effort state | A silently wrong book is worse than a visibly stale one — downstream consumers can make an informed choice (pause trading on that symbol, wait for resync) rather than act on bad data unknowingly | Exercised live, under real concurrent load, by the Phase 7 e2e test: a gap is observed and marked STALE *while other shards are still actively processing*, not only after the fact |
| CPU-pinned threads per stage | Let the OS scheduler place threads | Reduces cache-line bouncing and context-switch jitter; the tradeoff is reduced flexibility/portability and a manual core-layout decision that has to be tuned per deployment target | Measured, not assumed, on both platforms: macOS's `thread_policy_set` returned `KERN_NOT_SUPPORTED` for all 45 pin attempts across 5 runs; Linux's `pthread_setaffinity_np` succeeded for all 45, but the pinned/unpinned delta was still noise-level (0.03%) under Docker Desktop's VM — see `docs/BENCHMARKS.md` Phase 8 |
| Coroutines for network I/O | Thread-per-connection or blocking I/O | Non-blocking I/O avoids threads idling on syscalls; coroutines keep the code more readable than raw callback-based async, at the cost of some C++20 learning curve | Verified clean shutdown under every tested scenario — stop mid-flight, stop before a coroutine starts, destructor-only cleanup — with no orphaned coroutine frames, under ASan across all of them |
| Blocking consumer wait via `std::atomic::wait`/`notify` (Phase 12) | Keep the fixed `sleep_for(50µs)` poll | A poll bounds best-case latency to roughly the poll interval regardless of how fast the rest of the pipeline is; a real OS-level block wakes as soon as data (or a shutdown signal) actually arrives | tick-to-book-update p50 improved **8.1x on macOS (32,417ns → 4,000ns), 2.3x on Linux (67,625ns → 29,208ns)** for the identical workload. Also surfaced a real missed-wakeup bug (notifying on a value that hadn't changed) via a dedicated 2-thread test *before* it reached the pipeline — fixed with a dedicated `wake_generation_` counter, see `docs/BENCHMARKS.md` Phase 12 |

---

## 7. Capacity Estimation (back-of-envelope, stated assumptions)

- Assume ~500 actively traded symbols, ~50 msgs/sec/symbol average, bursting to 500 msgs/sec/symbol during volatile periods
- Aggregate: ~25K msgs/sec average, ~250K msgs/sec peak
- Each message: ~32-48 bytes fixed-size on the wire → aggregate bandwidth is modest (a few MB/sec even at peak), so the bottleneck is expected to be **processing latency per message**, not raw network bandwidth
- Per-shard ring buffer sized to absorb a few hundred milliseconds of peak burst before backpressure kicks in (tunable; documented in README as a configurable parameter, not a hardcoded assumption)

---

## 8. Interfaces Between Components

- **Network I/O → Ring Buffer:** fixed-size message struct, pushed by value (no pointers crossing the lock-free boundary, to avoid lifetime/ownership hazards)
- **Decoder → Order Book:** direct in-process function calls (same thread, no queue needed — this stage is intentionally not further parallelized to keep book mutation logic simple and provably correct)
- **Order Book → Publisher:** book delta struct (price level changed, new best bid/ask), not full book snapshots, to minimize per-update payload size

---

## 9. Failure Modes & Handling

| Failure | Handling |
|---|---|
| Packet loss / sequence gap | Detected by Gap Detector, shard marked STALE, resync triggered |
| Slow downstream consumer | Drop-oldest backpressure at the output queue, with a dropped-updates counter for observability |
| Decoder falls behind burst traffic | Ring buffer absorbs short bursts; sustained overload triggers backpressure at the input side too, same drop-oldest policy |
| Process crash / restart | Out of scope for this project (see Production Considerations) — a real system would need book state recovery from an exchange snapshot service |

---

## 10. Explicitly Out of Scope (see full architecture doc, Section 13, for detail)

Kernel bypass networking, FPGA acceleration, real exchange colocation, full ITCH/OUCH compliance, multi-feed failover/redundancy, crash recovery.

---

## 11. How to Use This Document

This HLD is the "what and why" — pair it with the full Architecture Specification document (component internals, folder structure, phased build plan) for the "how." In an interview, this document's Section 6 (Key Design Decisions & Tradeoffs) is the part worth having memorized well enough to discuss any row in depth, since that's the style of question Optiver and Akuna-style interviews are confirmed to ask.
