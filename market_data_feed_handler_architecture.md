# Low-Latency Market Data Feed Handler — Architecture Specification (v2, revised)

**A from-scratch C++20/23 systems project targeting HFT / market-making roles (Akuna, Optiver, and similar)**

---

## 1. System Overview

A pipeline that receives a simulated binary exchange market data feed (multicast, sequence-numbered, with simulated packet loss), decodes it, reconstructs a live limit order book, and publishes updates downstream — built entirely from low-level primitives (lock-free per-shard queues, coroutine-based async I/O, pre-allocated memory pools, CPU-pinned threads) rather than high-level language abstractions.

**What changed from v1, and why:** the original design had an SPSC-to-MPSC inconsistency (a single decoder feeding a queue labeled multi-producer), no sequence-gap handling, no stated memory allocation strategy, no CPU affinity, and no explicit backpressure policy. All five are fixed below — these are exactly the details a technical interviewer at a firm like this would probe on.

---

## 2. Architecture (corrected six-stage pipeline)

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

Observability: perf/vtune, latency histograms, gap + drop counters,
ASan/TSan test builds (separate from benchmark builds)
```

**Why this is genuinely SPSC throughout, not mislabeled MPSC:** demultiplexing by symbol shard happens immediately in the network I/O stage — one dedicated decoder thread owns each shard end-to-end. That means every queue in the pipeline has exactly one producer and one consumer, which is what actually lets you use the simpler, faster SPSC lock-free ring buffer everywhere instead of a heavier MPSC structure. If you ever need multiple network threads feeding one shard, that's the point where you'd genuinely need MPSC — but for this design, single-producer-per-shard is both simpler and truer to how real feed handlers partition work.

---

## 3. Sequence Gap Detection & Recovery

Real market data feeds (especially UDP multicast, which most exchanges use) drop packets. This pipeline treats that as a first-class concern, not an edge case:

- Every message carries a monotonically increasing per-shard sequence number
- The decoder thread tracks the last-seen sequence number per shard
- On a gap (received seq > expected seq + 1): log the gap, increment a `sequence_gaps_total` counter, and mark the book state for that shard as `STALE` until either a retransmission request is fulfilled (simulate a simple request/replay mechanism against the feed generator) or a full snapshot resync occurs
- This gives you a genuine, demonstrable answer to "what happens when a packet gets dropped" — a very likely interview follow-up

---

## 4. Memory Allocation Strategy (hot-path discipline)

No `malloc`/`new` calls in the steady-state decode → book-update → publish path:

- **Object pools**, pre-allocated at startup, sized for the maximum expected order count per shard — order objects, book-level nodes, and message structs are all pool-allocated and recycled, not heap-allocated per message
- **Fixed-size messages** on the wire (protocol section), so decoding never needs variable-length allocation
- Document this explicitly in the README, and ideally show it with a benchmark comparing pool-allocated vs. naive `new`-per-message throughput — a concrete, visual way to prove you understand *why* this matters, not just that you did it

---

## 5. CPU Affinity / Thread Pinning

- Each pipeline stage's threads (network I/O, per-shard decoder, per-shard publisher) are pinned to specific cores via `pthread_setaffinity_np` (or `std::thread::native_handle()` + platform affinity call)
- Avoid placing threads that communicate heavily (e.g. a shard's decoder and its publisher) on hyperthread siblings of unrelated shards, to reduce cache-line contention and context-switch jitter
- Document core layout in the README (e.g. "cores 0-1 reserved for OS/kernel, cores 2-9 pinned one-per-shard-decoder, cores 10-17 for publishers")

---

## 6. Backpressure Policy (explicit, not implied)

Every bounded queue in the pipeline (per-shard SPSC buffers) uses a **stated, deliberate policy**: on overflow, **drop the oldest entry**, not the newest, and increment a `dropped_updates_total` counter per shard. Rationale to document in the README: for live market data, the most recent book state matters more than a stale one — a consumer that's fallen behind is better served by catching up to current reality than by processing a backlog of outdated ticks. This is a defensible, explainable design choice, which is exactly what interviewers want to see reasoned through rather than left implicit.

---

## 7. Protocol & Message Format

| Message type | Fields |
|---|---|
| `AddOrder` | seq_num, order_id, symbol, side, price, quantity, timestamp |
| `CancelOrder` | seq_num, order_id, timestamp |
| `ExecuteOrder` | seq_num, order_id, executed_quantity, timestamp |
| `ReplaceOrder` | seq_num, old_order_id, new_order_id, price, quantity, timestamp |

Fixed-size, fixed-layout binary encoding (no variable-length fields) to support pool-based allocation. Encoder (feed generator side) and decoder (consumer side) both hand-written.

---

## 8. Order Book Engine

- Price levels mapped to FIFO order queues (price-time priority — logic proven in the earlier Go Crypto Order Book Engine, reimplemented from scratch in C++ with pool-allocated nodes)
- Applies decoded messages to mutate book state
- Exposes best bid/ask and depth snapshots to the publisher stage
- Tracks per-shard `STALE`/`LIVE` status based on gap detection state

---

## 9. Performance Measurement

- Tick-to-book-update latency instrumented at every stage boundary (receipt → demux → decode → book update → publish)
- `perf record` / `perf report`, flame graphs generated and included in the README
- p50/p99/p99.9 latency numbers documented, both with and without CPU pinning enabled (a genuinely interesting comparison to show you understand *why* pinning matters, not just that you added it)
- Full test suite run under Valgrind and AddressSanitizer/ThreadSanitizer — **in a separate debug build**, not the benchmark build, since sanitizers distort timing
- Object-pool vs. naive-allocation throughput benchmark (see Section 4)

---

## 10. Technology Stack

- **Language:** C++20 minimum, C++23 features where natural (`std::expected` for error handling)
- **Build system:** CMake, with separate `benchmark` and `debug` (sanitizer-enabled) build configurations
- **Testing:** Catch2 + GMock
- **Profiling:** `perf`, `vtune` if accessible
- **Networking:** raw sockets or `liburing`, UDP multicast simulation for the feed generator

---

## 11. Suggested Folder Structure

```
market-data-feed-handler/
├── protocol/
│   ├── encoder.hpp/.cpp
│   └── decoder.hpp/.cpp
├── orderbook/
│   ├── order_book.hpp/.cpp
│   ├── object_pool.hpp          # pool allocator for orders/messages
│   └── test_order_book.cpp
├── concurrency/
│   ├── spsc_ring_buffer.hpp     # per-shard, bounded, drop-oldest
│   └── gap_detector.hpp         # sequence gap tracking + resync trigger
├── io/
│   ├── async_network_reader.cpp
│   └── shard_demux.cpp
├── affinity/
│   └── thread_pinning.hpp       # CPU affinity helpers
├── publisher/
│   └── publisher_thread.cpp
├── bench/
│   ├── latency_bench.cpp
│   └── allocation_bench.cpp     # pool vs. naive allocation comparison
├── CMakeLists.txt
└── README.md
```

---

## 12. Phased Build Plan

1. **Phase 1:** Protocol definition (fixed-size messages), encoder, decoder + unit tests
2. **Phase 2:** Order book engine with object-pool allocation
3. **Phase 3:** Per-shard SPSC ring buffers with drop-oldest backpressure
4. **Phase 4:** Sequence gap detection + resync logic
5. **Phase 5:** Coroutine-based async network I/O + shard demux
6. **Phase 6:** CPU affinity/thread pinning
7. **Phase 7:** Performance measurement — latency instrumentation, allocation benchmark, pinned-vs-unpinned comparison, flame graphs, sanitizer runs
8. **Phase 8:** Polish — README with architecture diagram, all benchmark numbers, explicit production-gap notes

---

## 13. Production Considerations (explicitly out of scope, stated for honesty)

- Kernel bypass networking (DPDK, Solarflare) for true wire-to-wire microsecond latency
- FPGA/hardware acceleration for the hottest paths
- Real exchange colocation and physical network topology
- Full ITCH/OUCH protocol compliance rather than a simplified fixed-size subset
- Production-grade failover across redundant feed sources (most real exchanges provide A/B multicast feeds; this design assumes one)
