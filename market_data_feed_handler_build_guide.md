# Low-Latency Market Data Feed Handler — Step-by-Step Build Guide

Build one verified piece at a time. Each step has a **Done when** criterion — don't move to the next step until that's true. This sequencing follows the Architecture Spec and HLD, and covers every element we corrected/added in v2 (sharded SPSC, gap detection, object pools, CPU pinning, backpressure) so nothing gets missed.

---

## Phase 0 — Project scaffolding

- [ ] Set up CMake project with two build configurations: `benchmark` (optimized, no sanitizers) and `debug` (ASan + TSan enabled)
- [ ] Set up Catch2 + GMock
- [ ] Create the folder structure from the Architecture Spec (`protocol/`, `orderbook/`, `concurrency/`, `io/`, `affinity/`, `publisher/`, `bench/`)

**Done when:** `cmake --build` succeeds in both configs, and a trivial "hello world" Catch2 test passes.

---

## Phase 1 — Protocol (fixed-size binary messages)

- [ ] Define fixed-size structs for `AddOrder`, `CancelOrder`, `ExecuteOrder`, `ReplaceOrder`, each including a `seq_num` field (this is required for Phase 4's gap detection, so don't skip it now)
- [ ] Write the encoder (packs a struct into bytes)
- [ ] Write the decoder (unpacks bytes into a struct, validates message type/length)
- [ ] Unit test: round-trip encode → decode for each message type, plus malformed-input handling (truncated message, invalid type byte)

**Done when:** all four message types round-trip correctly under test, and the decoder rejects malformed input without crashing.

---

## Phase 2 — Order book engine (single-threaded first, no concurrency yet)

- [ ] Implement price-level structure: price → FIFO queue of orders (price-time priority) — reuse the logic you already proved in the Go engine, rewritten in C++
- [ ] Implement `apply(AddOrder)`, `apply(CancelOrder)`, `apply(ExecuteOrder)`, `apply(ReplaceOrder)` mutating book state
- [ ] Implement `best_bid_ask()` and `depth_snapshot(levels)` read methods
- [ ] Unit test: apply a scripted sequence of messages, assert book state matches hand-computed expected state at each step, including edge cases (cancel a non-existent order, execute more quantity than remains)

**Done when:** book state is provably correct against hand-verified scenarios — **do this before adding any concurrency**, so correctness bugs aren't tangled up with concurrency bugs later.

---

## Phase 3 — Object pool allocator (no hot-path `new`)

- [ ] Implement a fixed-size object pool for order/book-node objects, pre-allocated at startup
- [ ] Wire the order book engine (Phase 2) to allocate from the pool instead of `new`
- [ ] Write `bench/allocation_bench.cpp`: benchmark pool allocation vs. naive `new` per message, across a realistic message volume
- [ ] Document the benchmark result — this is one of the two comparison numbers your README needs (see Phase 9)

**Done when:** the order book engine has zero `new`/`malloc` calls in its message-processing path, and you have a real throughput/latency number showing the pool's advantage.

---

## Phase 4 — Sequence gap detection

- [ ] Implement a per-shard gap detector: tracks last-seen `seq_num`, compares against each incoming message's `seq_num`
- [ ] On gap detected: log it, increment `sequence_gaps_total`, mark that shard's book state as `STALE`
- [ ] Implement a simple resync mechanism (simulate a "replay request" against the feed generator, or a full snapshot resync) that clears `STALE` back to `LIVE`
- [ ] Unit test: feed a message sequence with an intentional gap, assert `STALE` is set; feed the resync, assert it clears

**Done when:** you can deliberately drop a message in a test and watch the system correctly detect, flag, and recover from it.

---

## Phase 5 — Lock-free SPSC ring buffer (the hardest phase — budget real time here)

- [ ] Before writing your own: read a reference implementation or two (e.g. Folly's or Boost's lock-free SPSC queue) to understand common pitfalls — this is study time, not copying
- [ ] Implement a bounded SPSC ring buffer using `std::atomic` with explicit `memory_order_acquire`/`memory_order_release` semantics
- [ ] Implement the drop-oldest backpressure policy on overflow, incrementing `dropped_updates_total`
- [ ] Unit test single-threaded first (push/pop correctness, wraparound, full/empty edge cases)
- [ ] **Concurrency test:** run producer and consumer on separate real threads, high volume, and run this test under **ThreadSanitizer** — this is non-negotiable for this component specifically, since correctness bugs here are the kind that "look fine" until they aren't

**Done when:** the ring buffer passes both single-threaded and real-two-thread stress tests under TSan with zero reported races, and the drop-oldest policy is verified under a deliberately overwhelmed producer.

---

## Phase 6 — Sharding + demux (tie Phases 1-5 together, still single-machine/simulated network)

- [ ] Implement shard assignment (hash symbol → shard index)
- [ ] Wire one ring buffer (Phase 5) + one gap detector (Phase 4) + one order book (Phase 2/3) per shard
- [ ] Implement a demux function that reads a raw message, extracts the symbol, and routes it to the correct shard's ring buffer
- [ ] Integration test: feed a mixed-symbol message stream through the full per-shard pipeline (still on a single thread simulating the feed, not real async yet), assert each shard's book ends up correct and isolated from other shards

**Done when:** a multi-symbol message stream produces correct, independent book state per shard, with no cross-shard interference.

---

## Phase 7 — Coroutine-based async network I/O

- [ ] Build the simulated feed generator (multicast-style, sequence-numbered, with a configurable packet-loss rate for testing Phase 4 realistically)
- [ ] Implement the network I/O layer using C++20 coroutines (or `liburing`) for non-blocking reads
- [ ] Wire network I/O → demux (Phase 6) → per-shard pipelines, now running as real concurrent threads (one decoder thread per shard)
- [ ] End-to-end test: run the full pipeline against the simulated feed generator, including its injected packet loss, and confirm gap detection/resync (Phase 4) triggers correctly under real concurrent conditions, not just the unit test from Phase 4

**Done when:** the full pipeline runs end-to-end against a live (simulated) feed, correctly handling both normal traffic and injected loss, with each shard's decoder running on its own thread.

---

## Phase 8 — CPU affinity / thread pinning

- [ ] Implement thread pinning (`pthread_setaffinity_np` or equivalent) for network I/O thread(s), per-shard decoder threads, and per-shard publisher threads
- [ ] Document your core layout decision (which cores get what, and why — avoid hyperthread siblings for communicating stages)
- [ ] Benchmark: run the full pipeline (Phase 7) both **with and without** pinning enabled, measure and compare latency — this is the second comparison number your README needs

**Done when:** you have real before/after latency numbers showing pinning's effect, not just the code enabled.

---

## Phase 9 — Publisher + downstream interface

- [ ] Implement per-shard publisher threads that read from the order book's output queue (a second SPSC ring buffer, same drop-oldest policy as Phase 5) and emit book deltas (not full snapshots) downstream
- [ ] Simple downstream consumer stub (even just logging or writing to a file) to demonstrate the full pipeline's output

**Done when:** you can observe correct, live book delta output at the far end of the full pipeline.

---

## Phase 10 — Full performance measurement suite

- [ ] Instrument tick-to-book-update latency at every stage boundary
- [ ] Run `perf record`/`perf report`, generate flame graphs
- [ ] Compute and document p50/p99/p99.9 latency
- [ ] Run the full test suite under Valgrind and ASan/TSan in the `debug` build — confirm zero leaks, zero races
- [ ] Collect all benchmark numbers in one place: allocation (Phase 3), pinning (Phase 8), end-to-end latency percentiles

**Done when:** every number referenced in this guide (allocation benchmark, pinning benchmark, latency percentiles, sanitizer results) is actually measured and recorded, not estimated.

---

## Phase 11 — README and polish

- [ ] Architecture diagram (already built — reuse it)
- [ ] All benchmark numbers from Phase 10, with brief explanation of *why* each design choice helped (tie back to the HLD's Key Design Decisions & Tradeoffs table)
- [ ] Explicit "Production Considerations" section (already drafted in the Architecture Spec — copy it in, adjust if anything changed during build)
- [ ] Short demo instructions (how to run the feed generator + pipeline + observe output)

**Done when:** someone who has never seen the project can read the README top to bottom and understand what it does, why it's built the way it is, and see real evidence (numbers, not claims) that it works.

---

## What NOT to do

- Don't build Phase 7 (async I/O) before Phases 1-6 are individually correct — debugging concurrency bugs on top of unverified business logic is much harder than debugging them separately.
- Don't skip the TSan run on the ring buffer (Phase 5) — this is the single most likely place for a subtle, interview-exposing bug.
- Don't add kernel bypass networking, FPGA, or real exchange colocation — these are explicitly out of scope (see Architecture Spec Section 13) and would blow the timeline without adding proportional value for a portfolio project.
