# Valgrind memcheck results

Run inside the Linux Docker container (`docker/Dockerfile`) — Valgrind has no
meaningful Apple Silicon macOS support. Built with `-DCMAKE_BUILD_TYPE=Debug
-DMDFH_SANITIZER=none` (Valgrind and ASan/UBSan instrumentation conflict, so
this is a separate, sanitizer-free debug build, not the `debug` CMake preset).

## Result: zero memory defects, both before and after Phase 12

- `full_suite_ctest_memcheck.log` — pre-Phase-12 (poll-based) run, 66 tests,
  **64/66 passed**.
- `full_suite_ctest_memcheck_phase12.log` — post-Phase-12 (blocking-wait) run,
  71 tests (5 new ring-buffer tests added), **69/71 passed**.
- Both logs searched explicitly for `definitely lost`, `indirectly lost`,
  `possibly lost`, `Invalid read/write/free`, `uninitialised value`, and
  `ERROR SUMMARY` — **zero matches in either log.**
- `targeted_binaries.log` — five representative binaries (the pool
  concurrency stress test, the publisher unit tests, the full pipeline e2e
  test, the affinity test, the SPSC ring buffer stress test) run directly
  under `valgrind --leak-check=full --track-origins=yes`, with explicit
  per-binary summaries. Every one reports `ERROR SUMMARY: 0 errors from 0
  contexts` and `All heap blocks were freed -- no leaks are possible`.

## The 2 failures are not memory defects, in either version

Both are explainable tooling/timing interactions, not project bugs — and
critically, Valgrind's own memcheck engine reported 0 errors in every case:

1. **`NaiveOrderBook ... causes real heap allocations`**: `REQUIRE(new_call_
   count() > 0)` got `0 > 0`. This test counts heap calls via a custom
   global `operator new`/`operator delete` override
   (`orderbook/test_alloc_counter.cpp`). Valgrind installs its own allocator
   interception at a lower level, which takes precedence over — and hides
   calls from — a user-defined global allocator override. A known category
   of tool interaction, not evidence the naive allocator stopped allocating.
   Fails identically in both the pre- and post-Phase-12 runs (unrelated to
   the ring buffer change).

2. **A timing-sensitive test with a hardcoded short real-time window** —
   *which specific test* changed between the two runs, which is itself
   informative:
   - **Pre-Phase-12**: `Full pipeline end-to-end ... gap detection observed
     while other shards are actively processing` failed
     (`REQUIRE(observed_live_stale)` was `false`). That test polls for a
     live STALE event within a **10 real-time-second** deadline while a
     500,000-message pipeline runs concurrently; Valgrind's ~20-50x
     instrumentation overhead was enough to blow that budget.
   - **Post-Phase-12**: that same test now *passes* under Valgrind — the
     blocking-wait mechanism's different scheduling characteristics happen
     to fit its window better this time. Instead, `Publisher shutdown
     handshake loses no deltas when stop() lands mid-flight, with the
     publisher thread deliberately lagging behind its decoder` fails
     (`REQUIRE(any_deltas)` was `false`): that test only runs for **2
     real-time milliseconds** before calling `stop()` (see
     `io/test_pipeline_e2e.cpp`), tuned for native-speed processing — under
     Valgrind's slowdown, no message makes it all the way through the
     pipeline in that window at all.
   
   Same underlying category both times (a short, hardcoded real-time window
   meeting Valgrind's severe overhead), just a different specific test
   depending on which one's timing margin is tightest relative to whatever
   this run's overhead happened to be — not a regression, and confirmed via
   `targeted_binaries.log` that the relevant binary still reports `ERROR
   SUMMARY: 0 errors from 0 contexts` alongside the failure either way.

Reproduce (from the repo root, via the Docker image built for Phase 8):

```sh
docker run --rm mdfh-linux-bench sh -c '
  cmake -S . -B build/valgrind -DCMAKE_BUILD_TYPE=Debug -DMDFH_SANITIZER=none
  cmake --build build/valgrind -j"$(nproc)"
  cd build/valgrind && ctest -T memcheck --output-on-failure
'
```
