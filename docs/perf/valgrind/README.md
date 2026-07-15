# Valgrind memcheck results

Run inside the Linux Docker container (`docker/Dockerfile`) — Valgrind has no
meaningful Apple Silicon macOS support. Built with `-DCMAKE_BUILD_TYPE=Debug
-DMDFH_SANITIZER=none` (Valgrind and ASan/UBSan instrumentation conflict, so
this is a separate, sanitizer-free debug build, not the `debug` CMake preset).

## Result: zero memory defects

- `full_suite_ctest_memcheck.log` — `ctest -T memcheck` across all 66 tests.
  **64/66 passed.** Searched explicitly for `definitely lost`, `indirectly
  lost`, `possibly lost`, `Invalid read/write/free`, `uninitialised value`,
  and `ERROR SUMMARY` — **zero matches anywhere in the log.**
- `targeted_binaries.log` — five representative binaries (the pool
  concurrency stress test, the publisher unit tests, the full pipeline e2e
  test, the affinity test, the SPSC ring buffer stress test) run directly
  under `valgrind --leak-check=full --track-origins=yes`, with explicit
  per-binary summaries. Every one reports `ERROR SUMMARY: 0 errors from 0
  contexts` and `All heap blocks were freed -- no leaks are possible`.

## The 2 failures are not memory defects

Both are explainable tooling/timing interactions, not project bugs — and
critically, valgrind's own memcheck engine reported 0 errors in both:

1. **`NaiveOrderBook ... causes real heap allocations` (test #20)**:
   `REQUIRE(new_call_count() > 0)` got `0 > 0`. This test counts heap calls
   via a custom global `operator new`/`operator delete` override
   (`orderbook/test_alloc_counter.cpp`). Valgrind installs its own allocator
   interception at a lower level, which takes precedence over — and hides
   calls from — a user-defined global allocator override. This is a known
   category of tool interaction, not evidence the naive allocator stopped
   allocating.
2. **`Full pipeline end-to-end ... gap detection observed while other
   shards are actively processing` (test #59)**: `REQUIRE(observed_live_stale)`
   was `false`. This test polls for a live STALE event within a **10
   real-time-second** deadline while a 500,000-message pipeline runs
   concurrently (see `io/test_pipeline_e2e.cpp`'s own comment documenting
   this exact class of timing sensitivity). Valgrind's instrumentation
   overhead (commonly 20-50x for heavily-threaded code) is enough to throw
   off that real-time budget in this run. Confirmed independent of memory
   correctness: `targeted_binaries.log`'s run of the same binary shows the
   identical single-test failure with `ERROR SUMMARY: 0 errors from 0
   contexts` alongside it.

Reproduce (from the repo root, via the Docker image built for Phase 8):

```sh
docker run --rm mdfh-linux-bench sh -c '
  cmake -S . -B build/valgrind -DCMAKE_BUILD_TYPE=Debug -DMDFH_SANITIZER=none
  cmake --build build/valgrind -j"$(nproc)"
  cd build/valgrind && ctest -T memcheck --output-on-failure
'
```
