# Phase 12 "after" artifacts

Captured the same way as Phase 10's (`docs/perf/README.md`), after replacing
`io/pipeline_runner.cpp`'s `sleep_for(50us)` poll with
`concurrency::SpscRingBuffer::wait_for_data()`/`notify_waiters()`
(`std::atomic::wait`/`notify`, C++20).

- `macos_latency_after.txt` / `linux_latency_after.txt` — `bench/mdfh_latency_bench`
  stdout, both platforms, same workload as the Phase 10 baseline (8 shards,
  1,000,000 messages, 0.05% packet loss).
- `macos_sample_after.txt` — native macOS `/usr/bin/sample` capture. No
  `nanosleep` anywhere in the profile (confirms the poll is really gone);
  dominated by real business logic (`OrderBookT::apply`, `insert_resting_order`,
  `ShardedPipeline::process_shard`) instead.
- `linux_perf_report_after.txt` — Linux `perf report` flat self-time ranking.
  `clock_nanosleep` is gone too, but replaced by meaningful time in
  `__sched_yield` and `std::__atomic_wait_address_v` -- see
  `docs/BENCHMARKS.md`'s Phase 12 section for what that means (glibc's
  `atomic::wait` spin-yields for a period before actually blocking, unlike
  macOS's implementation, which is why the Linux speedup is smaller).

See `docs/BENCHMARKS.md` for the full before/after comparison and analysis.
