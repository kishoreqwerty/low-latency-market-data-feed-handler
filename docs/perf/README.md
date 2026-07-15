# Phase 10 profiling artifacts

Generated inside the Linux Docker container from Phase 8 (`docker/Dockerfile`) —
perf has no meaningful Apple Silicon macOS equivalent, see `docker/README.md`.

- `flamegraph.svg` — `perf record -F 999 -g` against `bench/mdfh_latency_bench`
  (8 shards, 1,000,000 messages), rendered via Brendan Gregg's FlameGraph scripts.
  Interactive: click a frame to zoom, click text to search.
- `perf_report_flat.txt` — `perf report --stdio -g none`, flat self-time ranking.
- `perf_report_top.txt` — `perf report --stdio`, call-graph view (top of tree).
- `latency_bench_docker_stdout.txt` — the same benchmark run's own stage-boundary
  percentile output (p50/p99/p99.9), captured from inside the same container run
  as the flame graph above, for an apples-to-apples read against the CPU profile.
- `latency_report.html` — a single-page write-up combining the flame graph, the
  percentile table, and the hot-function breakdown with commentary. Also published
  as a Claude Artifact.
- `macos_sample_native.txt` — a genuine cross-check on the "is this a Docker
  virtualization artifact?" question: the same `mdfh_latency_bench` run,
  profiled natively (no container, no VM) via macOS's `/usr/bin/sample`
  CLI utility (`sample <pid> 10 1`). Independently shows the same
  `clock_gettime`/`nanosleep` dominance as the Linux perf profile, on real
  Apple Silicon hardware.

## Why `perf` needed a workaround

`/usr/bin/perf` (Ubuntu's `linux-tools-generic` package) refuses to run at all
inside the container: `WARNING: perf not found for kernel 6.12.76`. Docker
Desktop containers share the **host's** kernel (a LinuxKit build) rather than
running their own, so the apt-installed perf binary — built for a generic
Ubuntu kernel package, 6.8.0-134 — doesn't version-match and the wrapper
script refuses to try. The actual binary underneath,
`/usr/lib/linux-tools/6.8.0-134-generic/perf`, works fine when invoked
directly — the `perf_event_open()` ABI tolerated the version skew even though
the version-check wrapper wouldn't. Reproduce with:

```sh
docker run --rm --cap-add=SYS_ADMIN --cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
  mdfh-linux-bench sh -c '
    PERF=/usr/lib/linux-tools/6.8.0-134-generic/perf
    $PERF record -F 999 -g -o /tmp/perf.data -- ./build/benchmark/bench/mdfh_latency_bench
    $PERF script -i /tmp/perf.data > /tmp/perf.script.txt
    perl /opt/FlameGraph/stackcollapse-perf.pl /tmp/perf.script.txt > /tmp/perf.folded.txt
    perl /opt/FlameGraph/flamegraph.pl /tmp/perf.folded.txt > /tmp/flamegraph.svg
  '
```

The raw `perf.data` (~3MB binary, kernel-specific) isn't checked in — regenerate
it with the command above if you need to re-run `perf report`/`perf script`
with different options.
