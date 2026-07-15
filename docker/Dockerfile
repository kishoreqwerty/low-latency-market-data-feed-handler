# Phase 8/10: minimal Linux container for genuine pthread_setaffinity_np
# hard-pinning evidence (contrasted against the macOS advisory-hint finding
# in affinity/thread_affinity.hpp) and for Linux-only profiling tools this
# project needs (perf, Valgrind -- neither has real Apple Silicon macOS
# support). Not meant as a deployment image -- just enough toolchain to
# configure/build/run this project's existing CMake presets, plus perf
# record/report + flame graphs + Valgrind, unchanged from how they'd run
# on bare-metal Linux.
#
# Build (from the repo root):
#   docker build -f docker/Dockerfile -t mdfh-linux-bench .
# Run the pinning benchmark with the container's full CPU allotment
# visible (see thread_affinity_linux.cpp: it hard-pins within whatever
# sched_getaffinity() reports, so an artificially small --cpus here would
# just mean fewer distinct cores to pin to, not a broken benchmark):
#   docker run --rm --cpus="$(sysctl -n hw.ncpu)" mdfh-linux-bench
# perf needs elevated permissions Docker doesn't grant by default -- see
# docker/README.md for the exact `docker run --cap-add ... perf record`
# invocation and what actually worked vs. didn't in this environment.
# Or drop into a shell to run the full test suite / other binaries:
#   docker run --rm -it --entrypoint bash mdfh-linux-bench

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# g++ on 24.04 defaults to GCC 13, which has the C++23 support this
# project needs (std::expected, coroutines) -- the same reason libc++'s
# stricter allocator rebind requirements were already accounted for in
# orderbook/object_pool.hpp, so building against libstdc++ here needs no
# source changes.
#
# Retries/timeouts bumped: ports.ubuntu.com (the arm64 mirror) has shown
# real, transient per-connection timeouts on some of the larger packages
# below (valgrind, linux-tools-*, perl) during this project's own builds
# -- not a permanent block, just flaky enough on individual large-file
# transfers to need apt to retry rather than fail the whole layer.
RUN apt-get update -o Acquire::Retries=8 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends -o Acquire::Retries=8 -o Acquire::http::Timeout=60 \
        build-essential \
        cmake \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Split into its own layer/RUN so Docker's build cache keeps the (already
# reliable) toolchain above even if THIS layer needs a retry.
#
# linux-tools-generic/-common: perf. May or may not actually work -- perf
# talks to the KERNEL's perf_events subsystem, and a container shares the
# HOST kernel rather than running its own, so the apt-installed perf
# binary (built for a generic Ubuntu kernel) can mismatch Docker Desktop's
# actual (LinuxKit) kernel. Installed regardless and the real outcome
# reported honestly in docker/README.md, not assumed.
# valgrind: Linux-only, no meaningful Apple Silicon macOS build exists.
# perl: FlameGraph's stackcollapse-perf.pl/flamegraph.pl are Perl scripts.
RUN apt-get update -o Acquire::Retries=8 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends -o Acquire::Retries=8 -o Acquire::http::Timeout=60 \
        linux-tools-common \
        linux-tools-generic \
        valgrind \
        perl \
    && rm -rf /var/lib/apt/lists/*

# Brendan Gregg's FlameGraph scripts -- stackcollapse-perf.pl turns
# `perf script` output into the folded-stack format flamegraph.pl renders
# as an interactive SVG.
RUN git clone --depth 1 https://github.com/brendangregg/FlameGraph.git /opt/FlameGraph

ENV CC=gcc CXX=g++

WORKDIR /workspace
COPY . /workspace

RUN cmake --preset benchmark && cmake --build build/benchmark -j"$(nproc)"

CMD ["./build/benchmark/bench/mdfh_pinning_bench"]
