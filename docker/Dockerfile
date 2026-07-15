# Phase 8: minimal Linux container to get genuine pthread_setaffinity_np
# hard-pinning evidence, contrasted against the macOS advisory-hint finding
# documented in affinity/thread_affinity.hpp. Not meant as a deployment
# image -- just enough toolchain to configure/build/run this project's
# existing CMake presets unchanged.
#
# Build (from the repo root):
#   docker build -f docker/Dockerfile -t mdfh-linux-bench .
# Run the pinning benchmark with the container's full CPU allotment
# visible (see thread_affinity_linux.cpp: it hard-pins within whatever
# sched_getaffinity() reports, so an artificially small --cpus here would
# just mean fewer distinct cores to pin to, not a broken benchmark):
#   docker run --rm --cpus="$(sysctl -n hw.ncpu)" mdfh-linux-bench
# Or drop into a shell to run the full test suite / other binaries:
#   docker run --rm -it --entrypoint bash mdfh-linux-bench

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# g++ on 24.04 defaults to GCC 13, which has the C++23 support this
# project needs (std::expected, coroutines) -- the same reason libc++'s
# stricter allocator rebind requirements were already accounted for in
# orderbook/object_pool.hpp, so building against libstdc++ here needs no
# source changes.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV CC=gcc CXX=g++

WORKDIR /workspace
COPY . /workspace

RUN cmake --preset benchmark && cmake --build build/benchmark -j"$(nproc)"

CMD ["./build/benchmark/bench/mdfh_pinning_bench"]
