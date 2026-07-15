#pragma once

// Coroutine-based async network I/O (build_guide.md Phase 7). Since the
// "network" here is a simulated in-memory feed (see feed_generator.hpp),
// there's no real OS-level non-blocking read to wrap -- what this
// demonstrates is the actual C++20 coroutine mechanism real async I/O
// libraries build on: a coroutine suspends instead of blocking its thread,
// and something external (here: Reactor; in a real system: epoll/io_uring
// completion, a NIC interrupt) resumes it later.
//
// Reactor is deliberately single-threaded: the SAME thread that starts a
// coroutine also drives its resumes (see run_until()), so there is no
// thread-hopping and no synchronization needed inside Reactor itself. With
// only one simulated feed source, there's no OTHER work for that thread to
// interleave while "waiting" -- so this doesn't showcase multiplexing many
// concurrent I/O sources on one thread the way a real event loop would.
// The mechanism (custom awaiter, genuine suspend/resume, external
// scheduling) is real and correct; the concurrency payoff of coroutines
// specifically would only show up with multiple simultaneous sources,
// which is out of scope for a single simulated multicast feed.

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <queue>
#include <thread>
#include <vector>

#include "io/feed_generator.hpp"
#include "io/shard_demux.hpp"

namespace mdfh::io {

// Fire-and-forget coroutine task. initial_suspend/final_suspend are both
// suspend_never: calling a Task-returning coroutine function runs it
// eagerly (synchronously) up to its first real suspension point (a
// `co_await WaitFor{...}`), and the coroutine frame is destroyed
// automatically the moment the coroutine finishes -- there is no
// coroutine_handle retained anywhere that needs manual .destroy(), so
// there's nothing to leak as long as every suspended coroutine eventually
// gets resumed at least once more (Reactor::run_until guarantees this,
// see below).
struct Task {
    struct promise_type {
        Task get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        [[noreturn]] void unhandled_exception() { std::terminate(); }
    };
};

class Reactor;

// Suspends the coroutine and schedules it to resume after `delay`. Always
// genuinely suspends (await_ready() is always false) so every packet -- even
// with a zero delay -- round-trips through the reactor's schedule/resume
// cycle, not just the ones with a "real" wait.
struct WaitFor {
    Reactor& reactor;
    std::chrono::microseconds delay;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> handle) const;
    void await_resume() const noexcept {}
};

// Single-threaded coroutine scheduler: schedule() records a (wake time,
// handle) pair; run_until() is meant to be called on the SAME thread that
// started the coroutine(s), and resumes each one at its scheduled time,
// looping until nothing is left pending.
class Reactor {
public:
    void schedule(std::chrono::steady_clock::time_point when, std::coroutine_handle<> handle) {
        pending_.push(ScheduledResume{when, handle});
    }

    // Runs until the pending queue drains. `should_stop` is polled before
    // each wait; once it returns true, remaining resumes are dispatched
    // immediately (not waited out at their scheduled time), so a
    // coroutine that cooperatively checks a stop flag at the top of its
    // loop gets a prompt chance to observe it and return -- letting its
    // frame be destroyed via normal completion instead of sitting
    // orphaned in this queue forever.
    template <typename ShouldStop>
    void run_until(ShouldStop should_stop) {
        while (!pending_.empty()) {
            ScheduledResume next = pending_.top();
            pending_.pop();

            if (!should_stop()) {
                const auto now = std::chrono::steady_clock::now();
                if (next.when > now) {
                    std::this_thread::sleep_until(next.when);
                }
            }
            next.handle.resume();
        }
    }

private:
    struct ScheduledResume {
        std::chrono::steady_clock::time_point when;
        std::coroutine_handle<> handle;

        // std::priority_queue is a max-heap; we want the earliest wake
        // time first, so this orders "later" as "less".
        bool operator<(const ScheduledResume& other) const { return when > other.when; }
    };

    std::priority_queue<ScheduledResume> pending_;
};

inline void WaitFor::await_suspend(std::coroutine_handle<> handle) const {
    reactor.schedule(std::chrono::steady_clock::now() + delay, handle);
}

// The actual "network I/O" coroutine: pulls packets from `feed` one at a
// time, demux-ing each successfully-received one into `pipeline`, and
// co_await-ing a small simulated inter-packet delay between reads via
// `reactor` (a genuine suspend/resume cycle every iteration, not a busy
// loop). Stops -- setting producer_done, so shard decoder threads know no
// more messages are coming -- when the feed is exhausted or stop_requested
// becomes true. Meant to be called once, on the thread that will also call
// reactor.run_until(...) to drive its resumes (see pipeline_runner.hpp).
Task read_packets_async(FeedGenerator& feed, ShardedPipeline& pipeline, Reactor& reactor,
                         const std::atomic<bool>& stop_requested, std::atomic<bool>& producer_done);

}  // namespace mdfh::io
