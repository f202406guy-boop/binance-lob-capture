// Wait strategy benchmark: how should the book thread wait for new messages?
//
// A producer thread pushes a timestamp into the same SpscQueue the program
// uses, once every 10 ms (roughly how often a live symbol sends something).
// The consumer records how long each one sat in the queue before it was seen.
// Three ways for the consumer to wait when the queue is empty:
//   sleep    - yield 64 times, then sleep 200 us (default in main.cpp)
//   spin     - keep checking and never sleep (--busy-poll)
//   condvar  - block on a condition variable that the producer signals
// It also prints how much CPU the process used while waiting.
//
// usage: wait_bench [messages]     default 500, about 5 s per strategy

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <thread>
#include <vector>

#include "spsc_queue.h"
#include "types.h"

namespace {

enum class Mode { Sleep, Spin, CondVar };

const char* mode_name(Mode m) {
    if (m == Mode::Sleep) return "sleep";
    if (m == Mode::Spin) return "spin";
    return "condvar";
}

void run(Mode mode, int messages) {
    SpscQueue<std::int64_t, 1024> queue;
    std::mutex mu;
    std::condition_variable cv;
    std::vector<std::int64_t> waits;
    waits.reserve(messages);

    std::clock_t cpu_start = std::clock();
    auto wall_start = std::chrono::steady_clock::now();

    std::thread consumer([&] {
        int idle = 0;
        while (static_cast<int>(waits.size()) < messages) {
            std::int64_t* sent = queue.front();
            if (sent) {
                waits.push_back(mono_clock_ns() - *sent);
                queue.pop();
                idle = 0;
                continue;
            }
            if (mode == Mode::Sleep) {
                if (++idle < 64)
                    std::this_thread::yield();
                else
                    std::this_thread::sleep_for(std::chrono::microseconds(200));
            } else if (mode == Mode::CondVar) {
                std::unique_lock<std::mutex> lock(mu);
                cv.wait_for(lock, std::chrono::milliseconds(50), [&] { return queue.front() != nullptr; });
            }
        }
    });

    for (int i = 0; i < messages; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::int64_t* slot = queue.begin_push();
        if (!slot) continue;
        *slot = mono_clock_ns();
        queue.end_push();
        if (mode == Mode::CondVar) {
            { std::lock_guard<std::mutex> lock(mu); }
            cv.notify_one();
        }
    }
    consumer.join();

    double wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - wall_start).count();
    double cpu = static_cast<double>(std::clock() - cpu_start) / CLOCKS_PER_SEC;

    std::sort(waits.begin(), waits.end());
    auto pct = [&](double p) {
        std::size_t i = static_cast<std::size_t>(p / 100.0 * static_cast<double>(waits.size() - 1));
        return static_cast<double>(waits[i]) / 1000.0;
    };
    std::printf("%-8s p50 %8.1f us   p99 %8.1f us   max %8.1f us   cpu %5.1f%% of one core\n", mode_name(mode),
                pct(50), pct(99), static_cast<double>(waits.back()) / 1000.0, 100.0 * cpu / wall);
}

}  // namespace

int main(int argc, char** argv) {
    int messages = argc > 1 ? std::atoi(argv[1]) : 500;
    if (messages < 10) messages = 10;
    std::printf("%d messages, one every 10 ms\n", messages);
    run(Mode::Sleep, messages);
    run(Mode::Spin, messages);
    run(Mode::CondVar, messages);
    return 0;
}
