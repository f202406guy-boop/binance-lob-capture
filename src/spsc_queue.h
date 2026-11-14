#pragma once

#include <atomic>
#include <cstddef>
#include <vector>

// Single-producer single-consumer ring buffer. The producer fills a slot in
// place and publishes it, the consumer reads it in place and then releases it.
// Slots are reused, so strings inside them keep their capacity and the steady
// state does no heap allocation. head_ and tail_ live on separate cache lines
// so the two threads do not fight over one line.
template <typename T, std::size_t N>
class SpscQueue {
    static_assert(N >= 2 && (N & (N - 1)) == 0, "queue size must be a power of two");

public:
    SpscQueue() : slots_(N) {}
    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    // producer side
    T* begin_push() {
        std::size_t head = head_.load(std::memory_order_relaxed);
        if (head - cached_tail_ == N) {
            cached_tail_ = tail_.load(std::memory_order_acquire);
            if (head - cached_tail_ == N) return nullptr;
        }
        return &slots_[head & (N - 1)];
    }

    void end_push() {
        head_.store(head_.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }

    // consumer side
    T* front() {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == cached_head_) {
            cached_head_ = head_.load(std::memory_order_acquire);
            if (tail == cached_head_) return nullptr;
        }
        return &slots_[tail & (N - 1)];
    }

    void pop() {
        tail_.store(tail_.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }

private:
    std::vector<T> slots_;
    alignas(64) std::atomic<std::size_t> head_{0};
    std::size_t cached_tail_ = 0;
    alignas(64) std::atomic<std::size_t> tail_{0};
    std::size_t cached_head_ = 0;
};
