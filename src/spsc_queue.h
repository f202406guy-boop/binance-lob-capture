// This is the lock-free ring between the two threads. The network thread is the only producer
// and the book thread is the only consumer, so single-producer single-consumer is all I need,
// and it means no mutex or condition variable on the per-message path. The producer gets a
// slot with begin_push(), fills it in place and publishes it with end_push(). The consumer
// reads it in place with front() and releases it with pop(). Slots are reused, so the strings
// inside keep their capacity and the steady state does no heap allocation. The one technical
// bit: head_ and tail_ are atomics with acquire/release ordering on separate 64-byte cache
// lines, so the two threads don't fight over one line, and each side keeps a cached copy of
// the other side's index and only reloads it when the ring looks full or empty. N has to be
// a power of two so the slot index is just a mask. If the ring is full, begin_push() gives
// nullptr and the network thread waits instead of dropping a message (see ws_client.cpp).

#pragma once

#include <atomic>
#include <cstddef>
#include <vector>

template <typename T, std::size_t N>
class SpscQueue {
    static_assert(N >= 2 && (N & (N - 1)) == 0, "queue size must be a power of two");

public:
    SpscQueue() : slots_(N) {}
    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

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
