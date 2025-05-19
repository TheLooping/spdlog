#pragma once

#include "ring_buffer.h"
#include <atomic>
#include <condition_variable>
#include <mutex>

namespace spdlog {
namespace details {

template <typename T>
class mpmc_blocking_queue {
public:
    using item_type = T;
    explicit mpmc_blocking_queue(size_t max_items)
        : q_(max_items) {}

    // try to enqueue and block if no room left
    void enqueue(T &&item) {
        q_.push(std::move(item));
    }

    // enqueue immediately. overrun oldest message in the queue if no room left.
    void enqueue_nowait(T &&item) {
        q_.push_nowait(std::move(item));
    }

    void enqueue_if_have_room(T &&item) {
        if (q_.push_if_have_room(std::move(item)) <= 0) {
            discard_counter_.fetch_add(1);
        }
    }

    // dequeue with a timeout.
    // Return true, if succeeded dequeue item, false otherwise
    bool dequeue_for(T &popped_item, std::chrono::milliseconds wait_duration) {
        if (q_.pop_for(popped_item, wait_duration)){
            return true;
        }
        return false;
    }

    // blocking dequeue without a timeout.
    void dequeue(T &popped_item) {
        q_.pop(popped_item);
    }


    size_t overrun_counter() {
        return q_.overrun_counter();
    }

    size_t discard_counter() { return discard_counter_.load(); }

    size_t size() {
        return q_.size();
    }

    void reset_overrun_counter() {
        q_.reset_overrun_counter();
    }

    void reset_discard_counter() { discard_counter_.store(0, std::memory_order_relaxed); }

private:
    ring_buf_::ring_buffer<T> q_;
    std::atomic<size_t> discard_counter_{0};
};
}  // namespace details
}  // namespace spdlog
