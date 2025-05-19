#pragma once

#include <vector>
#include <atomic>
#include <cstddef>
#include <memory>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <thread>
#include <cstdlib>

#define RING_BUF_SUCCESS 0
#define RING_BUF_PUSH_ERROR_FULL -1
#define RING_BUF_PUSH_ERROR -2

#define RING_BUF_POP_ERROR_EMPTY -3
#define RING_BUF_POP_ERROR -4


namespace ring_buf_
{
    static constexpr size_t CACHELINE_SIZE = 64;
    

    template <typename T>
    class alignas(CACHELINE_SIZE) ring_buffer
    {
    public:
        explicit ring_buffer(size_t capacity);
        ring_buffer(const ring_buffer &) = delete;
        ring_buffer &operator=(const ring_buffer &) = delete;

        void reset_overrun_counter();
        size_t overrun_counter();
        size_t size();
        size_t room();
        bool full();
        bool empty();
        int push_nowait(T &&push_item);
        int push_if_have_room(T &&push_item);
        int push(T &&push_item);

        int pop_nowait(T &popped_item);
        int pop(T &popped_item);
        bool pop_for(T &popped_item, std::chrono::milliseconds wait_duration);


    private:
        alignas(CACHELINE_SIZE) std::atomic<size_t> capacity_;
        alignas(CACHELINE_SIZE) char pad2[CACHELINE_SIZE - sizeof(std::atomic<size_t>)];

        alignas(CACHELINE_SIZE) std::atomic<size_t> prod_head_;
        alignas(CACHELINE_SIZE) char pad3[CACHELINE_SIZE - sizeof(std::atomic<size_t>)];
        alignas(CACHELINE_SIZE) std::atomic<size_t> prod_tail_;
        alignas(CACHELINE_SIZE) char pad4[CACHELINE_SIZE - sizeof(std::atomic<size_t>)];
        alignas(CACHELINE_SIZE) std::atomic<size_t> cons_head_;
        alignas(CACHELINE_SIZE) char pad5[CACHELINE_SIZE - sizeof(std::atomic<size_t>)];
        

        alignas(CACHELINE_SIZE) std::unique_ptr<T[]> buffer_;
        alignas(CACHELINE_SIZE) char pad6[CACHELINE_SIZE - sizeof(std::atomic<size_t>)];

        alignas(CACHELINE_SIZE) std::atomic<size_t> overrun_counter_;
        alignas(CACHELINE_SIZE) char pad7[CACHELINE_SIZE - sizeof(std::atomic<size_t>)];

    };

    template <typename T>
    ring_buffer<T>::ring_buffer(size_t capacity)
    {
        capacity_ = capacity;
        prod_head_.store(0);
        prod_tail_.store(0);
        cons_head_.store(0);
        overrun_counter_.store(0);
        buffer_ = std::unique_ptr<T[]>(new T[capacity]);
    }
    template <typename T>
    bool ring_buffer<T>::full(){
        return room() == 1;
    }
    template <typename T>
    bool ring_buffer<T>::empty(){
        return (size() == 0)&&(room() == 0);
    }
    template <typename T>
    size_t ring_buffer<T>::size(){
        return prod_head_.load() >= cons_head_.load() ? (prod_head_.load() - cons_head_.load()) : (capacity_.load() + prod_head_.load() - cons_head_.load());
    }
    
    template <typename T>
    size_t ring_buffer<T>::room(){
        return cons_head_.load() >= prod_head_.load() ? (cons_head_.load() - prod_head_.load()) : (capacity_.load() + cons_head_.load() - prod_head_.load());
    }
    
    template <typename T>
    size_t ring_buffer<T>::overrun_counter(){
        return overrun_counter_.load();
    }
    template <typename T>
    void ring_buffer<T>::reset_overrun_counter(){
        overrun_counter_.store(0);
    }

    template <typename T>
    int ring_buffer<T>::push_nowait(T &&push_item)
    {
        while (push_if_have_room(std::move(push_item)) <= 0)
        {
            // 覆盖 cons_tail 到 cons_head 之间的元素
            size_t old_cons_head, new_cons_head;
            do
            {
                old_cons_head = cons_head_.load();
                new_cons_head = (old_cons_head + 1) % capacity_.load();
            } while (!cons_head_.compare_exchange_weak(old_cons_head, new_cons_head));
        }
        overrun_counter_.fetch_add(1);
        return 1;
    }

    template <typename T>
    int ring_buffer<T>::push_if_have_room(T &&push_item)
    {
        if (full())
        {
            return RING_BUF_PUSH_ERROR_FULL;
        }

        size_t old_prod_head, new_prod_head;
        do
        {
            old_prod_head = prod_head_.load();
            new_prod_head = (old_prod_head + 1) % capacity_.load();
        } while (!prod_head_.compare_exchange_weak(old_prod_head, new_prod_head));

        // 从 old_prod_head 处开始写入数据
        buffer_[old_prod_head] = std::move(push_item);

        size_t desire_prod_tail = (old_prod_head + 1) % capacity_.load();
        do
        {
            std::this_thread::yield();
        } while (!prod_tail_.compare_exchange_weak(old_prod_head, desire_prod_tail));

        return 1;
    }


    template <typename T>
    int ring_buffer<T>::push(T &&push_item)
    {
        while (full())
        {
            std::this_thread::yield();
        }

        size_t old_prod_head, new_prod_head;
        do
        {
            old_prod_head = prod_head_.load();
            new_prod_head = (old_prod_head + 1) % capacity_.load();
        } while (!prod_head_.compare_exchange_weak(old_prod_head, new_prod_head));

        // 从 old_prod_head 处开始写入数据
        buffer_[old_prod_head] = std::move(push_item);

        size_t desire_prod_tail = (old_prod_head + 1) % capacity_.load();
        do
        {
            std::this_thread::yield();
        } while (!prod_tail_.compare_exchange_weak(old_prod_head, desire_prod_tail));

        return 1;
    }


    template <typename T>
    int ring_buffer<T>::pop_nowait(T &popped_item)
    {
        size_t pop_max_size = prod_tail_.load() >= cons_head_.load() ? (prod_tail_.load() - cons_head_.load()) : (capacity_.load() + prod_tail_.load() - cons_head_.load());
        int fact_pop_size = std::min(pop_max_size, static_cast<size_t>(1));
        if (fact_pop_size <= 0)
        {
            return RING_BUF_POP_ERROR_EMPTY;
        }

        size_t old_cons_head, new_cons_head;
        do
        {
            old_cons_head = cons_head_.load();
            new_cons_head = (old_cons_head + fact_pop_size) % capacity_.load();
        } while (!cons_head_.compare_exchange_weak(old_cons_head, new_cons_head));

        size_t idx = (cons_head_.load() + capacity_.load() - 1) % capacity_.load();
        popped_item = std::move(buffer_[idx]);

        return fact_pop_size;
    }

    template <typename T>
    int ring_buffer<T>::pop(T &popped_item) {
        size_t pop_max_size;
        while ((pop_max_size = prod_tail_.load() >= cons_head_.load() ? (prod_tail_.load() - cons_head_.load()) : (capacity_.load() + prod_tail_.load() - cons_head_.load())) <= 0)
        {
            std::this_thread::yield();
        }
        int fact_pop_size = std::min(pop_max_size, static_cast<size_t>(1));
        size_t old_cons_head, new_cons_head;
        do
        {
            old_cons_head = cons_head_.load();
            new_cons_head = (old_cons_head + fact_pop_size) % capacity_.load();
        } while (!cons_head_.compare_exchange_weak(old_cons_head, new_cons_head));


        // 从 cons_head_ 处开始读取数据
        popped_item = std::move(buffer_[old_cons_head]);


        return fact_pop_size;
    }

    template <typename T>
    bool ring_buffer<T>::pop_for(T &popped_item, std::chrono::milliseconds wait_duration) {
        auto start_time = std::chrono::steady_clock::now();
        auto end_time = start_time + wait_duration;
        while (pop_nowait(popped_item) <= 0) {
            if (std::chrono::steady_clock::now() >= end_time) {
                return false;
            }
            std::this_thread::yield();
        }
        return true;
    }

}

