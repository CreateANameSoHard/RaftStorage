#include <iostream>

#include "../include/ThreadPool.h"

ThreadPool::ThreadPool(size_t num_threads) {
    if (num_threads == 0) num_threads = 1;
    workers_.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back(&ThreadPool::workerLoop, this);
    }
}

ThreadPool::~ThreadPool() {
    stop();
}

void ThreadPool::stop() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true)) {
        return;  // 已经停止
    }

    // 唤醒所有等待的线程
    condition_.notify_all();

    // 等待所有工作线程退出
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    // 清空未执行的任务（可选）
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::queue<std::function<void()>> empty;
        std::swap(tasks_, empty);
    }
}

void ThreadPool::workerLoop() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            condition_.wait(lock, [this]() {
                return stopped_ || !tasks_.empty();
            });
            if (stopped_ && tasks_.empty()) {
                return;  // 停止且无任务，退出线程
            }
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        // 执行任务（在锁外执行）
        task();
    }
}