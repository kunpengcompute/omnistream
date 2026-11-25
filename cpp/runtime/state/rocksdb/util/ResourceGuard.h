/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#ifndef OMNISTREAM_RESOURCEGUARD_H
#define OMNISTREAM_RESOURCEGUARD_H

#include <atomic>
#include <condition_variable>
#include <mutex>

class ThreadInterrupt {
public:
    static void interrupt();
    static bool isInterrupted();
    static void clear();

private:
    static thread_local std::atomic<bool> interrupted_;
};

class ResourceGuard {
public:
    ResourceGuard() : leaseCount(0), closed(false) {};
    ~ResourceGuard()
    {
        close();
    }

    // 租约内部类
    class Lease {
    public:
        Lease(Lease&& other) noexcept : parent(other.parent), closed(other.closed.load())
        {
            other.closed = true; // 标记原租约已关闭
        };

        Lease& operator=(Lease&& other) = delete;

        ~Lease()
        {
            close();
        };
        void close()
        {
            // 原子操作确保只释放一次
            if (!closed.exchange(true)) {
                parent.releaseResource();
            }
        };

        // 允许ResourceGuard访问Lease的私有构造函数
        friend class ResourceGuard;

    private:
        ResourceGuard& parent;    // 指向所属的ResourceGuard
        std::atomic<bool> closed; // 租约是否已关闭

        // 私有构造函数，只能通过ResourceGuard::acquireResource创建
        explicit Lease(ResourceGuard& guard) : parent(guard), closed(false) {};
    };

    // 获取资源租约（可能抛出异常）
    Lease* acquireResource()
    {
        std::unique_lock<std::mutex> lk(mtx);
        if (closed) {
            throw std::exception(); // 已关闭则抛出异常
        }
        leaseCount++; // 增加租约计数
        return new Lease(*this);
    };
    // 释放资源（内部使用）
    void releaseResource()
    {
        std::unique_lock<std::mutex> lk(mtx);
        if (--leaseCount == 0 && closed) {
            cv.notify_all(); // 所有租约释放且已关闭，通知等待线程
        }
    };
    // 可中断的关闭
    void closeInterruptibly()
    {
        std::unique_lock<std::mutex> lk(mtx);
        closed = true;
        // 等待所有租约释放
        while (leaseCount > 0) {
            cv.wait(lk);
        }
    };
    // 不可中断的关闭
    void closeUninterruptibly()
    {
        std::unique_lock<std::mutex> lk(mtx);
        closed = true;

        while (leaseCount > 0) {
            if (ThreadInterrupt::isInterrupted()) {
                ThreadInterrupt::clear();
            }
            cv.wait(lk, [this]() {
                // 退出等待的条件：租约数为 0 或线程被中断
                return leaseCount == 0 || ThreadInterrupt::isInterrupted();
            });
        }

        if (ThreadInterrupt::isInterrupted()) {
            ThreadInterrupt::clear();
        }
    };
    // 实现AutoCloseable接口
    void close()
    {
        closeUninterruptibly();
    };
    // 判断是否已关闭
    bool isClosed() const
    {
        return closed;
    };
    // 获取当前租约数量
    int getLeaseCount() const
    {
        return leaseCount;
    };

private:
    std::atomic<int> leaseCount;
    std::atomic<bool> closed;
    std::mutex mtx;
    std::condition_variable cv;
};

#endif // OMNISTREAM_RESOURCEGUARD_H
