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

#pragma once

#include <string>
#include <exception>
#include <memory>
#include <mutex>
#include <sstream>
#include "core/utils/function/ThrowingRunnable.h"

namespace omnistream {
class StreamTaskActionExecutor {
public:
    virtual ~StreamTaskActionExecutor() = default;

    virtual void run(ThrowingRunnable* const runnable) = 0;

    virtual std::string toString() const
    {
        return "StreamTaskActionExecutor";
    }

    static std::shared_ptr<StreamTaskActionExecutor> IMMEDIATE;
};

class ImmediateStreamTaskActionExecutor : public StreamTaskActionExecutor {
public:
    ImmediateStreamTaskActionExecutor() = default;
    ~ImmediateStreamTaskActionExecutor() override = default;

    void run(ThrowingRunnable* const runnable) override
    {
        runnable->Run();
    }

    std::string toString() const override
    {
        return "ImmediateStreamTaskActionExecutor";
    }
};

class SynchronizedStreamTaskActionExecutor : public StreamTaskActionExecutor {
public:
    SynchronizedStreamTaskActionExecutor() = default;

    explicit SynchronizedStreamTaskActionExecutor(std::recursive_mutex* mutex) : mutex_(mutex)
    {
    }
    ~SynchronizedStreamTaskActionExecutor() override = default;

    static std::shared_ptr<SynchronizedStreamTaskActionExecutor> synchronizedExecutor()
    {
        return std::make_shared<SynchronizedStreamTaskActionExecutor>();
    }

    static std::shared_ptr<SynchronizedStreamTaskActionExecutor> synchronizedExecutor(std::recursive_mutex* mutex)
    {
        return std::make_shared<SynchronizedStreamTaskActionExecutor>(mutex);
    }

    void run(ThrowingRunnable* runnable) override
    {
        std::lock_guard<std::recursive_mutex> lock(*mutex_);
        runnable->Run();
    }

    std::recursive_mutex* getMutex()
    {
        return mutex_;
    }

    std::string toString() const override
    {
        std::stringstream ss;
        ss << "SynchronizedStreamTaskActionExecutor";
        return ss.str();
    }

private:
    std::recursive_mutex* mutex_;
};
} // namespace omnistream
