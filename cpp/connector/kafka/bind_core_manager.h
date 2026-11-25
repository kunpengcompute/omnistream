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

#ifndef OMNISTREAM_BINDCOREMANAGER_H
#define OMNISTREAM_BINDCOREMANAGER_H

#include <unistd.h>
#include <thread>
#include <vector>
#include <mutex>
#include <sched.h>
#include <cstring>
#include <sstream>
#include <iostream>
#include <string>
#include <algorithm>
#include <cerrno>
#include "common.h"

namespace omnistream {
class BindStrategy {
public:
    virtual int32_t GetSourceCore(int32_t subtaskIndex) = 0;

    virtual int32_t GetSinkCore(int32_t subtaskIndex) = 0;

    virtual int32_t GetOpCore(int32_t subtaskIndex) = 0;

    virtual bool NeedBindSource() = 0;

    virtual bool NeedBindSink() = 0;

    virtual bool NeedBindOp() = 0;
};

class NoNeedBind : public BindStrategy {
public:
    bool NeedBindSource() override
    {
        return false;
    }

    bool NeedBindSink() override
    {
        return false;
    }

    bool NeedBindOp() override
    {
        return false;
    }

    int32_t GetOpCore(int32_t subtaskIndex) override
    {
        throw std::out_of_range("not implement GetOpCore in NoNeedBind Strategy ");
    }

    int32_t GetSourceCore(int32_t subtaskIndex) override
    {
        throw std::out_of_range("not implement GetSourceCore in NoNeedBind Strategy");
    }

    int32_t GetSinkCore(int32_t subtaskIndex) override
    {
        throw std::out_of_range("not implement GetSinkCore in NoNeedBind Strategy");
    }
};

enum class BindCoreStrategy {
    NONE,
    ALL_IN_ONE,
    ONLY_SOURCE_SINK
};

class BindCoreManager {
#define BIND_CORE_PER_PARA 2
public:
    static std::unique_ptr<BindCoreManager>& GetInstance()
    {
        static auto instance = std::make_unique<BindCoreManager>();
        instance->Init();
        return instance;
    }

    void SetBindStrategy(BindCoreStrategy strategy);

    /*
     * return coreId = -1 means no empty core to bind
     */
    int32_t GetUnBindCore(int operatorIndex)
    {
        int32_t coreId = -1;
        int coreIndex = -1;
        auto totalPara = operatorIndex * BIND_CORE_PER_PARA;
        auto curStart = totalPara % static_cast<int32_t>(availableCores.size());
        {
            std::lock_guard<std::mutex> lock(coreMutex);
            if (!availableCores.empty()) {
                auto it = bindedCores.begin() + curStart;
                it = std::find(it, bindedCores.end(), false);
                if (it == bindedCores.end()) {
                    // no unbinded core : start from begin
                    it = std::find(bindedCores.begin(), bindedCores.end(), false);
                    if (it == bindedCores.end()) {
                        // still no unbinded core : reset all cores
                        std::fill(bindedCores.begin(), bindedCores.end(), false);
                    }
                    it = bindedCores.begin();
                }
                coreIndex = std::distance(bindedCores.begin(), it);
                coreId = availableCores[coreIndex];
                *it = true; // 标记为已占用
            }
        }
        return coreId;
    }

    int32_t BindThread2core(int operatorIndex)
    {
        int32_t coreId = -1;
        int coreIndex = -1;
        auto totalPara = operatorIndex * BIND_CORE_PER_PARA;
        auto curStart = totalPara % static_cast<int32_t>(availableCores.size());
        {
            std::lock_guard<std::mutex> lock(coreMutex);
            if (!availableCores.empty()) {
                auto it = bindedCores.begin() + curStart;
                it = std::find(it, bindedCores.end(), false);
                if (it == bindedCores.end()) {
                    // no unbinded core : start from begin
                    it = std::find(bindedCores.begin(), bindedCores.end(), false);
                    if (it == bindedCores.end()) {
                        // still no unbinded core : reset all cores
                        std::fill(bindedCores.begin(), bindedCores.end(), false);
                    }
                    it = bindedCores.begin();
                }
                coreIndex = std::distance(bindedCores.begin(), it);
                coreId = availableCores[coreIndex];
                *it = true; // 标记为已占用
            }

            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(static_cast<size_t>(coreId), &cpuset);

            if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
                std::stringstream ss;
                ss << "Thread " << operatorIndex << ": Failed to bind to core " << coreId << " (" << strerror(errno)
                   << ")" << std::endl;
                availableCores[coreIndex] = true;
            }
        }
        return coreId;
    }

    void Init()
    {
        std::call_once(initFlag, [this] {
            this->InitAvailableCores(); // Lambda 捕获 this，调用成员函数
        });
    }

    int32_t Bind2Core(int coreId)
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(static_cast<size_t>(coreId), &cpuset);

        auto err = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (err != EOK) {
            std::stringstream ss;
            ss << "Thread " << coreId << ": Failed to bind to core " << coreId << " (" << strerror(err) << ")"
               << std::endl;
            INFO_RELEASE(ss.str());
        }
        return coreId;
    }

    int32_t BindSimpleCore(int index)
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        int coreId = 0;
        int coreOffset = index;
        if (sched_getaffinity(getpid(), sizeof(cpu_set_t), &cpuset) != -1) {
            for (int i = 0; i < CPU_SETSIZE; i++) {
                if (CPU_ISSET(static_cast<size_t>(i), &cpuset)) {
                    --coreOffset;
                    if (coreOffset == 0) {
                        coreId = i;
                        break;
                    }
                }
            }
            CPU_ZERO(&cpuset);
            CPU_SET(static_cast<size_t>(coreId), &cpuset);
            auto err = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
            if (err != 0) {
                INFO_RELEASE("Failed to bind thread to core " << coreId << " with reason:" << strerror(err));
            }
        }
        return coreId;
    }

    int32_t BindDirectCore(int index)
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        int coreId = index;

        if (sched_getaffinity(getpid(), sizeof(cpu_set_t), &cpuset) != -1) {
            CPU_ZERO(&cpuset);
            CPU_SET(static_cast<size_t>(coreId), &cpuset);
            auto err = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
            if (err != EOK) {
                INFO_RELEASE("Failed to bind thread to core " << coreId << " with reason:" << strerror(err));
            }
        }
        return coreId;
    }

    int32_t GetSourceCore(int32_t subtaskIndex)
    {
        return bindStrategy->GetSourceCore(subtaskIndex);
    }

    int32_t GetSinkCore(int32_t subtaskIndex)
    {
        return bindStrategy->GetSinkCore(subtaskIndex);
    }

    int32_t GetOpCore(int32_t subtaskIndex)
    {
        return bindStrategy->GetOpCore(subtaskIndex);
    }

    bool NeedBindSource()
    {
        return bindStrategy->NeedBindSource();
    }

    bool NeedBindSink()
    {
        return bindStrategy->NeedBindSink();
    }

    bool NeedBindOp()
    {
        return bindStrategy->NeedBindOp();
    }

    std::once_flag initFlag;

private:
    std::mutex coreMutex;
    std::vector<int> availableCores;
    std::vector<int8_t> bindedCores;
    std::unique_ptr<BindStrategy> bindStrategy = std::make_unique<NoNeedBind>();
    std::atomic<BindCoreStrategy> bindCoreStrategy = BindCoreStrategy::NONE;

    void InitAvailableCores()
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        if (sched_getaffinity(getpid(), sizeof(cpu_set_t), &cpuset) == -1) {
            perror("sched_getaffinity");
            throw std::out_of_range("sched_getaffinity.");
        }

        // Do not bind to the same physical core.
        int step = 2;
        for (int i = 0; i < CPU_SETSIZE; i += step) {
            if (CPU_ISSET(static_cast<size_t>(i), &cpuset)) {
                availableCores.push_back(i);
            }
        }

        if (availableCores.empty()) {
            LOG("No available cores for binding.");
            throw std::out_of_range("No available cores for binding.");
        }

        bindedCores.resize(availableCores.size(), false);
    }
};

class NeedBind : public BindStrategy {
public:
    bool NeedBindSource() override
    {
        return true;
    }

    bool NeedBindSink() override
    {
        return true;
    }

    bool NeedBindOp() override
    {
        return true;
    }

protected:
    std::mutex paraMute;
};

class AllInOneBind : public NeedBind {
public:
    int32_t GetSourceCore(int32_t subtaskIndex) override
    {
        return GetCore(subtaskIndex);
    }

    int32_t GetSinkCore(int32_t subtaskIndex) override
    {
        return GetCore(subtaskIndex);
    }

    int32_t GetOpCore(int32_t subtaskIndex) override
    {
        return GetCore(subtaskIndex);
    }

private:
    std::unordered_map<int, int> coreMap;

    int32_t GetCore(int32_t subtaskIndex)
    {
        std::lock_guard<std::mutex> lck(paraMute);
        auto coreId = 0;
        if (coreMap.find(subtaskIndex) == coreMap.end()) {
            auto getCoreId = BindCoreManager::GetInstance()->GetUnBindCore(subtaskIndex);
            coreMap[subtaskIndex] = getCoreId;
            coreId = getCoreId;
        } else {
            coreId = coreMap[subtaskIndex];
        }
        return coreId;
    }
};

class OnlySourceSinkBind : public NeedBind {
public:
    int32_t GetSourceCore(int32_t subtaskIndex) override
    {
        std::lock_guard<std::mutex> lck(paraMute);
        if (sourceCoreId == -1) {
            sourceCoreId = BindCoreManager::GetInstance()->GetUnBindCore(subtaskIndex);
        }
        return sourceCoreId;
    }

    int32_t GetSinkCore(int32_t subtaskIndex) override
    {
        std::lock_guard<std::mutex> lck(paraMute);
        if (sinkCoreId == -1) {
            sinkCoreId = BindCoreManager::GetInstance()->GetUnBindCore(subtaskIndex);
        }
        return sinkCoreId;
    }

    int32_t GetOpCore(int32_t subtaskIndex) override
    {
        throw std::out_of_range("not implement GetOpCore in OnlySourceSinkBind");
    }

    bool NeedBindOp() override
    {
        return false;
    }

private:
    int32_t sourceCoreId = -1;
    int32_t sinkCoreId = -1;
};

inline void BindCoreManager::SetBindStrategy(BindCoreStrategy strategy)
{
    auto expectd = bindCoreStrategy.load();
    if (expectd != strategy) {
        while (not bindCoreStrategy.compare_exchange_strong(expectd, strategy)) {
            expectd = bindCoreStrategy.load();
        }
        if (expectd != strategy) {
            switch (strategy) {
                case BindCoreStrategy::NONE: {
                    bindStrategy = std::make_unique<NoNeedBind>();
                    break;
                }
                case BindCoreStrategy::ALL_IN_ONE: {
                    bindStrategy = std::make_unique<AllInOneBind>();
                    break;
                }
                case BindCoreStrategy::ONLY_SOURCE_SINK: {
                    bindStrategy = std::make_unique<OnlySourceSinkBind>();
                    break;
                }

                default: {
                    throw std::out_of_range("unsupported strategy");
                }
            }
        }
    }
}
} // namespace omnistream
#endif // OMNISTREAM_BINDCOREMANAGER_H
