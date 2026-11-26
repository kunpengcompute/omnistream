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

#ifndef FLINK_TNEL_FETCHTASK_H
#define FLINK_TNEL_FETCHTASK_H

#include <iostream>
#include <memory>
#include <functional>
#include <vector>
#include <atomic>
#include <stdexcept>
#include <thread>
#include <map>
#include "common.h"
#include "connector/kafka/source/reader/RecordsWithSplitIds.h"
#include "connector/kafka/source/reader/synchronization/FutureCompletingBlockingQueue.h"
#include "core/api/connector/source/SourceSplit.h"
#include "connector/kafka/source/reader/KafkaPartitionSplitReader.h"
#include "SplitFetcherTask.h"
#include "connector/kafka/bind_core_manager.h"

template <typename E, typename SplitT>
class FetchTask : public SplitFetcherTask {
public:
    FetchTask(
        SplitReader<E, SplitT>* splitReader,
        FutureCompletingBlockingQueue<E>* elementsQueue,
        std::function<void(const std::set<std::string>&)> splitFinishedCallback,
        int fetcherIndex)
        : splitReader(splitReader),
          elementsQueue(elementsQueue),
          splitFinishedCallback(splitFinishedCallback),
          fetcherIndex(fetcherIndex),
          lastRecords(nullptr)
    {
    }

    ~FetchTask() override
    {
        delete lastRecords;
    }

    bool Run() override
    {
        if (unlikely(not binded)) {
            coreId = elementsQueue->getCoreId();
            if (coreId >= 0) {
                omnistream::BindCoreManager::GetInstance()->BindDirectCore(coreId);
            }
            binded = true;
        }
        try {
            fetch();
            // 异常类全局重写
        } catch (const std::exception& e) {
            throw std::runtime_error("Source fetch execution was interrupted: " + std::string(e.what()));
        }
        // finally 是否需要关注
        if (IsWakenUp()) {
            wakeup.store(false);
        }
        return true;
    }

    void fetch()
    {
        if (!IsWakenUp() && !lastRecords) {
            lastRecords = splitReader->fetch();
        }
        if (!IsWakenUp()) {
            // 此处原本会无条件调用 elementsQueue->put, 但是为了防止 lastRecords->finishedSplits
            // 解引用空指针同时不影响原本语义 , 并且节约一次 finishedSplits 初始化, 此处会判断 lastRecords 是否为
            // nullptr 再重复调用 elementsQueue->put
            if (lastRecords == nullptr) {
                WARN_RELEASE("FetchTask::fetch() : lastRecords is nullptr");
                elementsQueue->put(fetcherIndex, lastRecords);
            } else {
                // copy the finishedSplits
                const auto finishedSplits = lastRecords->finishedSplits();
                if (elementsQueue->put(fetcherIndex, lastRecords)) {
                    lastRecords = nullptr;
                    if (!finishedSplits.empty()) {
                        splitFinishedCallback(finishedSplits);
                    }
                }
            }
        }
    }

    void WakeUp() override
    {
        wakeup.store(true);
        if (!lastRecords) {
            splitReader->wakeUp();
        } else {
            elementsQueue->wakeUpPuttingThread(fetcherIndex);
        }
    }

    bool IsWakenUp() const
    {
        return wakeup.load();
    }

    std::string ToString() override
    {
        return "FetchTask";
    }

private:
    SplitReader<E, SplitT>* splitReader;
    FutureCompletingBlockingQueue<E>* elementsQueue;
    const std::function<void(const std::set<std::string>&)> splitFinishedCallback;
    const int fetcherIndex;
    RecordsWithSplitIds<E>* lastRecords;
    std::atomic<bool> wakeup{false};
    int32_t coreId = -1;
    bool binded = false;
};

#endif // FLINK_TNEL_FETCHTASK_H
