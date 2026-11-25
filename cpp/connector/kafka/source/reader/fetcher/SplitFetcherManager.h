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

#ifndef FLINK_TNEL_SPLITFETCHERMANAGER_H
#define FLINK_TNEL_SPLITFETCHERMANAGER_H

#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <stdexcept>
#include <chrono>
#include "connector/kafka/source/reader/RecordsWithSplitIds.h"
#include "connector/kafka/source/reader/synchronization/FutureCompletingBlockingQueue.h"
#include "core/api/connector/source/SourceSplit.h"
#include "SplitFetcher.h"

// todo concurrentHashmap 用map和锁来替换

template <typename E, typename SplitT>
class SplitFetcherManager {
public:
    SplitFetcherManager(
        FutureCompletingBlockingQueue<E>* elementsQueue, std::function<SplitReader<E, SplitT>*()>& splitReaderFactory)
        : elementsQueue(elementsQueue),
          splitReaderFactory(splitReaderFactory)
    {
        errorHandler = [this, elementsQueue](const std::exception_ptr& e) {
            try {
                if (e) {
                    std::rethrow_exception(e);
                }
            } catch (const std::exception& ex) {
                std::cerr << ": Received uncaught exception: " << ex.what() << std::endl;
            }
            // 异常信息保存
            std::lock_guard<std::mutex> lock(exceptionMutex);
            if (!uncaughtFetcherException) {
                uncaughtFetcherException = e;
            } else {
                // 这里模拟 Java 的 addSuppressed 比较复杂，暂不详细实现
            }
            elementsQueue->notifyAvailable();
        };
    }

    virtual ~SplitFetcherManager()
    {
        close(0);
        for (auto& t : executorThreads) {
            if (t.joinable()) {
                t.join();
            }
        }
        // Queued messages can retain broker references. Release them before
        // destroying the consumers, whose destructors join broker threads.
        while (auto* records = elementsQueue->poll()) {
            delete records;
        }
        fetchers.clear();
        ownedFetchers.clear();
    }

    // 抽象方法，派生类需要实现
    virtual void addSplits(std::vector<SplitT*>& splitsToAdd) = 0;

    // 启动一个 fetcher
    void startFetcher(SplitFetcher<E, SplitT>* fetcher)
    {
        executorThreads.emplace_back([fetcher]() { fetcher->run(); });
    }

    // 创建一个新的 split fetcher
    SplitFetcher<E, SplitT>* createSplitFetcher()
    {
        std::lock_guard<std::mutex> lock(fetchersMutex);
        if (closed.load()) {
            throw std::runtime_error("The split fetcher manager has closed.");
        }
        SplitReader<E, SplitT>* splitReader = splitReaderFactory();
        int fetcherId = fetcherIdGenerator.fetch_add(1);
        auto fetcher =
            new SplitFetcher<E, SplitT>(fetcherId, elementsQueue, splitReader, errorHandler, [this, fetcherId]() {
                std::lock_guard<std::mutex> lock(fetchersMutex);
                fetchers.erase(fetcherId);
                elementsQueue->notifyAvailable();
            });
        // The worker only unregisters itself. Keep its consumer alive until
        // all workers have stopped and their outstanding messages are freed.
        ownedFetchers.emplace_back(fetcher);
        fetchers[fetcherId] = fetcher;
        return fetcher;
    }

    // 检查并关闭已经完成工作的 fetchers
    bool maybeShutdownFinishedFetchers()
    {
        std::lock_guard<std::mutex> lock(fetchersMutex);
        auto it = fetchers.begin();
        while (it != fetchers.end()) {
            auto& fetcher = it->second;
            if (fetcher->isIdle()) {
                LOG(": Closing splitFetcher " + std::to_string(it->first) + " because it is idle.");
                fetcher->shutdown();
                it = fetchers.erase(it);
            } else {
                ++it;
            }
        }
        return fetchers.empty();
    }

    // 关闭 split fetcher manager
    void close(long timeoutMs)
    {
        std::lock_guard<std::mutex> lock(fetchersMutex);
        closed = true;
        for (const auto& fetcher : fetchers) {
            fetcher.second->shutdown();
        }
    }

    void checkErrors()
    {
        if (!uncaughtFetcherException) {
            return;
        }
        try {
            std::rethrow_exception(uncaughtFetcherException);
        } catch (const std::exception& ex) {
            throw std::runtime_error("One or more fetchers have encountered exception: " + std::string(ex.what()));
        }
    }

    // 测试用，获取存活的 fetcher 数量
    int getNumAliveFetchers()
    {
        std::lock_guard<std::mutex> lock(fetchersMutex);
        return static_cast<int>(fetchers.size());
    }

protected:
    std::map<int, SplitFetcher<E, SplitT>*> fetchers;

private:
    std::function<void(const std::exception_ptr&)> errorHandler;
    std::atomic<int> fetcherIdGenerator{0};
    FutureCompletingBlockingQueue<E>* elementsQueue;
    std::function<SplitReader<E, SplitT>*()> splitReaderFactory;
    std::exception_ptr uncaughtFetcherException;
    std::mutex exceptionMutex;
    std::vector<std::thread> executorThreads;
    std::vector<std::unique_ptr<SplitFetcher<E, SplitT>>> ownedFetchers;
    std::atomic<bool> closed{false};
    std::mutex fetchersMutex;
};

#endif // FLINK_TNEL_SPLITFETCHERMANAGER_H
