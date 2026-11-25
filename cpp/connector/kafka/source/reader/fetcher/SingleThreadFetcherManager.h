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

#ifndef FLINK_TNEL_SINGLETHREADFETCHERMANAGER_H
#define FLINK_TNEL_SINGLETHREADFETCHERMANAGER_H

#include <vector>
#include <memory>
#include <functional>
#include <queue>
#include <map>
#include "SplitFetcherManager.h"
#include "core/include/common.h"

template <typename E, typename SplitT>
class SingleThreadFetcherManager : public SplitFetcherManager<E, SplitT> {
public:
    SingleThreadFetcherManager(
        FutureCompletingBlockingQueue<E>* elementsQueue, std::function<SplitReader<E, SplitT>*()>& splitReaderSupplier)
        : SplitFetcherManager<E, SplitT>(elementsQueue, splitReaderSupplier)
    {
    }

    void addSplits(std::vector<SplitT*>& splitsToAdd) override
    {
        auto fetcher = getRunningFetcher();
        if (fetcher == nullptr) {
            fetcher = this->createSplitFetcher();
            // Add the splits to the fetchers.
            fetcher->addSplits(splitsToAdd);
            this->startFetcher(fetcher);
        } else {
            fetcher->addSplits(splitsToAdd);
        }
    }

protected:
    SplitFetcher<E, SplitT>* getRunningFetcher()
    {
        return this->getNumAliveFetchers() == 0 ? nullptr : this->fetchers.begin()->second;
    }
};

#endif // FLINK_TNEL_SINGLETHREADFETCHERMANAGER_H
