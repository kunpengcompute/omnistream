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

#include "GeneralWindowProcessFunction.h"

#include <memory>

#include "table/runtime/operators/window/TimeWindow.h"

template <typename K, typename W>
std::vector<W> GeneralWindowProcessFunction<K, W>::assignStateNamespace(RowData* inputRow, int64_t timestamp)
{
    std::vector<W> elementWindows = windowAssigner->assignWindows(inputRow, timestamp);
    reuseAffectedWindows.clear();
    for (const auto& window : elementWindows) {
        if (!this->isWindowLate(window)) {
            reuseAffectedWindows.push_back(window);
        }
    }
    return reuseAffectedWindows;
}

template <typename K, typename W>
std::vector<W> GeneralWindowProcessFunction<K, W>::assignActualWindows(RowData* inputRow, int64_t timestamp)
{
    return reuseAffectedWindows;
}

template <typename K, typename W>
void GeneralWindowProcessFunction<K, W>::prepareAggregateAccumulatorForEmit(const W& window)
{
    RowData* acc = this->ctx->getWindowAccumulators(window);
    if (acc == nullptr) {
        acc = this->windowAggregator->createAccumulators();
    }
    this->windowAggregator->setAccumulators(window, acc);
}

template <typename K, typename W>
void GeneralWindowProcessFunction<K, W>::cleanWindowIfNeeded(const W& window, int64_t currentTime)
{
    if (this->isCleanupTime(window, currentTime)) {
        this->ctx->clearWindowState(window);
        this->ctx->clearPreviousState(window);
        this->ctx->clearTrigger(window);
    }
}

template class GeneralWindowProcessFunction<std::shared_ptr<RowData>, TimeWindow>;
