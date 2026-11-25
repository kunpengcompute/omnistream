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

#include "MergingWindowProcessFunction.h"

template <typename K, typename W>
void MergingWindowProcessFunction<K, W>::open(Context<K, W>* ctx)
{
    InternalWindowProcessFunction<K, W>::open(ctx);

    // init windowState
    auto* mapStateDescriptor =
        new MapStateDescriptor<W, W>("session-window-mapping", windowSerializer, windowSerializer);
    MapState<W, W>* mapState =
        dynamic_cast<typename WindowOperator<K, W>::WindowContext*>(ctx)->getPartitionedState(mapStateDescriptor);
    mergingWindows = std::make_unique<MergingWindowSet<K, W>>(windowAssigner, mapState);
}

template <typename K, typename W>
std::vector<W> MergingWindowProcessFunction<K, W>::assignStateNamespace(RowData* inputRow, int64_t timestamp)
{
    std::vector<W> elementWindows = windowAssigner->assignWindows(inputRow, timestamp);
    mergingWindows->initializeCache(this->ctx->currentKey());
    reuseActualWindows.clear();

    auto MergingFunction = [this](
                               W& mergeResult,
                               std::unordered_set<W>& mergedWindows,
                               W& stateWindowResult,
                               std::vector<W>& stateWindowsToBeMerged) {
        int64_t mergeResultMaxTs = mergeResult.maxTimestamp();
        if (!windowAssigner->isEventTime() && mergeResultMaxTs <= this->ctx->currentProcessingTime()) {
            throw std::runtime_error(
                "The end timestamp of a processing-time window cannot become earlier than the "
                "current processing time ");
        }

        this->ctx->onMerge(mergeResult, stateWindowsToBeMerged);

        // clear registered timers
        for (const auto& m : mergedWindows) {
            this->ctx->clearTrigger(m);
            this->ctx->deleteCleanupTimer(m);
        }

        // merge the merged state windows into the newly resulting state window
        if (!stateWindowsToBeMerged.empty()) {
            RowData* targetAcc = this->ctx->getWindowAccumulators(stateWindowResult);
            if (targetAcc == nullptr) {
                targetAcc = this->windowAggregator->createAccumulators();
            }
            this->windowAggregator->setAccumulators(stateWindowResult, targetAcc);
            for (const auto& w : stateWindowsToBeMerged) {
                RowData* acc = this->ctx->getWindowAccumulators(w);
                if (acc != nullptr) {
                    this->windowAggregator->merge(w, acc);
                }
                // clear merged window
                this->ctx->clearWindowState(w);
                this->ctx->clearPreviousState(w);
            }
            targetAcc = this->windowAggregator->getAccumulators();
            this->ctx->setWindowAccumulators(stateWindowResult, targetAcc);
        }
    };

    for (const auto& window : elementWindows) {
        // adding the new window might result in a merge, in that case the actualWindow
        // is the merged window and we work with that. If we don't merge then
        // actualWindow == window
        W actualWindow = mergingWindows->addWindow(window, MergingFunction);
        // drop if the window is already late, not for now
        if (this->isWindowLate(actualWindow)) {
            mergingWindows->retireWindow(actualWindow);
        } else {
            reuseActualWindows.push_back(actualWindow);
        }
    }

    std::vector<W> affectedWindows;
    affectedWindows.reserve(reuseActualWindows.size());
    for (const auto& actual : reuseActualWindows) {
        affectedWindows.push_back(mergingWindows->getStateWindow(actual));
    }
    return affectedWindows;
}

template <typename K, typename W>
std::vector<W> MergingWindowProcessFunction<K, W>::assignActualWindows(RowData* inputRow, int64_t timestamp)
{
    return reuseActualWindows;
}

template <typename K, typename W>
void MergingWindowProcessFunction<K, W>::prepareAggregateAccumulatorForEmit(const W& window)
{
    W stateWindow = mergingWindows->getStateWindow(window);
    RowData* acc = this->ctx->getWindowAccumulators(stateWindow);
    if (acc == nullptr) {
        acc = InternalWindowProcessFunction<K, W>::windowAggregator->createAccumulators();
    }
    InternalWindowProcessFunction<K, W>::windowAggregator->setAccumulators(stateWindow, acc);
}

template <typename K, typename W>
void MergingWindowProcessFunction<K, W>::cleanWindowIfNeeded(const W& window, int64_t currentTime)
{
    if (this->isCleanupTime(window, currentTime)) {
        this->ctx->clearTrigger(window);
        W stateWindow = mergingWindows->getStateWindow(window);
        this->ctx->clearWindowState(stateWindow);
        // retire expired window
        mergingWindows->initializeCache(this->ctx->currentKey());
        mergingWindows->retireWindow(window);
        // do not need to clear previous state, previous state is disabled in session window
    }
}

template class MergingWindowProcessFunction<shared_ptr<RowData>, TimeWindow>;
