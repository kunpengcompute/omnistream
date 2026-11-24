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

#include "WindowOperator.h"
#include "assigners/MergingWindowAssigner.h"
#include "assigners/PanedWindowAssigner.h"
#include "data/util/RowDataUtil.h"
#include "internal/GeneralWindowProcessFunction.h"
#include "internal/MergingWindowProcessFunction.h"
#include "internal/PanedWindowProcessFunction.h"
#include "table/utils/TimeWindowUtil.h"

template <typename K, typename W>
WindowOperator<K, W>::~WindowOperator()
{
    WindowOperator<K, W>::close();
}

template <typename K, typename W>
void WindowOperator<K, W>::open()
{
    internalTimerService =
        AbstractStreamOperator<K>::getInternalTimerService("window-timers", windowSerializer_.get(), this);
    triggerContext_ = std::make_unique<TriggerContext>(this);
    triggerContext_->open();

    // init windowState
    omnistream::RowType accRowType(true, accTypes_);
    accSerializer_ = std::make_unique<RowDataSerializer>(&accRowType);
    std::string aggName = "window-aggs";
    auto* valueStateDescriptor = new ValueStateDescriptor<RowData*>(aggName, accSerializer_.get());
    using S = InternalValueState<K, W, RowData*>;
    auto keyedStateBackend = this->stateHandler->getKeyedStateBackend();

    if (dynamic_cast<RocksdbKeyedStateBackend<K>*>(keyedStateBackend) != nullptr) {
        backendType_ = omnistream::StateType::ROCKSDB;
    } else if (dynamic_cast<HeapKeyedStateBackend<K>*>(keyedStateBackend) != nullptr) {
        backendType_ = omnistream::StateType::HEAP;
    } else {
        THROW_LOGIC_EXCEPTION("The keyedStateBackend is not supported");
    }

    S* state = keyedStateBackend->template getOrCreateKeyedState<TimeWindow, S, RowData*>(
        windowSerializer_.get(), valueStateDescriptor);
    windowState = state;

    if (dynamic_cast<MergingWindowAssigner<W>*>(windowAssigner.get())) {
        this->windowFunction = std::make_unique<MergingWindowProcessFunction<K, W>>(
            static_cast<MergingWindowAssigner<W>*>(windowAssigner.get()),
            windowAggregator.get(),
            new TimeWindow::Serializer(),
            allowedLateness);
    } else if (dynamic_cast<PanedWindowAssigner<W>*>(windowAssigner.get())) {
        this->windowFunction = std::make_unique<PanedWindowProcessFunction<K, W>>(
            static_cast<PanedWindowAssigner<W>*>(windowAssigner.get()), windowAggregator.get(), allowedLateness);
    } else {
        this->windowFunction = std::make_unique<GeneralWindowProcessFunction<K, W>>(
            windowAssigner.get(), windowAggregator.get(), allowedLateness);
    }
    windowFunction->open(new WindowContext(this));
}

template <typename K, typename W>
void WindowOperator<K, W>::close()
{
    AbstractStreamOperator<K>::close();
    INFO_RELEASE("WindowOperator closed");
}

template <typename K, typename W>
int64_t WindowOperator<K, W>::cleanupTime(const W& window)
{
    if (windowAssigner->isEventTime()) {
        return TimeWindowUtil::toCleanupTimerMills(window.maxTimestamp(), allowedLateness, shiftTimeZone);
    }
    return std::max<int64_t>(0, window.maxTimestamp());
}

template <typename K, typename W>
void WindowOperator<K, W>::registerCleanupTimer(const W& window)
{
    int64_t cleanupTime = this->cleanupTime(window);
    if (cleanupTime != INT64_MAX) {
        if (windowAssigner->isEventTime()) {
            triggerContext_->registerEventTimeTimer(cleanupTime);
        } else {
            triggerContext_->registerProcessingTimeTimer(cleanupTime);
        }
    }
}
template <typename K, typename W>
class AggregateWindowOperator;

template <typename K, typename W>
void WindowOperator<K, W>::onEventTime(TimerHeapInternalTimer<K, W>* timer)
{
    this->setCurrentKey(timer->getKey());

    TimeWindow window = timer->getNamespace();
    triggerContext_->window = window;

    if (triggerContext_->onEventTime(timer->getTimestamp())) {
        emitWindowResult(triggerContext_->window);
    }
    if (windowAssigner->isEventTime()) {
        windowFunction->cleanWindowIfNeeded(triggerContext_->window, timer->getTimestamp());
    }
}

template <typename K, typename W>
void WindowOperator<K, W>::onProcessingTime(TimerHeapInternalTimer<K, W>* timer)
{
    NOT_IMPL_EXCEPTION;
}

template <typename K, typename W>
void WindowOperator<K, W>::processWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    this->output->emitWatermarkStatus(watermarkStatus);
}

template <typename K, typename W>
void WindowOperator<K, W>::processBatch(StreamRecord* input)
{
    auto record = std::unique_ptr<StreamRecord>(input);
    auto batch =
        std::unique_ptr<omnistream::VectorBatch>(reinterpret_cast<omnistream::VectorBatch*>(record->getValue()));
    auto rowCount = batch->GetRowCount();
    if (rowCount <= 0) {
        return;
    }
    for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto currentRow = std::unique_ptr<RowData>(batch->extractRowData(rowIndex));
        auto currentRowKey = this->keySelector_->getKey(batch.get(), rowIndex, false);
        this->stateHandler->setCurrentKey(currentRowKey);
        this->processElement(currentRow.get());
    }
}

template <typename K, typename W>
void WindowOperator<K, W>::processElement(RowData* inputRow)
{
    int64_t timestamp = 0;
    if (windowAssigner->isEventTime()) {
        timestamp = *inputRow->getLong(rowtimeIndex);
    } else {
        THROW_LOGIC_EXCEPTION("Processing time window is not supported yet!");
    }
    timestamp = TimeWindowUtil::toUtcTimestampMills(timestamp, shiftTimeZone);

    // the windows which the input row should be placed into
    std::vector<W> affectedWindows = windowFunction->assignStateNamespace(inputRow, timestamp);
    for (const auto& window : affectedWindows) {
        windowState->setCurrentNamespace(window);
        auto acc = reinterpret_cast<RowData*>(windowState->value());
        if (acc == nullptr) {
            acc = windowAggregator->createAccumulators();
        }
        windowAggregator->setAccumulators(window, acc);

        if (RowDataUtil::isAccumulateMsg(inputRow->getRowKind())) {
            windowAggregator->accumulate(inputRow);
        } else {
            windowAggregator->retract(inputRow);
        }
        acc = windowAggregator->getAccumulators();
        windowState->update(acc);
        if (shouldDeleteWindowStateValue()) {
            delete acc;
        }
    }

    // the actual window which the input row is belongs to
    const auto& actualWindows = windowFunction->assignActualWindows(inputRow, timestamp);
    for (const auto& window : actualWindows) {
        triggerContext_->window = window;
        bool triggerResult = triggerContext_->onElement(inputRow, timestamp);
        if (triggerResult) {
            emitWindowResult(window);
        }
        // register a clean up timer for the window
        registerCleanupTimer(window);
    }
}

template class WindowOperator<std::shared_ptr<RowData>, TimeWindow>;
