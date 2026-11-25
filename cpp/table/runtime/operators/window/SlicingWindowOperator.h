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

template <typename K, typename W>
class SlicingWindowProcessor;

#include <ctime>
#include <limits>
#include <string>

#include "nlohmann/json.hpp"
#include "streaming/api/watermark/Watermark.h"
#include "streaming/api/operators/TimerHeapInternalTimer.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/TableStreamOperator.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/Triggerable.h"
#include "core/api/common/state/ListState.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/runtime/operators/InternalTimerService.h"
#include "streaming/api/operators/Output.h"
#include "functions/RuntimeContext.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "runtime/state/DefaultOperatorStateBackend.h"
#include "runtime/state/StateInitializationContextImpl.h"
#include "runtime/state/StateSnapshotContextSynchronousImpl.h"
#include "core/api/common/state/ListStateDescriptor.h"

using namespace omniruntime::type;
using json = nlohmann::json;

template <typename K, typename W>
class SlicingWindowOperator : public TableStreamOperator<K>, public OneInputStreamOperator, public Triggerable<K, W> {
public:
    SlicingWindowOperator(std::unique_ptr<SlicingWindowProcessor<K, W>> windowProcessor, const nlohmann::json config);
    ~SlicingWindowOperator() override = default;
    void open() override;
    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        INFO_RELEASE(
            "SlicingWindowOperator initializeState with initializer, operatorID: "
            << AbstractStreamOperator<K>::GetOperatorID().toString());
        AbstractStreamOperator<K>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<K>::initializeState(initializer, keySerializer);
    };

    void initializeState(StateInitializationContextImpl* context) override
    {
        INFO_RELEASE("SlicingWindowOperator initializeState");
        AbstractStreamOperator<K>::initializeState(context);

        std::string watermarkStateName = "watermark";
        auto* watermarkStateDesc = new ListStateDescriptor<int64_t>(watermarkStateName, new LongSerializer());
        auto* stateBackend = static_cast<DefaultOperatorStateBackend*>(context->getOperatorStateBackend());
        auto rawState = stateBackend->getUnionListState<int64_t>(watermarkStateDesc);
        this->watermarkState = rawState;

        if (context->isRestored()) {
            auto* watermarks = this->watermarkState->get();
            if (watermarks != nullptr && !watermarks->empty()) {
                int64_t minWatermark = std::numeric_limits<int64_t>::max();
                for (int64_t wm : *watermarks) {
                    if (wm < minWatermark) {
                        minWatermark = wm;
                    }
                }
                if (minWatermark != std::numeric_limits<int64_t>::max()) {
                    this->currentWatermark = minWatermark;
                }
            }
        }
    };

    void snapshotState(StateSnapshotContextSynchronousImpl* context) override
    {
        INFO_RELEASE("SlicingWindowOperator snapshotState");
        TableStreamOperator<K>::snapshotState(context);
        if (this->watermarkState != nullptr) {
            this->watermarkState->clear();
            this->watermarkState->add(this->currentWatermark);
        }
    };

    void notifyCheckpointComplete(long checkpointId) override
    {
        AbstractStreamOperator<K>::notifyCheckpointComplete(checkpointId);
    }

    void notifyCheckpointAborted(long checkpointId) override
    {
        AbstractStreamOperator<K>::notifyCheckpointAborted(checkpointId);
    }

    void snapshotState() {};
    void close();
    void processBatch(omnistream::VectorBatch* batch);
    void processBatch(StreamRecord* record) override;
    void ProcessWatermark(Watermark* mark) override;
    void processElement(StreamRecord* element) override {};
    void onEventTime(TimerHeapInternalTimer<K, W>* timer) override;
    void onProcessingTime(TimerHeapInternalTimer<K, W>* timer) override;
    void PrepareSnapshotPreBarrier(long checkpointId) override;
    Output* getOutput();
    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }
    void onTimer(TimerHeapInternalTimer<K, W>* timer);

private:
    std::unique_ptr<SlicingWindowProcessor<K, W>> windowProcessor_ = nullptr;
    int64_t lastTriggeredProcessingTime = std::numeric_limits<int64_t>::min();
    std::shared_ptr<ListState<int64_t>> watermarkState;
    nlohmann::json description;
    InternalTimerServiceImpl<K, int64_t>* internalTimerService = nullptr;
    Output* output;
};

template <typename W>
class WindowProcessorContext {
public:
    omniruntime::mem::MemoryManager* getMemoryManager();
    int64_t getMemorySize();
    AbstractKeyedStateBackend<omnistream::VectorBatch>* getKeyedStateBackend();
    InternalTimerService<W>* getTimerService();
    void setBackend();

private:
    // operatorOwner is not used
    omniruntime::mem::MemoryManager* memoryManager;
    int64_t memorySize;
    InternalTimerService<W>* timerService;
    AbstractKeyedStateBackend<omnistream::VectorBatch>* keyedStateBackend;
    Output* collector;
    RuntimeContext* runtimeContext;
};

template <typename K, typename W>
SlicingWindowOperator<K, W>::SlicingWindowOperator(
    std::unique_ptr<SlicingWindowProcessor<K, W>> windowProcessor, const nlohmann::json config)
    : TableStreamOperator<K>(windowProcessor->getOutput()),
      windowProcessor_(std::move(windowProcessor)),
      description(config)
{
    this->output = windowProcessor_->getOutput();
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::open()
{
    TableStreamOperator<K>::open();
    lastTriggeredProcessingTime = std::numeric_limits<int64_t>::min();
    auto* runtimeCtx = AbstractStreamOperator<K>::getRuntimeContext();
    auto backState = this->stateHandler->getKeyedStateBackend();
    TypeSerializer* windowSerializer = windowProcessor_->createWindowSerializer();
    internalTimerService =
        AbstractStreamOperator<K>::template getInternalTimerService<int64_t>("window-timers", windowSerializer, this);
    windowProcessor_->open(backState, description, runtimeCtx, internalTimerService);
    windowProcessor_->initializeWatermark(this->currentWatermark);
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::close()
{
    windowProcessor_->close();
    AbstractStreamOperator<K>::close();
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::processBatch(StreamRecord* input)
{
    auto record = std::unique_ptr<StreamRecord>(input);
    auto batch =
        std::unique_ptr<omnistream::VectorBatch>(reinterpret_cast<omnistream::VectorBatch*>(record->getValue()));
    this->processBatch(batch.get());
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::processBatch(omnistream::VectorBatch* batch)
{
    windowProcessor_->processBatch(batch);
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::ProcessWatermark(Watermark* mark)
{
    if (mark->getTimestamp() > this->currentWatermark) {
        windowProcessor_->advanceProgress(mark->getTimestamp());
        TableStreamOperator<K>::ProcessWatermark(mark);
    } else {
        auto watermark = Watermark(this->currentWatermark);
        TableStreamOperator<K>::ProcessWatermark(&watermark);
    }
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::onEventTime(TimerHeapInternalTimer<K, W>* timer)
{
    onTimer(timer);
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::onProcessingTime(TimerHeapInternalTimer<K, W>* timer)
{
    if (timer->getTimestamp() > lastTriggeredProcessingTime) {
        lastTriggeredProcessingTime = timer->getTimestamp();
        windowProcessor_->advanceProgress(timer->getTimestamp());
    }
    onTimer(timer);
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::onTimer(TimerHeapInternalTimer<K, W>* timer)
{
    K windowKey = timer->getKey();
    this->stateHandler->setCurrentKey(windowKey);
    W window = timer->getNamespace();
    windowProcessor_->fireWindow(window);
    windowProcessor_->clearWindow(window);
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::PrepareSnapshotPreBarrier(long checkpointId)
{
    const std::string operatorId = OneInputStreamOperator::GetOperatorID().toString();
    LOG("SlicingWindowOperator pre-barrier preparation start: operatorId=" << operatorId
                                                                           << ", checkpointId=" << checkpointId);
    try {
        windowProcessor_->prepareCheckpoint();
        LOG("SlicingWindowOperator pre-barrier preparation complete: operatorId=" << operatorId
                                                                                  << ", checkpointId=" << checkpointId);
    } catch (const std::exception& exception) {
        ERROR_RELEASE(
            "SlicingWindowOperator pre-barrier preparation failed: operatorId="
            << operatorId << ", checkpointId=" << checkpointId << ", error=" << exception.what());
        throw;
    } catch (...) {
        ERROR_RELEASE(
            "SlicingWindowOperator pre-barrier preparation failed with unknown exception: operatorId="
            << operatorId << ", checkpointId=" << checkpointId);
        throw;
    }
}

template <typename W>
AbstractKeyedStateBackend<omnistream::VectorBatch>* WindowProcessorContext<W>::getKeyedStateBackend()
{
    return keyedStateBackend;
}

template <typename K, typename W>
Output* SlicingWindowOperator<K, W>::getOutput()
{
    return windowProcessor_->getOutput();
}
