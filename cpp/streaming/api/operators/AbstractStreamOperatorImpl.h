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

#include "AbstractStreamOperator.h"

#include "StreamingRuntimeContext.h"
#include "streaming/runtime/tasks/omni/OmniStreamTask.h"
#include "table/typeutils/RowDataSerializer.h"

template <typename K>
AbstractStreamOperator<K>::~AbstractStreamOperator()
{
    LOG("AbstractStreamOperator::~AbstractStreamOperator()");
    // delete output; ?
    delete stateHandler;
    delete runtimeContext;
    delete combinedWatermark;
}

template <typename K>
void AbstractStreamOperator<K>::setup()
{
    LOG("AbstractStreamOperator::setup()" << "new StreamingRuntimeContext<K>");
    this->runtimeContext = new StreamingRuntimeContext<K>(nullptr, nullptr);
    constexpr int inputsCount = 2;
    combinedWatermark = new omnistream::IndexedCombinedWatermarkStatus(inputsCount);
    // Flink intialize stateKeySelector here
}

template <typename K>
void AbstractStreamOperator<K>::setup(std::shared_ptr<omnistream::OmniStreamTask> task)
{
    this->setup();
    if (task != nullptr) {
        this->metrics = task->env()->taskMetricGroup();
    }
    // Flink intialize stateKeySelector here
}

template <typename K>
void AbstractStreamOperator<K>::setCurrentKey(K key)
{
    stateHandler->setCurrentKey(key);
}

template <typename K>
K AbstractStreamOperator<K>::getCurrentKey()
{
    return stateHandler->getCurrentKey();
}

template <typename K>
void AbstractStreamOperator<K>::close()
{
    if (stateHandler != nullptr) {
        stateHandler->dispose();
    }
}

template <typename K>
TypeSerializer* AbstractStreamOperator<K>::GetOperatorKeySerializer()
{
    return new BinaryRowDataSerializer(1);
}

template <typename K>
void AbstractStreamOperator<K>::initializeState(
    StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer)
{
    LOG("abstractStreamOperator::initializeState");
    auto operatorID = this->GetOperatorID();
    StreamOperatorStateContextImpl<K>* context = initializer->streamOperatorStateContext<K>(
        keySerializer, this, processingTimeService, &operatorID, flinkSavepointAdaptorInfo_, desc, GetOpName());
    stateHandler = new StreamOperatorStateHandler<K>(context);
    auto stateStore = stateHandler->getKeyedStateStore();
    if (runtimeContext != nullptr) {
        runtimeContext->setKeyedStateStore(stateStore);
        runtimeContext->setEnvironment(initializer->getEnvironment());
    }
    timeServiceManager = context->getInternalTimeServiceManager();
    stateHandler->initializeOperatorState(this);
}

template <typename K>
AbstractKeyedStateBackend<K>* AbstractStreamOperator<K>::getKeyedStateBackend() const
{
    return stateHandler->getKeyedStateBackend();
}

template <typename K>
OperatorStateBackend* AbstractStreamOperator<K>::getOperatorStateBackend()
{
    return stateHandler->getOperatorStateBackend();
}

template <typename K>
OperatorSnapshotFutures* AbstractStreamOperator<K>::SnapshotState(
    long checkpointId,
    long timestamp,
    CheckpointOptions* checkpointOptions,
    std::shared_ptr<CheckpointStreamFactory> storageLocation,
    const std::shared_ptr<OmniTaskBridge>& bridge)
{
    return stateHandler->SnapshotState(
        this,
        timeServiceManager,
        GetOpName(),
        checkpointId,
        timestamp,
        checkpointOptions,
        storageLocation,
        false,
        bridge);
}

template <typename K>
void AbstractStreamOperator<K>::notifyCheckpointComplete(long checkpointId)
{
    stateHandler->notifyCheckpointComplete(checkpointId);
}

template <typename K>
void AbstractStreamOperator<K>::notifyCheckpointAborted(long checkpointId)
{
    stateHandler->notifyCheckpointAborted(checkpointId);
}
