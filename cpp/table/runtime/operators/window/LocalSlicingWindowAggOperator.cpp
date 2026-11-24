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

#include "LocalSlicingWindowAggOperator.h"
#include "runtime/generated/function/AverageFunction.h"
#include "runtime/generated/function/CountDistinctFunction.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "runtime/generated/function/CountFunction.h"
#include "runtime/generated/function/MinMaxFunction.h"
#include "runtime/generated/function/SumFunction.h"
#include "table/runtime/generated/function/EmptyNamespaceFunction.h"
#include <iostream>
#include "table/data/util/RowDataUtil.h"
#include "streaming/api/operators/TimestampedCollector.h"

void LocalSlicingWindowAggOperator::open()
{
    aggregateCallsCount = description["aggInfoList"]["aggregateCalls"].size();
    if (aggregateCallsCount == 0) {
        AggsHandleFunction* function = new EmptyNamespaceFunction();
        functions.push_back(function);
        aggregateCallsCount = 1;
        return;
    }

    accTypes = description["aggInfoList"]["accTypes"].get<std::vector<std::string>>();
    aggValueTypes = description["aggInfoList"]["aggValueTypes"].get<std::vector<std::string>>();
    accumulatorArity = accTypes.size();
}

void LocalSlicingWindowAggOperator::processBatch(StreamRecord* input)
{
    auto record = std::unique_ptr<StreamRecord>(input);
    auto batch = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());

    if (!batch) {
        return;
    }
    auto rowCount = batch->GetRowCount();
    if (!batch || rowCount < 0) {
        return;
    }

    std::vector<int64_t> sliceEndArr(rowCount);
    for (int64_t i = 0; i < batch->GetRowCount(); i++) {
        sliceEndArr[i] = sliceAssigner->assignSliceEnd(batch, i, clock);
    }

    windowBuffer->addVectorBatch(std::move(batch), sliceEndArr);
}

void LocalSlicingWindowAggOperator::ProcessWatermark(Watermark* mark)
{
    LOG("LocalSlicingWindowAggOperator::processWatermark start: " << mark->getTimestamp());
    if (mark->getTimestamp() > currentWatermark) {
        currentWatermark = mark->getTimestamp();
        if (currentWatermark >= nextTriggerWatermark) {
            windowBuffer->advanceProgress(currentWatermark);
            nextTriggerWatermark = getNextTriggerWatermark(currentWatermark, windowInterval);
        }
    }
    LOG("LocalSlicingWindowAggOperator::processWatermark end: " << mark->getTimestamp());
    if (timeServiceManager != nullptr) {
        timeServiceManager->advanceWatermark(mark);
    }
    output->emitWatermark(mark);
}

void LocalSlicingWindowAggOperator::PrepareSnapshotPreBarrier(long checkpointId)
{
    windowBuffer->flush();
}

Output* LocalSlicingWindowAggOperator::getOutput()
{
    return this->output;
}

void LocalSlicingWindowAggOperator::close()
{
    for (auto func : functions) {
        delete func;
    }
    delete clock;
    AbstractStreamOperator<long>::close();
}

const char* LocalSlicingWindowAggOperator::getName()
{
    return "LocalWindowAggOperator";
}

std::string LocalSlicingWindowAggOperator::getTypeName()
{
    std::string typeName = "LocalWindowAggOperator";
    typeName.append(__PRETTY_FUNCTION__);
    return typeName;
}
