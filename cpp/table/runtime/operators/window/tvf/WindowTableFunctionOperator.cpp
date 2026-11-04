/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
 * See the Mulan PSL v2 for more details.
 */

#include "WindowTableFunctionOperator.h"

#include "table/runtime/operators/window/assigners/CumulativeWindowAssigner.h"
#include "table/runtime/operators/window/assigners/SlidingWindowAssigner.h"
#include "table/runtime/operators/window/assigners/TumblingWindowAssigner.h"
#include "table/types/logical/LogicalType.h"
#include "core/include/common.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"

using json = nlohmann::json;
using omniruntime::type::DataTypeId;

WindowTableFunctionOperator::WindowTableFunctionOperator(const nlohmann::json& config, Output* output)
    : AbstractStreamOperator(output),
      description_(config)
{
    inputTypeStrs_ = config["inputTypes"].get<std::vector<std::string>>();
    outputTypeStrs_ = config["outputTypes"].get<std::vector<std::string>>();
    if (outputTypeStrs_.size() != inputTypeStrs_.size() + 3) {
        THROW_LOGIC_EXCEPTION(
            "WindowTableFunction outputTypes must be inputTypes plus 3 window columns, but got inputTypes size "
                << inputTypeStrs_.size() << " and outputTypes size " << outputTypeStrs_.size());
    }
    for (const auto& typeStr : outputTypeStrs_) {
        outputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    isEventTime_ = config.value("timeType", std::string("event")) == "event";
    rowtimeIndex_ = config.contains("timeAttributeIndex") ? config["timeAttributeIndex"].get<int64_t>() : -1;
    shiftTimeZone_ = config.value("shiftTimeZone", std::string("UTC"));

    // windowType 由 Java 规划侧权威下发（对应 Flink createWindowAssigner 的分派决策），
    // 此处仅按权威字段实例化对应 assigner，不再解析 window 计划字符串。
    const std::string windowType = config.at("windowType").get<std::string>();
    const int64_t offset = config.contains("windowOffset") ? config["windowOffset"].get<int64_t>() : 0;
    if (windowType == "TUMBLE") {
        windowAssigner_ = new TumblingWindowAssigner(config.at("windowSize").get<int64_t>(), offset, isEventTime_);
    } else if (windowType == "HOP") {
        windowAssigner_ = new SlidingWindowAssigner(config.at("windowSize").get<int64_t>(),
            config.at("windowSlide").get<int64_t>(), offset, isEventTime_);
    } else if (windowType == "CUMULATE") {
        windowAssigner_ = new CumulativeWindowAssigner(config.at("maxSize").get<int64_t>(),
            config.at("step").get<int64_t>(), offset, isEventTime_);
    } else {
        THROW_LOGIC_EXCEPTION("Unsupported window type: " + windowType);
    }

    if (isEventTime_ && rowtimeIndex_ < 0) {
        THROW_LOGIC_EXCEPTION("Event time WindowTableFunction requires timeAttributeIndex >= 0");
    }
    if (rowtimeIndex_ >= static_cast<int64_t>(inputTypeStrs_.size())) {
        THROW_LOGIC_EXCEPTION(
            "timeAttributeIndex is out of range of inputTypes, timeAttributeIndex=" << rowtimeIndex_);
    }
}

WindowTableFunctionOperator::~WindowTableFunctionOperator()
{
    delete windowAssigner_;
    delete collector_;
}

void WindowTableFunctionOperator::open()
{
    AbstractStreamOperator<long>::open();
    collector_ = new TimestampedCollector(output);
    collector_->eraseTimestamp();
}

const char* WindowTableFunctionOperator::getName()
{
    return "WindowTableFunctionOperator";
}

std::string WindowTableFunctionOperator::getTypeName()
{
    return "WindowTableFunctionOperator";
}

void WindowTableFunctionOperator::close()
{
    if (collector_ != nullptr) {
        collector_->close();
    }
}

void WindowTableFunctionOperator::processBatch(StreamRecord* input)
{
    auto record = std::unique_ptr<StreamRecord>(input);
    auto batch = std::unique_ptr<omnistream::VectorBatch>(
        reinterpret_cast<omnistream::VectorBatch*>(record->getValue()));
    if (batch == nullptr || batch->GetRowCount() <= 0) {
        return;
    }
    processBatchInternal(batch.get());
}

void WindowTableFunctionOperator::processBatchInternal(omnistream::VectorBatch* batch)
{
    if (batch == nullptr) {
        return;
    }
    const int32_t rowCount = batch->GetRowCount();
    if (rowCount <= 0) {
        return;
    }
    std::vector<std::vector<TimeWindow>> windowsByRow(rowCount);
    for (int32_t i = 0; i < rowCount; ++i) {
        int64_t timestamp;
        if (isEventTime_) {
            auto* timeVector = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(
                batch->GetVectors()[rowtimeIndex_]);
            if (timeVector->IsNull(i)) {
                ++numNullRowTimeRecordsDropped_;
                continue;
            }
            timestamp = TimeWindowUtil::toUtcTimestampMills(timeVector->GetValue(i), shiftTimeZone_);
        } else {
            ProcessingTimeService* processingTimeService = getProcessingTimeService();
            if (processingTimeService == nullptr) {
                THROW_LOGIC_EXCEPTION("Processing time WindowTableFunction requires ProcessingTimeService");
            }
            timestamp = TimeWindowUtil::toUtcTimestampMills(
                processingTimeService->getCurrentProcessingTime(), shiftTimeZone_);
        }
        windowsByRow[i] = windowAssigner_->assignWindows(nullptr, timestamp);
    }

    omnistream::VectorBatch* outputBatch = buildOutputBatch(batch, windowsByRow);
    if (outputBatch != nullptr) {
        collector_->collect(outputBatch);
    }
}

omnistream::VectorBatch* WindowTableFunctionOperator::buildOutputBatch(
    omnistream::VectorBatch* inputBatch, const std::vector<std::vector<TimeWindow>>& windowsByRow)
{
    const int32_t inputRowCount = inputBatch->GetRowCount();
    int32_t totalRows = 0;
    for (const auto& windows : windowsByRow) {
        totalRows += static_cast<int32_t>(windows.size());
    }
    if (totalRows <= 0) {
        return nullptr;
    }

    std::vector<int32_t> positions;
    positions.reserve(totalRows);
    for (int32_t i = 0; i < inputRowCount; ++i) {
        for (size_t j = 0; j < windowsByRow[i].size(); ++j) {
            positions.push_back(i);
        }
    }

    auto* outputBatch = new omnistream::VectorBatch(totalRows);
    const int32_t inputColCount = inputBatch->GetVectorCount();
    for (int32_t col = 0; col < inputColCount; ++col) {
        auto* outVec =
            omniruntime::vec::VectorHelper::CopyPositionsVector(inputBatch->Get(col), positions.data(), 0, totalRows);
        outputBatch->Append(outVec);
    }

    auto* windowStartVec = new omniruntime::vec::Vector<int64_t>(totalRows, outputTypeIds_[inputColCount]);
    auto* windowEndVec = new omniruntime::vec::Vector<int64_t>(totalRows, outputTypeIds_[inputColCount + 1]);
    auto* windowTimeVec = new omniruntime::vec::Vector<int64_t>(totalRows, outputTypeIds_[inputColCount + 2]);

    int32_t outRow = 0;
    for (int32_t i = 0; i < inputRowCount; ++i) {
        for (const auto& window : windowsByRow[i]) {
            windowStartVec->SetValue(outRow, window.getStart());
            windowEndVec->SetValue(outRow, window.getEnd());
            windowTimeVec->SetValue(outRow, TimeWindowUtil::toEpochMills(window.maxTimestamp(), shiftTimeZone_));
            outputBatch->setRowKind(outRow, inputBatch->getRowKind(i));
            ++outRow;
        }
    }
    outputBatch->Append(windowStartVec);
    outputBatch->Append(windowEndVec);
    outputBatch->Append(windowTimeVec);
    return outputBatch;
}

void WindowTableFunctionOperator::ProcessWatermark(Watermark* mark)
{
    output->emitWatermark(mark);
}

void WindowTableFunctionOperator::processWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    output->emitWatermarkStatus(watermarkStatus);
}

void WindowTableFunctionOperator::initializeState(
    StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer)
{
    AbstractStreamOperator<long>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
    AbstractStreamOperator<long>::initializeState(initializer, keySerializer);
}

void WindowTableFunctionOperator::notifyCheckpointComplete(long checkpointId)
{
    AbstractStreamOperator<long>::notifyCheckpointComplete(checkpointId);
}

void WindowTableFunctionOperator::notifyCheckpointAborted(long checkpointId)
{
    AbstractStreamOperator<long>::notifyCheckpointAborted(checkpointId);
}
