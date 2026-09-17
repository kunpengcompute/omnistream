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

#include "AbstractWindowAggProcessor.h"

#include "table/utils/TimeWindowUtil.h"
#include "../../../generated/NamespaceAggsBasicFunctionFactory.h"
#include "runtime/operators/VectorBatchUtils.h"

AbstractWindowAggProcessor::AbstractWindowAggProcessor(const nlohmann::json description, Output* output)
    : output(output)
{
    sliceAssigner = AssignerAtt::createSliceAssigner(description);
    isEventTime = sliceAssigner->isEventTime();
    windowInterval = sliceAssigner->getSliceEndInterval();
    WindowedSliceAssigner* windowedSliceAssigner = dynamic_cast<WindowedSliceAssigner*>(sliceAssigner);
    SliceAssigner* assigner = windowedSliceAssigner->GetInnerAssigner();
    // jojo check description
    outputRowData = description.contains("OutputRecordType") &&
                    (description["OutputRecordType"].get<std::string>() == "BINARY_ROW_DATA");
    if (outputRowData) {
        collector = std::make_unique<TimestampedCollector>(this->output, false, RecordValueType::BINARY_ROW_DATA);
    } else {
        collector = std::make_unique<TimestampedCollector>(this->output);
    }

    outputTypes = description["outputTypes"].get<std::vector<std::string>>();
    for (const auto& typeStr : outputTypes) {
        outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    inputTypes = description["inputTypes"].get<std::vector<std::string>>();
    for (const auto& typeStr : inputTypes) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    keyedIndex = description["grouping"].get<std::vector<int32_t>>();
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    keySelector = std::make_unique<KeySelector<KeyType>>(keyedTypes, keyedIndex);
    const auto keyArity = keyedIndex.size();
    if (keyArity > outputTypes.size()) {
        THROW_LOGIC_EXCEPTION("The size of key fields must not exceed output type fields.");
    }
    bool isWindowAgg = description.contains("isWindowAggregate") && description["isWindowAggregate"].get<bool>();
    aggregator = initNamespaceAggsHandleFunction(isWindowAgg, description["aggInfoList"]);
}

bool AbstractWindowAggProcessor::processBatch(omnistream::VectorBatch* vectorbatch)
{
    const std::string shiftTimeZone = ResolveShiftTimeZoneId(sliceAssigner);
    std::vector<int64_t> sliceEndArr(vectorbatch->GetRowCount());
    std::vector<bool> dropArr(vectorbatch->GetRowCount(), false);
    for (int i = 0; i < vectorbatch->GetRowCount(); i++) {
        int64_t sliceEnd = sliceAssigner->assignSliceEnd(vectorbatch, i, clockService.get());
        sliceEndArr[i] = sliceEnd;
        auto currentKey = keySelector->getKey(vectorbatch, i);
        if (!isEventTime) {
            auto temp = std::make_unique<WindowKey>(sliceEnd, currentKey);
            auto result = uniqueData.insert(temp->hash());
            if (result.second) {
                stateBackend->setCurrentKey(currentKey);
                internalTimerService->registerProcessingTimeTimer(sliceEnd, sliceEnd - 1);
            }
        }
        if (isEventTime && TimeWindowUtil::isWindowFired(sliceEnd, currentProgress, shiftTimeZone)) {
            WindowedSliceAssigner* windowedSliceAssigner = dynamic_cast<WindowedSliceAssigner*>(sliceAssigner);
            SliceAssigner* assigner = windowedSliceAssigner->GetInnerAssigner();
            if (assigner == nullptr) {
                THROW_RUNTIME_ERROR("SliceAssigner is nullptr");
            }
            long lastWindowEnd = assigner->getLastWindowEnd(sliceEnd);
            if (TimeWindowUtil::isWindowFired(lastWindowEnd, currentProgress, shiftTimeZone)) {
                // the last window has been triggered, so the element can be dropped now
                dropArr[i] = true;
                continue;
            }
            // register elements;
            int64_t unfiredFirstWindow = sliceEnd;
            while (TimeWindowUtil::isWindowFired(unfiredFirstWindow, currentProgress, shiftTimeZone)) {
                unfiredFirstWindow += windowInterval;
            }
            stateBackend->setCurrentKey(currentKey);
            internalTimerService->registerEventTimeTimer(
                unfiredFirstWindow, TimeWindowUtil::toEpochMillsForTimer(unfiredFirstWindow - 1, shiftTimeZone));
        }
    }
    windowBuffer->addVectorBatch(vectorbatch, sliceEndArr, dropArr);
    return false;
}

void AbstractWindowAggProcessor::open(
    AbstractKeyedStateBackend<AbstractWindowAggProcessor::KeyType>* keyedStateBackend,
    const nlohmann::json& config,
    StreamingRuntimeContext<AbstractWindowAggProcessor::KeyType>* runtimeCtx,
    InternalTimerServiceImpl<AbstractWindowAggProcessor::KeyType, int64_t>* internalTimerService)
{
    this->stateBackend = keyedStateBackend;
    if (dynamic_cast<RocksdbKeyedStateBackend<AbstractWindowAggProcessor::KeyType>*>(keyedStateBackend) != nullptr) {
        backendType_ = omnistream::StateType::ROCKSDB;
    } else if (
        dynamic_cast<HeapKeyedStateBackend<AbstractWindowAggProcessor::KeyType>*>(keyedStateBackend) != nullptr) {
        backendType_ = omnistream::StateType::HEAP;
    } else {
        THROW_LOGIC_EXCEPTION("The keyedStateBackend is not supported");
    }
    this->internalTimerService = internalTimerService;
    omnistream::RowType accRowType(true, accTypes_);
    auto* accSerializer = new RowDataSerializer(&accRowType);
    // init WindowValueState
    std::string aggName = "window-aggs";
    auto* accDesc = new ValueStateDescriptor<RowData*>(aggName, accSerializer);
    using S = InternalValueState<KeyType, int64_t, RowData*>;
    S* state = keyedStateBackend->template getOrCreateKeyedState<int64_t, S, RowData*>(new LongSerializer(), accDesc);
    windowState = std::make_unique<WindowValueState<KeyType, int64_t, RowData*>>(state);
    windowBuffer = std::make_unique<RecordsWindowBuffer>(
        config, windowState.get(), output, this->stateBackend, sliceAssigner, internalTimerService);
}

void AbstractWindowAggProcessor::initializeWatermark(int64_t watermark)
{
    if (isEventTime) {
        currentProgress = watermark;
    }
}

void AbstractWindowAggProcessor::advanceProgress(long progress)
{
    if (progress > currentProgress) {
        currentProgress = progress;
        if (currentProgress >= nextTriggerProgress) {
            windowBuffer->advanceProgress(currentProgress);
            nextTriggerProgress =
                LocalSlicingWindowAggOperator::getNextTriggerWatermark(currentProgress, windowInterval);
        }
    }
    LOG("end AbstractWindowAggProcessor::advanceProgress");
}

void AbstractWindowAggProcessor::prepareCheckpoint()
{
    windowBuffer->flush();
}

void AbstractWindowAggProcessor::fireWindow(int64_t windowEnd)
{
    LOG("AbstractWindowAggProcessor " << this << " fireWindow:" << windowEnd);
    if (!sliceAssigner->hasSliceEndIndex && !sliceAssigner->hasWindowEndIndex) {
        LOG("get value in the firewindow");
        std::unique_ptr<RowData> result(GetNonHopResult(windowEnd));
        if (stateBackend->getCurrentKey() == nullptr || result == nullptr) {
            return;
        }
        ProcessNonHopResult(result.get());
        return;
    }
    WindowedSliceAssigner* windowedSliceAssigner = dynamic_cast<WindowedSliceAssigner*>(sliceAssigner);
    SliceAssigner* assigner = windowedSliceAssigner->GetInnerAssigner();
    if (dynamic_cast<HoppingSliceAssigner*>(assigner) != nullptr && sliceAssigner->hasSliceEndIndex) {
        // HOP时间窗口处理
        LOG("merge in the firewindow");
        HoppingSliceAssigner* hoppingSliceAssigner = dynamic_cast<HoppingSliceAssigner*>(assigner);
        std::unique_ptr<RowData> result(
            GetHopResult(windowEnd, hoppingSliceAssigner->getNumSlicesPerWindow(), assigner->getSliceEndInterval()));
        if (stateBackend->getCurrentKey() == nullptr || result == nullptr) {
            return;
        }
        ProcessHopResult(result.get());
        int64_t nextWindowEnd = hoppingSliceAssigner->NextTriggerWindow(windowEnd, isWindowEmpty());
        NextWindowEndProcess(nextWindowEnd, assigner);
        delete aggregator->getAccumulators();
    } else {
        // 非HOP时间窗口处理
        LOG("get value in the firewindow");
        std::unique_ptr<RowData> result(GetNonHopResult(windowEnd));
        if (stateBackend->getCurrentKey() == nullptr || result == nullptr) {
            return;
        }
        ProcessNonHopResult(result.get());
    }
}

void AbstractWindowAggProcessor::ProcessHopResult(RowData* result)
{
    resultRow->replace(stateBackend->getCurrentKey().get(), result);
    if (static_cast<size_t>(resultRow->getArity()) == outputTypes.size()) {
        if (!isWindowEmpty()) {
            LOG("window not empty");
            if (outputRowData) {
                collector->collect(BinaryRowDataSerializer::joinedRowToBinaryRow(resultRow.get(), outputTypeIds));
            } else {
                auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(1, outputTypeIds);
                SetOutputBatch(outputBatch, resultRow.get());
                collector->collect(outputBatch);
            }
        } else {
            LOG("window is empty");
        }
    } else {
        THROW_LOGIC_EXCEPTION("Unexpected fireWindow result! ResultRow and outputTypes columns not equal!!");
    }
}

void AbstractWindowAggProcessor::ProcessNonHopResult(RowData* result)
{
    resultRow->replace(stateBackend->getCurrentKey().get(), result);
    if (resultRow->getArity() == static_cast<int>(outputTypes.size())) {
        if (outputRowData) {
            collector->collect(BinaryRowDataSerializer::joinedRowToBinaryRow(resultRow.get(), outputTypeIds));
        } else {
            auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(1, outputTypeIds);
            collector->collect(BinaryRowDataSerializer::joinedRowToBinaryRow(resultRow.get(), outputTypeIds));
            SetOutputBatch(outputBatch, resultRow.get());
            collector->collect(outputBatch);
        }
    } else {
        THROW_LOGIC_EXCEPTION("Unexpected fireWindow result! ResultRow and outputTypes columns not equal!!");
    }
}

RowData* AbstractWindowAggProcessor::GetNonHopResult(int64_t windowEnd)
{
    RowData* acc = windowState->value(windowEnd);
    if (acc == nullptr) {
        return nullptr;
    }
    aggregator->setAccumulators(windowEnd, acc);
    RowData* result = aggregator->getValue(windowEnd);
    if (shouldDeleteWindowStateValue()) {
        delete acc;
    }
    return result;
}

void AbstractWindowAggProcessor::NextWindowEndProcess(int64_t nextWindowEnd, SliceAssigner* assigner)
{
    if (nextWindowEnd != 0) {
        const std::string shiftTimeZone = ResolveShiftTimeZoneId(sliceAssigner);
        if (assigner->isEventTime()) {
            internalTimerService->registerEventTimeTimer(
                nextWindowEnd, TimeWindowUtil::toEpochMillsForTimer(nextWindowEnd - 1, shiftTimeZone));
        } else {
            internalTimerService->registerProcessingTimeTimer(nextWindowEnd, nextWindowEnd - 1);
        }
    }
}

RowData* AbstractWindowAggProcessor::GetHopResult(int64_t windowEnd, int64_t numSlices, int64_t interval)
{
    int64_t tempWindow = windowEnd;
    auto acc = aggregator->createAccumulators();
    aggregator->setAccumulators(windowEnd, acc);

    for (int i = 0; i < numSlices; ++i) {
        RowData* sliceAcc = windowState->value(tempWindow);
        if (sliceAcc != nullptr) {
            aggregator->merge(tempWindow, sliceAcc);
        }
        tempWindow -= interval;
        if (shouldDeleteWindowStateValue()) {
            delete sliceAcc;
        }
    }
    RowData* result = aggregator->getValue(windowEnd);

    return result;
}

bool AbstractWindowAggProcessor::isWindowEmpty()
{
    if (indexOfCountStar_ < 0) {
        return false;
    }
    auto acc = aggregator->getAccumulators();
    return acc == nullptr || acc->isNullAt(indexOfCountStar_) || *acc->getLong(indexOfCountStar_) == 0;
}

bool AbstractWindowAggProcessor::shouldDeleteWindowStateValue() const
{
    return backendType_ == omnistream::StateType::ROCKSDB && !windowState->isFalconEnabled();
}

void AbstractWindowAggProcessor::SetOutputBatch(omnistream::VectorBatch* outputBatch, JoinedRowData* resultRow)
{
    int numColumns = outputTypes.size();

    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        switch (outputTypeIds.at(colIndex)) {
            case DataTypeId::OMNI_LONG: {
                auto vector = static_cast<omniruntime::vec::Vector<int64_t>*>(outputBatch->Get(colIndex));
                vector->SetValue(0, *resultRow->getLong(colIndex));
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP: {
                auto vector = static_cast<omniruntime::vec::Vector<int64_t>*>(outputBatch->Get(colIndex));
                vector->SetValue(0, *resultRow->getLong(colIndex));
                break;
            }
            case DataTypeId::OMNI_INT: {
                auto vector = static_cast<omniruntime::vec::Vector<int64_t>*>(outputBatch->Get(colIndex));
                vector->SetValue(0, *resultRow->getInt(colIndex));
                break;
            }
            case DataTypeId::OMNI_DOUBLE: {
                auto vector = static_cast<omniruntime::vec::Vector<int64_t>*>(outputBatch->Get(colIndex));
                vector->SetValue(0, *resultRow->getLong(colIndex));
                break;
            }
            case DataTypeId::OMNI_BOOLEAN: {
                auto vector = static_cast<omniruntime::vec::Vector<int64_t>*>(outputBatch->Get(colIndex));
                vector->SetValue(0, *resultRow->getInt(colIndex));
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                auto vector =
                    static_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                        outputBatch->Get(colIndex));
                std::string_view strView = resultRow->getStringView(colIndex);
                vector->SetValue(0, strView);
                break;
            }
            default: {
                throw std::runtime_error("Unsupported column type in inputRow");
            }
        }
    }

    outputBatch->setRowKind(0, resultRow->getRowKind());
}

void AbstractWindowAggProcessor::setClockService(ClockService* newClock)
{
    if (clockService.get() == newClock) return;
    clockService.reset(newClock);
}

void AbstractWindowAggProcessor::clearWindow(int64_t windowEnd)
{
    auto* windowedSliceAssigner = dynamic_cast<WindowedSliceAssigner*>(sliceAssigner);
    SliceAssigner* assigner = windowedSliceAssigner->GetInnerAssigner();
    IteratorBase* expiredSlices = assigner->expiredSlices(windowEnd);
    while (expiredSlices->hasNext()) {
        int64_t expiredSliceEnd = expiredSlices->next();
        windowState->clear(expiredSliceEnd);
        aggregator->cleanup(windowEnd);
    }
}

void AbstractWindowAggProcessor::close()
{
    if (windowBuffer != nullptr) {
        windowBuffer->close();
    }
    aggregator->close();
}

TypeSerializer* AbstractWindowAggProcessor::createWindowSerializer()
{
    return LongSerializer::INSTANCE;
}

Output* AbstractWindowAggProcessor::getOutput()
{
    return output;
}

std::unique_ptr<NamespaceAggsHandleFunction<int64_t>> AbstractWindowAggProcessor::initNamespaceAggsHandleFunction(
    bool isWindowAgg, const nlohmann::json& aggInfoList)
{
    std::string const aggCallsName = isWindowAgg ? "aggregateCalls" : "globalAggregateCalls";
    std::string const accTypesName = isWindowAgg ? "AccTypes" : "globalAccTypes";

    indexOfCountStar_ = aggInfoList["indexOfCountStar"].get<int32_t>();
    const bool countStarInserted = aggInfoList.value("countStarInserted", false);

    auto accTypes = aggInfoList[accTypesName].get<std::vector<std::string>>();
    accTypes.erase(
        std::remove_if(
            accTypes.begin(),
            accTypes.end(),
            [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
        accTypes.end());
    accumulatorArity_ = accTypes.size();
    accTypes_ = accTypes;
    std::vector<int32_t> accTypeIds;
    for (const auto& typeStr : accTypes) {
        accTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    const std::string aggValueTypeName = isWindowAgg ? "aggValueTypes" : "globalAggValueTypes";
    auto aggValueTypes = aggInfoList[aggValueTypeName].get<std::vector<std::string>>();
    aggValueTypes.erase(
        std::remove_if(
            aggValueTypes.begin(),
            aggValueTypes.end(),
            [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
        aggValueTypes.end());
    std::vector<int32_t> aggValueTypeIds;
    for (const auto& typeStr : aggValueTypes) {
        aggValueTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    std::vector<std::unique_ptr<NamespaceAggsBasicFunction<int64_t>>> functions;
    auto accStartIndex = 0;
    auto aggValueStartIndex = 0;
    for (const auto& aggCall : aggInfoList[aggCallsName]) {
        std::string aggTypeStr = aggCall["name"];
        const int32_t filterIndex = aggCall["filterArg"].get<int32_t>(); // TODO: not support now

        const auto argIndexes = aggCall.value("argIndexes", std::vector<int32_t>{});
        const bool isInsertedCountStar = countStarInserted && accStartIndex == indexOfCountStar_;
        const int32_t aggValueIndex = isInsertedCountStar ? -1 : aggValueStartIndex;
        const int32_t aggValueTypeId = isInsertedCountStar ? -1 : aggValueTypeIds[aggValueIndex];
        auto accIndexes = NamespaceAggsBasicFunctionFactory::getAccIndexes(aggTypeStr, accStartIndex);

        functions.push_back(
            NamespaceAggsBasicFunctionFactory::create<int64_t>(
                aggTypeStr, argIndexes, this->inputTypeIds_, accIndexes, accTypeIds, aggValueIndex, aggValueTypeId));
        accStartIndex += accIndexes.size();
        if (!isInsertedCountStar) {
            aggValueStartIndex++;
        }
    }

    return std::make_unique<WindowAggsHandleFunction>(
        std::move(functions),
        std::move(aggValueTypeIds),
        std::vector<int32_t>(outputTypeIds.begin() + keyedIndex.size(), outputTypeIds.end()),
        sliceAssigner,
        accumulatorArity_);
}
