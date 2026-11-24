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

#include "RecordsWindowBuffer.h"
#include "runtime/generated/function/CountDistinctFunction.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "../../../../generated/NamespaceAggsBasicFunctionFactory.h"
#include "table/data/util/RowDataUtil.h"
#include "table/utils/TimeWindowUtil.h"
#include "table/runtime/operators/VectorBatchUtils.h"

RecordsWindowBuffer::RecordsWindowBuffer(
    const nlohmann::json& config,
    WindowValueState<RecordsWindowBuffer::KeyType, int64_t, RowData*>* state,
    Output* output,
    KeyedStateBackend<KeyType>* stateBackend,
    SliceAssigner* sliceAssigner,
    InternalTimerServiceImpl<RecordsWindowBuffer::KeyType, int64_t>* internalTimerService)
    : output(output),
      stateBackend_(stateBackend),
      internalTimerService(internalTimerService)
{
    this->description = config;
    this->sliceAssigner = sliceAssigner;

    if (dynamic_cast<RocksdbKeyedStateBackend<KeyType>*>(stateBackend_)) {
        INFO_RELEASE("RecordsWindowBuffer backend is rocksdb");
        this->backendType_ = omnistream::StateType::ROCKSDB;
    } else {
        INFO_RELEASE("RecordsWindowBuffer backend is mem");
        this->backendType_ = omnistream::StateType::HEAP;
    }

    shiftTimeZone = ResolveShiftTimeZoneId(sliceAssigner);
    inputTypes = config["inputTypes"].get<std::vector<std::string>>();
    for (const auto& typeStr : inputTypes) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    outputTypes = config["outputTypes"].get<std::vector<std::string>>();
    for (const auto& typeStr : outputTypes) {
        outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    InitializeKeySelectorAndTypes(config);
    isWindowAgg = config.contains("isWindowAggregate") && description["isWindowAggregate"].get<bool>();

    accState = state;
    const auto keyArity = keyedIndex.size();
    if (keyArity > outputTypes.size()) {
        THROW_LOGIC_EXCEPTION("The size of key fields must not exceed output type fields.");
    }
    initNamespaceAggsHandleFunction(description["aggInfoList"]);
}

RecordsWindowBuffer::RecordsWindowBuffer(const nlohmann::json& config, Output* output, SliceAssigner* sliceAssigner)
    : output(output),
      stateBackend_(nullptr),
      internalTimerService(nullptr)
{
    this->description = config;
    this->sliceAssigner = sliceAssigner;
    this->collector = new TimestampedCollector(this->output);
    shiftTimeZone = ResolveShiftTimeZoneId(sliceAssigner);

    inputTypes = config["inputTypes"].get<std::vector<std::string>>();
    for (const auto& typeStr : inputTypes) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    outputTypes = config["outputTypes"].get<std::vector<std::string>>();
    for (const auto& typeStr : outputTypes) {
        outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    InitializeKeySelectorAndTypes(config);
    isWindowAgg = true;

    accState = nullptr;
    windowRow = std::make_unique<GenericRowData>(1);
    accWindowRow = std::make_unique<JoinedRowData>();
    resultRow = std::make_unique<JoinedRowData>();

    const auto keyArity = keyedIndex.size();
    if (keyArity > outputTypes.size()) {
        THROW_LOGIC_EXCEPTION("The size of key fields must not exceed output type fields.");
    }
    initNamespaceAggsHandleFunction(description["aggInfoList"]);
}

void RecordsWindowBuffer::InitializeKeySelectorAndTypes(const nlohmann::json& config)
{
    keyedIndex = config["grouping"].get<std::vector<int32_t>>();
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    keySelector = std::make_unique<KeySelector<KeyType>>(keyedTypes, keyedIndex);
}

void RecordsWindowBuffer::initNamespaceAggsHandleFunction(const nlohmann::json& aggInfoList)
{
    std::string const accTypesName = isWindowAgg ? "accTypes" : "globalAccTypes";
    std::string const aggCallsName = isWindowAgg ? "aggregateCalls" : "globalAggregateCalls";
    std::string const aggValueTypesName = isWindowAgg ? "aggValueTypes" : "globalAggValueTypes";

    auto accTypes = aggInfoList[accTypesName].get<std::vector<std::string>>();
    accTypes.erase(
        std::remove_if(
            accTypes.begin(),
            accTypes.end(),
            [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
        accTypes.end());
    accumulatorArity = accTypes.size();

    std::vector<int32_t> accTypeIds;
    accTypeIds.reserve(accTypes.size());
    for (const auto& type : accTypes) {
        accTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(type));
    }

    auto aggValueTypes = aggInfoList[aggValueTypesName].get<vector<std::string>>();
    aggValueTypes.erase(
        std::remove_if(
            aggValueTypes.begin(),
            aggValueTypes.end(),
            [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
        aggValueTypes.end());
    std::vector<int32_t> aggValueTypeIds;
    for (const auto& type : aggValueTypes) {
        aggValueTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(type));
    }

    int accStartIndex = 0;
    int aggValueStartIndex = 0;

    const int32_t indexOfCountStar = aggInfoList["indexOfCountStar"].get<int32_t>();
    const bool countStarInserted = aggInfoList.value("countStarInserted", false);

    for (const auto& aggCall : aggInfoList[aggCallsName]) {
        std::string aggTypeStr = aggCall["name"];
        auto argIndexes = aggCall.value("argIndexes", std::vector<int32_t>{});
        // aggIndex identifies the aggregate function; argIndexes identify columns in the current input row.
        const int32_t filterIndex = aggCall.value("filterArg", -1); // TODO: not support now
        const bool isInsertedCountStar = countStarInserted && accStartIndex == indexOfCountStar;
        const int32_t aggValueIndex = isInsertedCountStar ? -1 : aggValueStartIndex;
        const int32_t aggValueTypeId = isInsertedCountStar ? -1 : aggValueTypeIds[aggValueIndex];
        auto accIndexes = NamespaceAggsBasicFunctionFactory::getAccIndexes(aggTypeStr, accStartIndex);
        auto localFunction = NamespaceAggsBasicFunctionFactory::create<int64_t>(
            aggTypeStr, argIndexes, this->inputTypeIds_, accIndexes, accTypeIds, aggValueIndex, aggValueTypeId);
        localFunctions.push_back(std::move(localFunction));
        if (!isWindowAgg) {
            auto globalFunction = NamespaceAggsBasicFunctionFactory::create<int64_t>(
                aggTypeStr, argIndexes, this->inputTypeIds_, accIndexes, accTypeIds, aggValueIndex, aggValueTypeId);
            globalFunctions.push_back(std::move(globalFunction));
        }
        accStartIndex += accIndexes.size();
        if (!isInsertedCountStar) {
            aggValueStartIndex++;
        }
    }
    localAggregator = std::make_unique<WindowAggsHandleFunction>(
        std::move(localFunctions),
        aggValueTypeIds,
        std::vector<int32_t>(outputTypeIds.begin() + keyedIndex.size(), outputTypeIds.end()),
        sliceAssigner,
        accumulatorArity);
    if (!isWindowAgg) {
        globalAggregator = std::make_unique<WindowAggsHandleFunction>(
            std::move(globalFunctions),
            aggValueTypeIds,
            std::vector<int32_t>(outputTypeIds.begin() + keyedIndex.size(), outputTypeIds.end()),
            sliceAssigner,
            accumulatorArity);
    }
}

std::vector<std::string> RecordsWindowBuffer::getKeyedTypes(
    std::vector<int32_t> keyedIndex, std::vector<std::string> inputTypes)
{
    std::vector<std::string> keyedTypes;
    for (int32_t index : keyedIndex) {
        if (index >= 0 && static_cast<size_t>(index) < inputTypes.size()) {
            keyedTypes.push_back(inputTypes[index]);
        }
    }
    return keyedTypes;
}

// skip droped records, only add valid records to window buffer
void RecordsWindowBuffer::addVectorBatch(
    omnistream::VectorBatch* input, std::vector<int64_t>& sliceEndArr, std::vector<bool>& dropArr)
{
    auto rowCount = input->GetRowCount();
    if (rowCount <= 0) {
        return;
    }

    bool needsFlush = false;
    {
        std::lock_guard<std::mutex> lock(bufferMutex);

        for (int row = 0; row < rowCount; ++row) {
            // Skip dropped records early
            if (dropArr[row]) {
                continue;
            }

            auto key = keySelector->getKey(input, row);
            const long rowTime = sliceEndArr[row];

            minSliceEnd = std::min(rowTime, minSliceEnd);

            auto sliceResultRow = std::unique_ptr<RowData>(localAggregator->createAccumulators());
            sliceResultRow->setRowKind(input->getRowKind(row));

            for (int accIndex = 0; accIndex < accumulatorArity; ++accIndex) {
                int32_t columnIndex = keyedIndex.size() + accIndex;
                if (!input->Get(columnIndex)->IsNull(row)) {
                    // TODO: only BIGINT is supported now
                    sliceResultRow->setLong(accIndex, input->GetValueAt<long>(columnIndex, row));
                }
            }

            // Insert into the buffer
            auto [it, inserted] = globalRecordsBuffer.try_emplace(WindowKey(rowTime, key));
            it->second.push_back(std::move(sliceResultRow));
            ++recordsBufferSize_;
        }

        if (recordsBufferSize_ > recordsBufferSizeLimit_) {
            needsFlush = true;
        }
    }

    if (needsFlush) {
        LOG("reach recordsBufferSize_ limit");
        flush();
    }
}

// for local windowAgg
void RecordsWindowBuffer::addVectorBatch(omnistream::VectorBatch* input, std::vector<int64_t>& sliceEndArr)
{
    auto rowCount = input->GetRowCount();
    if (rowCount <= 0) {
        return;
    }

    bool needsFlush = false;
    {
        std::lock_guard<std::mutex> lock(bufferMutex);
        for (int row = 0; row < rowCount; ++row) {
            auto keyRow = keySelector->getKey(input, row);
            long rowTime = sliceEndArr[row];

            minSliceEnd = std::min(rowTime, minSliceEnd);

            auto [it, inserted] = localRecordsBuffer.try_emplace(WindowKey(rowTime, keyRow));
            it->second.push_back(VectorBatchUtil::getComboId(currentBatchId, row));

            ++recordsBufferSize_;
        }
        currentBatchId++;
        if (recordsBufferSize_ > recordsBufferSizeLimit_) {
            needsFlush = true;
        }
    }
    retainedBatches.push_back(std::move(input));
    if (needsFlush) {
        LOG("reach recordsBufferSize_ limit");
        flush();
    }
}

void RecordsWindowBuffer::advanceProgress(long currentProgress)
{
    if (!TimeWindowUtil::isWindowFired(minSliceEnd, currentProgress, shiftTimeZone)) {
        LOG("no windows in record buffer is fired.");
        return;
    }
    flush();
}

void RecordsWindowBuffer::flush()
{
    if (isWindowAgg) {
        decltype(localRecordsBuffer) localBuffer;
        {
            std::lock_guard<std::mutex> lock(bufferMutex);
            std::swap(localBuffer, localRecordsBuffer);
            minSliceEnd = INT64_MAX;
        }
        if (localBuffer.empty()) {
            return;
        }
        int numRows = localBuffer.size();
        int numColumns = outputTypes.size();
        auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(numRows, outputTypeIds);
        int currentRowNum = 0;
        for (auto& pair : localBuffer) {
            WindowKey currentKey = pair.first;
            std::vector<int64_t>& combinedIdArr = pair.second;
            // do we still need the iteration?
            auto iter = combinedIdArr.begin();
            while (iter != combinedIdArr.end()) {
                int64_t element = *iter;
                int batchId = VectorBatchUtil::getBatchId(element);
                int rowId = VectorBatchUtil::getRowId(element);

                if (batchId >= retainedBatches.size()) {
                    LOG("ERROR: batchId out of bounds!");
                    iter = combinedIdArr.erase(iter);
                    continue;
                }

                auto& targetBatch = retainedBatches[batchId];
                if (RowDataUtil::isRetractMsg(targetBatch->getRowKind(rowId))) {
                    iter = combinedIdArr.erase(iter);
                } else {
                    ++iter;
                }
            }

            if (combinedIdArr.empty()) {
                continue;
            }
            winAggProcess(currentKey, combinedIdArr);

            for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
                switch (outputTypeIds[colIndex]) {
                    case DataTypeId::OMNI_LONG: {
                        SetLong(outputBatch, currentRowNum, colIndex, resultRow.get());
                        break;
                    }
                    case DataTypeId::OMNI_TIMESTAMP: {
                        SetLong(outputBatch, currentRowNum, colIndex, resultRow.get());
                        break;
                    }
                    case DataTypeId::OMNI_INT: {
                        SetInt(outputBatch, currentRowNum, colIndex, resultRow.get());
                        break;
                    }
                    case DataTypeId::OMNI_DOUBLE: {
                        SetLong(outputBatch, currentRowNum, colIndex, resultRow.get());
                        break;
                    }
                    case DataTypeId::OMNI_BOOLEAN: {
                        SetInt(outputBatch, currentRowNum, colIndex, resultRow.get());
                        break;
                    }
                    case DataTypeId::OMNI_VARCHAR: {
                        SetStringVectorBatch(outputBatch, currentRowNum, colIndex, resultRow.get());
                        break;
                    }
                    default: {
                        throw std::runtime_error("Unsupported column type in inputRow");
                    }
                }
            }
            outputBatch->setRowKind(currentRowNum, resultRow->getRowKind());
            currentRowNum++;
            if (accWindowRow != nullptr) {
                auto accRow = accWindowRow->getRow1();
                if (accRow != nullptr) {
                    delete accRow;
                    accWindowRow->setRow1(nullptr);
                }
            }
        }
        // output local windowAgg here.
        if (currentRowNum != numRows) {
            outputBatch->Resize(currentRowNum);
        }
        collector->collect(outputBatch);
        recordsBufferSize_ = 0;
        for (int i = lastDeletedBatchIndex; i <= currentBatchId - 1; i++) {
            auto input = retainedBatches[i];
            delete input;
            retainedBatches[i] = nullptr;
        }
        lastDeletedBatchIndex = currentBatchId;
        return;
    }
    // for global window Agg
    decltype(globalRecordsBuffer) localBuffer;
    {
        std::lock_guard<std::mutex> lock(bufferMutex);
        std::swap(localBuffer, globalRecordsBuffer);
        minSliceEnd = INT64_MAX;
    }
    if (localBuffer.empty()) {
        return;
    }
    for (auto& pair : localBuffer) {
        WindowKey currentKey = pair.first;
        auto& sliceResultArr = pair.second;
        auto iter = sliceResultArr.begin();
        while (iter != sliceResultArr.end()) {
            if (RowDataUtil::isRetractMsg((*iter)->getRowKind())) {
                iter = sliceResultArr.erase(iter); // Safe iteration erasure
            } else {
                ++iter;
            }
        }
        if (!sliceResultArr.empty()) {
            globalWinAggProcess(currentKey, sliceResultArr);
        }
    }
    recordsBufferSize_ = 0;
}

void RecordsWindowBuffer::winAggProcess(const WindowKey& currentWindowKey, std::vector<int64_t>& combinedIdArr)
{
    long window = currentWindowKey.getWindow();

    RowData* accumulators = localAggregator->createAccumulators();

    localAggregator->setAccumulators(window, accumulators);
    std::vector<int64_t> accumulateArr;
    std::vector<int64_t> retractArr;

    for (auto combinedId : combinedIdArr) {
        int batchId = VectorBatchUtil::getBatchId(combinedId);
        int rowId = VectorBatchUtil::getRowId(combinedId);
        if (batchId >= retainedBatches.size()) {
            LOG("ERROR: batchId out of bounds!");
            continue;
        }

        auto& targetBatch = retainedBatches[batchId];
        if (targetBatch == nullptr) {
            LOG("ERROR: targetBatch is NULL for batchId " << batchId);
            continue;
        }

        if (RowDataUtil::isAccumulateMsg(targetBatch->getRowKind(rowId))) {
            accumulateArr.push_back(combinedId);
        } else {
            retractArr.push_back(combinedId);
        }
    }
    if (!accumulateArr.empty()) {
        localAggregator->accumulate(retainedBatches, accumulateArr);
    }
    if (!retractArr.empty()) {
        localAggregator->retract(retainedBatches, retractArr);
    }

    accumulators = localAggregator->getAccumulators();
    windowRow->setField(0, window);
    accWindowRow->replace(accumulators, windowRow.get());
    resultRow->replace(currentWindowKey.getKey().get(), accWindowRow.get());
}

void RecordsWindowBuffer::globalWinAggProcess(
    const WindowKey& currentWindowKey, std::vector<std::unique_ptr<RowData>>& sliceResultArr)
{
    RowData* accumulators = localAggregator->createAccumulators();
    localAggregator->setAccumulators(currentWindowKey.getWindow(), accumulators);

    for (auto& sliceResultRow : sliceResultArr) {
        if (RowDataUtil::isAccumulateMsg(sliceResultRow->getRowKind())) {
            localAggregator->merge(currentWindowKey.getWindow(), sliceResultRow.get());
        }
    }
    accumulators = localAggregator->getAccumulators();
    combineAccumulator(currentWindowKey, accumulators);
    delete accumulators;
}

void RecordsWindowBuffer::combineAccumulator(const WindowKey& windowKey, RowData* acc)
{
    // step 1: set current key for states and timers
    stateBackend_->setCurrentKey(windowKey.getKey());
    long window = windowKey.getWindow();

    // step2: merge acc into state
    RowData* stateVal = accState->value(window);

    if (stateVal == nullptr) {
        stateVal = globalAggregator->createAccumulators();
    }
    globalAggregator->setAccumulators(window, stateVal);
    globalAggregator->merge(window, acc);
    stateVal = globalAggregator->getAccumulators();
    accState->update(window, stateVal);
    if (shouldDeleteWindowStateValue()) {
        delete stateVal;
    }

    // step 3: register timer for current window
    if (!TimeWindowUtil::isWindowFired(window, internalTimerService->currentWatermark(), shiftTimeZone)) {
        internalTimerService->registerEventTimeTimer(
            window, TimeWindowUtil::toEpochMillsForTimer(window - 1, shiftTimeZone));
    }
}

omnistream::VectorBatch* RecordsWindowBuffer::createOutputBatch(std::vector<std::unique_ptr<RowData>>& collectedRows)
{
    int numColumns = outputTypes.size();
    int numRows = collectedRows.size(); // Number of rows collected
    // Create a new VectorBatch (empty if no rows exist)
    std::unique_ptr<omnistream::VectorBatch> outputBatch(new omnistream::VectorBatch(numRows));
    // Loop through each column and create vectors
    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        switch (outputTypeIds[colIndex]) {
            case DataTypeId::OMNI_LONG: {
                VectorBatchUtils::AppendLongVectorForInt64(outputBatch.get(), collectedRows, numRows, colIndex);
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP: {
                VectorBatchUtils::AppendLongVectorForInt64(outputBatch.get(), collectedRows, numRows, colIndex);
                break;
            }
            case DataTypeId::OMNI_INT: {
                VectorBatchUtils::AppendIntVector(outputBatch.get(), collectedRows, numRows, colIndex);
                break;
            }
            case DataTypeId::OMNI_DOUBLE: {
                VectorBatchUtils::AppendLongVectorForDouble(outputBatch.get(), collectedRows, numRows, colIndex);
                break;
            }
            case DataTypeId::OMNI_BOOLEAN: {
                VectorBatchUtils::AppendIntVectorForBool(outputBatch.get(), collectedRows, numRows, colIndex);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                VectorBatchUtils::AppendStringVector(outputBatch.get(), collectedRows, numRows, colIndex);
                break;
            }
            default: {
                throw std::runtime_error("Unsupported column type in inputRow");
            }
        }
    }

    // Set row kind for all rows (only if there are rows)
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        outputBatch->setRowKind(rowIndex, collectedRows[rowIndex]->getRowKind());
    }
    return outputBatch.release();
}

void RecordsWindowBuffer::collectOutputBatch(TimestampedCollector* out, omnistream::VectorBatch* outputBatch)
{
    out->collect(outputBatch);
}

Output* RecordsWindowBuffer::getOutput()
{
    return this->output;
}

bool RecordsWindowBuffer::shouldDeleteWindowStateValue() const
{
    return backendType_ == omnistream::StateType::ROCKSDB && !accState->isFalconEnabled();
}

void RecordsWindowBuffer::SetStringVectorBatch(
    omnistream::VectorBatch* outputBatch, int rowIndex, int colIndex, RowData* collectedRow)
{
    auto vector = static_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
        outputBatch->Get(colIndex));
    std::string_view strView = collectedRow->getStringView(colIndex);
    vector->SetValue(rowIndex, strView);
}

void RecordsWindowBuffer::SetLong(
    omniruntime::vec::VectorBatch* outputBatch, int rowIndex, int colIndex, RowData* collectedRow)
{
    auto vector = static_cast<omniruntime::vec::Vector<int64_t>*>(outputBatch->Get(colIndex));
    vector->SetValue(rowIndex, *collectedRow->getLong(colIndex));
}

void RecordsWindowBuffer::SetInt(
    omniruntime::vec::VectorBatch* outputBatch, int rowIndex, int colIndex, RowData* collectedRow)
{
    auto vector = static_cast<omniruntime::vec::Vector<int64_t>*>(outputBatch->Get(colIndex));
    vector->SetValue(rowIndex, *collectedRow->getInt(colIndex));
}
