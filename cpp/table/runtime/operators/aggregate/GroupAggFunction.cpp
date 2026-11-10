/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include "GroupAggFunction.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "runtime/generated/function/AverageFunction.h"
#include "runtime/generated/function/SharedDistinctCountContainerFunction.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "runtime/generated/function/CountFunction.h"
#include "runtime/generated/function/MinMaxFunction.h"
#include "runtime/generated/function/SumFunction.h"
#include "runtime/generated/function/udf/LastStringValueFunction.h"
#include "runtime/generated/function/JsonObjectAggFunction.h"
#include "runtime/generated/function/JsonArrayAggFunction.h"
#include <algorithm>
#include <iostream>
#include <regex>
#include <tuple>
#include <unordered_map>
#include <utility>

GroupAggFunction::GroupAggFunction(long stateRetentionTime, const nlohmann::json& config)
    : stateRetentionTime(stateRetentionTime),
      description(config)
{
    indexOfCountStar = config["aggInfoList"]["indexOfCountStar"];
    recordCounter = std::move(RecordCounter::of(indexOfCountStar));
    accTypes = config["aggInfoList"]["accTypes"].get<std::vector<std::string>>();
    accTypes.erase(
        std::remove_if(
            accTypes.begin(),
            accTypes.end(),
            [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
        accTypes.end());
    aggValueTypes = config["aggInfoList"]["aggValueTypes"].get<std::vector<std::string>>();
    accumulatorArity = accTypes.size();
    generateUpdateBefore = config.value("generateUpdateBefore", false);
    inputTypes = config["inputTypes"].get<std::vector<std::string>>();
    outputTypes = config["outputTypes"].get<std::vector<std::string>>();
    keyedIndex = config["grouping"].get<std::vector<int32_t>>();
    keyedTypes = getKeyedTypes(keyedIndex, config["inputTypes"]);
    groupByKeySelector = new KeySelector<RowData*>(keyedTypes, keyedIndex);
    distinctInfos = config["distinctInfos"].get<std::vector<DistinctInfo>>();
}

GroupAggFunction::~GroupAggFunction()
{
    for (auto func : functions) {
        delete func;
    }
}

bool IntEqualiser(RowData* r1, RowData* r2, int colIdx)
{
    return *r1->getInt(colIdx) == *r2->getInt(colIdx);
}

bool LongEqualiser(RowData* r1, RowData* r2, int colIdx)
{
    return *r1->getLong(colIdx) == *r2->getLong(colIdx);
}

bool TimestampEqualiser(RowData* r1, RowData* r2, int colIdx)
{
    return *r1->getLong(colIdx) == *r2->getLong(colIdx);
}

std::string extractAggFunction(const std::string& input)
{
    std::regex aggRegex(
        R"((?:JSON_OBJECTAGG|JSON_ARRAYAGG|MAX|COUNT|SUM|MIN|AVG|last_string_value_without_retract))",
        std::regex_constants::icase);
    std::smatch match;
    if (std::regex_search(input, match, aggRegex)) {
        return match.str();
    } else {
        return "NONE";
    }
}

std::vector<std::string> GroupAggFunction::handleInputTypes()
{
    std::vector<std::string> types;
    for (const std::string& inputType : description["inputTypes"]) {
        types.push_back(inputType);
        auto typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
        if (typeId == DataTypeId::OMNI_INT) {
            equalisers.push_back(IntEqualiser);
        } else if (typeId == DataTypeId::OMNI_LONG) {
            equalisers.push_back(LongEqualiser);
        } else if (
            typeId == DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE ||
            typeId == DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            equalisers.push_back(TimestampEqualiser);
        } else {
            equalisers.push_back(nullptr);
            LOG("Warning equaliser for type " + inputType + " is not supported yet");
        }
    }
    return types;
}

namespace {
struct DistinctAggMeta {
    int filterIndex = -1;
    int argIndex = -1;
};

struct AggCallMeta {
    int aggFuncIndex = -1;
    std::string aggTypeStr;
    std::string aggregationFunction;
    std::string aggType;
    int filterIndex = -1;
    int aggIndex = -1;
    std::vector<int> argIndexes;
    std::string aggDataType;
    bool isDistinctCount = false;
    bool shouldDoRetract = false;
};
} // namespace

void GroupAggFunction::open(const Configuration& parameters)
{
    LOG("GroupAggFunction open() running");
    // Init HeapValueState
    omnistream::RowType accRowType(true, this->accTypes);
    auto accRowTypeInfo = InternalTypeInfo::ofRowType(&accRowType);
    std::string accStateName = "accState";
    ValueStateDescriptor<RowData*>* accDesc = new ValueStateDescriptor<RowData*>(accStateName, accRowTypeInfo);
    accDesc->SetStateSerializer(accRowTypeInfo->getTypeSerializer());

    // This kind of specific template type should all be solved by an if-else based on stateDescription
    accState = static_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext())->getState<RowData*>(accDesc);

#ifdef WITH_OMNISTATESTORE
    if (dynamic_cast<BssValueState<RowData*, VoidNamespace, RowData*>*>(accState)) {
        this->backend = 1;
    } else
#endif
    if (dynamic_cast<RocksdbValueState<RowData*, VoidNamespace, RowData*>*>(accState)) {
        this->backend = 2;
    }

    int accStartingIndex = 0;
    int aggValueIndex = 0;
    InitAggFunctions(accStartingIndex, aggValueIndex);

    // When the value of consumeRetraction of any function in aggregateCalls is true,
    // that is, the input stream may contain a pullback message,
    // indexOfCountStar will not be -1 ，which is mainly used for the COUNT(*) operation.
    // In this case, we initialize an extra count function.
    if (indexOfCountStar != -1) {
        auto* function = new CountFunction(-1, "BIGINT", -1, -1, -1);
        function->setCountStart(true);
        function->bindAccValueIndex(accStartingIndex, -1);
        functions.push_back(function);
        accStartingIndex += function->accumulatorSlots();
    }

    LOG("group agg accStartingIndex: " << accStartingIndex);
    LOG("group agg accumulatorArity: " << accumulatorArity);
    if (accStartingIndex != accumulatorArity) {
        throw std::runtime_error("GroupAggFunction open: accStartingIndex does not match accumulatorArity");
    }
    if (aggValueIndex != static_cast<int>(aggValueTypes.size())) {
        throw std::runtime_error("GroupAggFunction open: aggValueIndex does not match aggValueTypes size");
    }

    aggregateCallsCount = static_cast<int>(aggValueTypes.size());
    resultRow = new JoinedRowData();
    reUsePrevAggValue = BinaryRowData::createBinaryRowDataWithMem(aggregateCallsCount);
    LOG("init reUsePrevAggValue getArity : " << reUsePrevAggValue->getArity());
    reUseNewAggValue = BinaryRowData::createBinaryRowDataWithMem(aggregateCallsCount);
    sharedAccmulators = BinaryRowData::createBinaryRowDataWithMem(accTypes.size());
}

void GroupAggFunction::InitAggFunctions(int& accStartingIndex, int& aggValueIndex)
{
    functions.clear();
    std::vector<std::string> types = handleInputTypes();
    const auto& aggregateCalls = description["aggInfoList"]["aggregateCalls"];

    std::unordered_map<int, DistinctAggMeta> distinctInfoMap;
    if (distinctInfos.size() > 0) {
        LOG("GroupAggFunction open: processing distinctInfos with size " << distinctInfos.size());
        for (const DistinctInfo& info : distinctInfos) {
            for (size_t i = 0; i < info.aggIndexes.size(); i++) {
                int distinctCountAggFuncIndex = info.aggIndexes[i];

                DistinctAggMeta distinctAggMeta{info.filterArgs[i], info.argIndexes[0]};
                distinctInfoMap[distinctCountAggFuncIndex] = distinctAggMeta;
            }
        }
    }

    std::vector<AggCallMeta> sortedMetas;
    sortedMetas.reserve(aggregateCalls.size());
    for (size_t i = 0; i < aggregateCalls.size(); ++i) {
        const auto& aggCall = aggregateCalls[i];
        if (!aggCall.contains("name") || !aggCall["name"].is_string()) {
            throw std::runtime_error("GroupAggFunction InitAggFunctions: aggregateCalls missing string field 'name'.");
        }
        if (!aggCall.contains("filterArg") || !aggCall["filterArg"].is_number_integer()) {
            throw std::runtime_error(
                "GroupAggFunction InitAggFunctions: aggregateCalls missing integer field 'filterArg'.");
        }
        if (!aggCall.contains("argIndexes") || !aggCall["argIndexes"].is_array()) {
            throw std::runtime_error(
                "GroupAggFunction InitAggFunctions: aggregateCalls missing array field 'argIndexes'.");
        }

        AggCallMeta meta;
        meta.aggFuncIndex = static_cast<int>(i);
        meta.aggTypeStr = aggCall["name"].get<std::string>();
        meta.aggregationFunction =
            aggCall.contains("aggregationFunction") ? aggCall["aggregationFunction"].get<std::string>() : "";
        meta.aggType = extractAggFunction(meta.aggTypeStr);
        meta.filterIndex = aggCall["filterArg"].get<int>();

        const auto argIndexes = aggCall["argIndexes"].get<std::vector<int>>();
        meta.argIndexes = argIndexes;
        meta.aggIndex = argIndexes.empty() ? -1 : argIndexes[0];

        auto distinctInfoIt = distinctInfoMap.find(meta.aggFuncIndex);
        meta.isDistinctCount = (meta.aggType == "COUNT" && distinctInfoIt != distinctInfoMap.end());
        if (meta.isDistinctCount) {
            meta.filterIndex = distinctInfoIt->second.filterIndex;
        }

        meta.aggDataType = meta.aggIndex == -1 ? "BIGINT" : types[meta.aggIndex];
        meta.shouldDoRetract =
            meta.aggType == "SUM" && (meta.aggregationFunction.find("WithRetract") != std::string::npos);
        sortedMetas.emplace_back(std::move(meta));
    }

    std::unordered_map<int, AggsHandleFunction*> functionByAggFuncIndex;
    functionByAggFuncIndex.reserve(sortedMetas.size());
    SharedDistinctCountContainerFunction* sharedDistinctContainer = nullptr;
    bool distinctContainerInserted = false;

    for (const auto& meta : sortedMetas) {
        if (meta.isDistinctCount) {
            if (meta.aggIndex < 0) {
                throw std::runtime_error(
                    "GroupAggFunction InitAggFunctions: distinct COUNT requires a valid aggIndex.");
            }
            if (sharedDistinctContainer == nullptr) {
                sharedDistinctContainer = new SharedDistinctCountContainerFunction("distinctAcc");
            }
            sharedDistinctContainer->addDistinctEntry(
                meta.aggFuncIndex, meta.aggType, meta.aggIndex, meta.filterIndex, meta.aggDataType);
            if (!distinctContainerInserted) {
                functions.push_back(sharedDistinctContainer);
                distinctContainerInserted = true;
            }
            continue;
        }

        AggsHandleFunction* function = nullptr;
        if (meta.aggType == "AVG") {
            function = new AverageFunction(meta.aggIndex, meta.aggDataType, -1, -1, -1, meta.filterIndex);
        } else if (meta.aggType == "COUNT") {
            function = new CountFunction(meta.aggIndex, meta.aggDataType, -1, -1, meta.filterIndex);
        } else if (meta.aggType == "MAX") {
            function = new MinMaxFunction(meta.aggIndex, meta.aggDataType, -1, -1, MAX_FUNC, meta.filterIndex);
        } else if (meta.aggType == "MIN") {
            function = new MinMaxFunction(meta.aggIndex, meta.aggDataType, -1, -1, MIN_FUNC, meta.filterIndex);
        } else if (meta.aggType == "SUM") {
            auto* sumFunction = new SumFunction(meta.aggIndex, meta.aggDataType, -1, -1, meta.filterIndex);
            if (meta.shouldDoRetract) {
                sumFunction->setRetraction(0);
            }
            function = sumFunction;
        } else if (meta.aggType == "last_string_value_without_retract") {
            function = new LastStringValueFunction(meta.aggIndex, meta.aggDataType, -1, -1);
        } else if (meta.aggType == "JSON_OBJECTAGG") {
            // JSON_OBJECTAGG(KEY key VALUE value [ {NULL|ABSENT} ON NULL ]): two arguments (key, value).
            // Acc/value indices are bound later via bindAccValueIndex(); state lives in a keyed MapView,
            // so accumulatorSlots()==0 keeps accStartingIndex aligned with accumulatorArity.
            if (meta.argIndexes.size() < 2) {
                throw std::runtime_error("JSON_OBJECTAGG requires KEY and VALUE arguments: " + meta.aggTypeStr);
            }
            int keyIdx = meta.argIndexes[0];
            int valIdx = meta.argIndexes[1];
            // Default is NULL ON NULL; only an explicit "ABSENT ON NULL" clause drops null values.
            bool onNullAbsent = meta.aggTypeStr.find("ABSENT") != std::string::npos;
            auto* jsonObjFunction = new JsonObjectAggFunction(
                keyIdx, valIdx, types[keyIdx], types[valIdx], meta.aggFuncIndex, onNullAbsent, meta.filterIndex);
            jsonObjFunction->open(
                new PerKeyStateDataViewStore(dynamic_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext())));
            function = jsonObjFunction;
        } else if (meta.aggType == "JSON_ARRAYAGG") {
            // JSON_ARRAYAGG(items [ {NULL|ABSENT} ON NULL ]): single argument. See JSON_OBJECTAGG above for
            // the acc/value-index and MapView notes (accumulatorSlots()==0, indices bound later).
            if (meta.argIndexes.empty()) {
                throw std::runtime_error("JSON_ARRAYAGG requires an argument: " + meta.aggTypeStr);
            }
            int itemIdx = meta.argIndexes[0];
            // Planner names carry the ON NULL clause as a suffix: JSON_ARRAYAGG_ABSENT_ON_NULL (default)
            // vs JSON_ARRAYAGG_NULL_ON_NULL. Match on "ABSENT" (underscore-joined, not spaced).
            bool onNullAbsent = meta.aggTypeStr.find("ABSENT") != std::string::npos;
            auto* jsonArrFunction = new JsonArrayAggFunction(
                itemIdx, types[itemIdx], meta.aggFuncIndex, onNullAbsent, meta.filterIndex);
            jsonArrFunction->open(
                new PerKeyStateDataViewStore(dynamic_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext())));
            function = jsonArrFunction;
        } else {
            throw std::runtime_error("Unsupported aggregate type: " + meta.aggTypeStr);
        }

        functionByAggFuncIndex[meta.aggFuncIndex] = function;
        functions.push_back(function);
    }

    if (sharedDistinctContainer != nullptr) {
        sharedDistinctContainer->finalizeEntries();
        sharedDistinctContainer->open(
            new PerKeyStateDataViewStore(dynamic_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext())));
    }

    int nextAccIndex = accStartingIndex;
    int nextValueIndex = aggValueIndex;
    for (const auto& meta : sortedMetas) {
        if (meta.isDistinctCount) {
            sharedDistinctContainer->bindEntryAccValueIndex(meta.aggFuncIndex, nextAccIndex, nextValueIndex);
            nextAccIndex++;
            nextValueIndex++;
            continue;
        }

        auto funcIt = functionByAggFuncIndex.find(meta.aggFuncIndex);
        if (funcIt == functionByAggFuncIndex.end()) {
            throw std::runtime_error("GroupAggFunction InitAggFunctions: function binding lookup failed.");
        }
        AggsHandleFunction* function = funcIt->second;
        function->bindAccValueIndex(nextAccIndex, nextValueIndex);
        nextAccIndex += function->accumulatorSlots();
        if (function->hasAggOutput()) {
            nextValueIndex++;
        }
    }

    accStartingIndex = nextAccIndex;
    aggValueIndex = nextValueIndex;
}

JoinedRowData* GroupAggFunction::getResultRow()
{
    return resultRow;
}

void GroupAggFunction::processElement(RowData* input, Context* ctx, TimestampedCollector* out)
{
    bool firstRow;
    bool isEqual = true;
    RowData* currentKey = ctx->getCurrentKey();
    RowData* accumulators = accState->value();

    if (accumulators == nullptr) {
        // This is a new key
        if (!currentKey) {
            LOG("current key is nullptr");
            throw std::runtime_error("current key is nullptr");
        }
        RowData* updatedKey = currentKey->copy();
        ctx->setCurrentKey(updatedKey);
        currentKey = updatedKey;
        if (RowDataUtil::isRetractMsg(input->getRowKind())) {
            return;
        }
        firstRow = true;
        accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        auto binRowAcc = static_cast<BinaryRowData*>(accumulators);
        for (int i = 0; i < accumulatorArity; i++) {
            binRowAcc->setNullAt(i);
        }
        // Flink don't do update here, it updates it in if (!recordCounter->recordCountIsZero(accumulators)){} line 146
        accState->update(accumulators);
    } else {
        firstRow = false;
    }
    // set accumulators to handler first
    for (auto& func : functions) {
        func->setAccumulators(accumulators);
        func->setCurrentGroupKey(currentKey);
        func->setBackend(backend);
    }
    // get previous aggregate result
    for (auto& function : functions) {
        if (function->hasAggOutput()) {
            function->getValue(reUsePrevAggValue);
        }
        if (RowDataUtil::isAccumulateMsg(input->getRowKind())) {
            function->accumulate(input);
        } else {
            function->retract(input);
        }
        if (function->hasAggOutput()) {
            function->getValue(reUseNewAggValue);
        }
        function->getAccumulators(reinterpret_cast<BinaryRowData*>(accumulators));
    }

    if (!recordCounter->recordCountIsZero(accumulators)) {
        // Flink update accumulators in state here. But since we directly take the RowData* and updates in
        // getAccumulator, the value in statebackend is already updated!
        if (!firstRow) {
            for (auto& function : functions) {
                if (!function->hasAggOutput()) {
                    continue;
                }
                if (!function->equaliser(reUsePrevAggValue, reUseNewAggValue)) {
                    isEqual = false;
                    break;
                }
            }
            if (stateRetentionTime <= 0 && isEqual) {
                return;
            }

            if (generateUpdateBefore) {
                resultRow->replace(currentKey, reUsePrevAggValue)->setRowKind(RowKind::UPDATE_BEFORE);
                out->collect(resultRow);
            }

            resultRow->replace(currentKey, reUseNewAggValue)->setRowKind(RowKind::UPDATE_AFTER);
        } else {
            resultRow->replace(currentKey, reUseNewAggValue)->setRowKind(RowKind::INSERT);
        }
        out->collect(resultRow);
    } else {
        if (!firstRow) {
            resultRow->replace(currentKey, reUsePrevAggValue)->setRowKind(RowKind::DELETE);
            out->collect(resultRow);
        }
        accState->clear();
    }
}

void GroupAggFunction::processBatchColumnar(
    omnistream::VectorBatch* input, const std::vector<RowInfo>& groupInfo, RowData* accumulators)
{
    // Separate into accumulate and retract batches
    std::vector<int> accumulateIndices;
    std::vector<int> retractIndices;

    for (const RowInfo& info : groupInfo) {
        if (RowDataUtil::isAccumulateMsg(info.rowKind)) {
            accumulateIndices.push_back(info.rowIndex);
        } else {
            retractIndices.push_back(info.rowIndex);
        }
    }

    // Process all aggregate functions
    for (auto& function : functions) {
        if (function->hasAggOutput()) {
            function->getValue(reUsePrevAggValue);
        }

        // Batch accumulate
        if (!accumulateIndices.empty()) {
            function->accumulate(input, accumulateIndices);
        }
        // Batch retract
        if (!retractIndices.empty()) {
            function->retract(input, retractIndices);
        }
        if (function->hasAggOutput()) {
            function->getValue(reUseNewAggValue);
        }
        function->getAccumulators(reinterpret_cast<BinaryRowData*>(accumulators));
    }
}

void GroupAggFunction::processBatch(
    omnistream::VectorBatch* input,
    KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx,
    TimestampedCollector& out)
{
    auto rowCount = input->GetRowCount();
    if (rowCount < 0) {
        delete input;
        return;
    }

    GroupedRowsByKey keyToRowIndices;
    keyToRowIndices.reserve(keyToRowIndices.size());
    LOG("getEntireRow rowCount :" << rowCount);
    FillRowIndices(input, keyToRowIndices, rowCount);

    std::vector<RowData*> resultKeys;
    std::vector<RowData*> resultValues;
    std::vector<RowKind> resultRowKinds;
    for (auto& pair : keyToRowIndices) {
        bool isEqual = true;
        RowData* currentKey = pair.first;
        auto& groupedRows = pair.second;
        ctx.setCurrentKey(currentKey);

        RowData* accumulators = accState->value();
        bool firstRow = accumulators == nullptr;
        if (firstRow && !FirstRowAccumulate(groupedRows, accumulators)) {
            continue;
        }

        for (auto& func : functions) {
            func->setAccumulators(accumulators);
            func->setCurrentGroupKey(currentKey);
            func->setBackend(backend);
        }

        if (!groupedRows.empty()) {
            processBatchColumnar(input, groupedRows, accumulators);
        } else {
            // No rows left for this key after retract filtering.
            if (firstRow) {
                continue;
            }
        }
        LOG("functions loop aggregateCallsCount end");
        AssembleResultForBatch(accumulators, isEqual, firstRow, currentKey, resultKeys, resultValues, resultRowKinds);
    }

    if (backend == 2) {
        UpdateAccumulatorsInRocksDB(pendingUpdates);
        for (auto& pair : pendingUpdates) {
            // delete each current key's related accumulator
            delete pair.second;
        }
        pendingUpdates.clear();
        for (auto& func : functions) {
            func->updateInnerState();
        }
    }
    if (!resultKeys.empty()) {
        resultBatch = createOutputBatch(resultKeys, resultValues, resultRowKinds);
        collectOutputBatch(out, resultBatch);
    }

    delete input;
    for (auto& keyRowsPair : keyToRowIndices) {
        // delete each generate key from vb
        delete keyRowsPair.first;
    }
    keyToRowIndices.clear();

    resultKeys.clear();
    // delete the result RowData copied from reUseNewAggValue and reUsePreAggValue
    deleteRowData(resultValues);
    resultRowKinds.clear();
    LOG("GroupAggFunction processBatch end");
}

void GroupAggFunction::finish(
    KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx, TimestampedCollector& out)
{
    (void)ctx;
    (void)out;
}

void GroupAggFunction::deleteRowData(vector<RowData*>& rowVector)
{
    for (auto row : rowVector) {
        delete row;
    }
    rowVector.clear();
}

void GroupAggFunction::setInt(
    omniruntime::vec::VectorBatch* outputBatch, int numRows, int colIndex, std::vector<RowData*> vec)
{
    auto* vector = new omniruntime::vec::Vector<int32_t>(numRows);
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        if (vec[rowIndex]->isNullAt(colIndex)) {
            vector->SetNull(rowIndex);
        } else {
            vector->SetValue(rowIndex, *vec[rowIndex]->getInt(colIndex));
        }
    }
    outputBatch->Append(vector);
}

void GroupAggFunction::setLong(
    omniruntime::vec::VectorBatch* outputBatch, int numRows, int colIndex, std::vector<RowData*> vec)
{
    auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        if (vec[rowIndex]->isNullAt(colIndex)) {
            vector->SetNull(rowIndex);
        } else {
            vector->SetValue(rowIndex, *vec[rowIndex]->getLong(colIndex));
        }
    }
    outputBatch->Append(vector);
}

void GroupAggFunction::setString(
    omniruntime::vec::VectorBatch* outputBatch, int numRows, int colIndex, std::vector<RowData*> vec)
{
    auto* vector = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(numRows);
    for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
        if (vec[rowIndex]->isNullAt(colIndex)) {
            vector->SetNull(rowIndex);
        } else {
            std::string_view strView = vec[rowIndex]->getStringView(colIndex);
            vector->SetValue(rowIndex, strView);
        }
    }
    outputBatch->Append(vector);
}

omnistream::VectorBatch* GroupAggFunction::createOutputBatch(
    std::vector<RowData*> collectedKeys, std::vector<RowData*> collectedValues, std::vector<RowKind> rowKinds)
{
    int numColumns = outputTypes.size();
    auto* outputRowType = new std::vector<omniruntime::type::DataTypeId>;
    for (const auto& typeStr : outputTypes) {
        outputRowType->push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    int numRows = collectedKeys.size(); // Number of rows collected
    int keySize = collectedKeys[0]->getArity();
    // Create a new VectorBatch (empty if no rows exist)
    auto* outputBatch = new omnistream::VectorBatch(numRows);
    for (int colIndex = 0; colIndex < numColumns; colIndex++) {
        std::vector<RowData*> vec = colIndex < keySize ? collectedKeys : collectedValues;
        int offset = colIndex < keySize ? 0 : keySize;
        switch (outputRowType->at(colIndex)) {
            case DataTypeId::OMNI_LONG: {
                setLong(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP: {
                setLong(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            case DataTypeId::OMNI_INT: {
                setInt(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            case DataTypeId::OMNI_DOUBLE: {
                setLong(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            case DataTypeId::OMNI_BOOLEAN: {
                setInt(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                setString(outputBatch, numRows, colIndex - offset, vec);
                break;
            }
            default: {
                delete outputRowType;
                delete outputBatch;
                LOG("Unsupported column type in inputRow (createOutputBatch). colIndex : " << colIndex);
                throw std::runtime_error("Unsupported column type in inputRow");
            }
        }
    }

    // Set row kind for all rows (only if there are rows)
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        outputBatch->setRowKind(rowIndex, rowKinds[rowIndex]);
    }
    delete outputRowType;
    return outputBatch;
}

std::vector<int32_t> GroupAggFunction::getKeyedTypes(
    const std::vector<int32_t> keyedIndex, const std::vector<std::string> inputTypes)
{
    std::vector<int32_t> keyedTypes;
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    return keyedTypes;
}

void GroupAggFunction::collectOutputBatch(TimestampedCollector out, omnistream::VectorBatch* outputBatch)
{
    out.collect(outputBatch);
}

void GroupAggFunction::close()
{
}

ValueState<RowData*>* GroupAggFunction::getValueState()
{
    return accState;
}

bool GroupAggFunction::FirstRowAccumulate(std::vector<RowInfo>& groupInfo, RowData*& accumulators)
{
    auto entireIter = groupInfo.begin();
    while (entireIter != groupInfo.end()) {
        if (RowDataUtil::isRetractMsg(entireIter->rowKind)) {
            entireIter = groupInfo.erase(entireIter);
        } else {
            break;
        }
    }
    if (groupInfo.empty()) {
        return false;
    }
    accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    // If the accumulator does not exist, traverse functions to create the accumulators.
    for (auto& func : functions) {
        func->createAccumulators(dynamic_cast<BinaryRowData*>(accumulators));
    }
    // Flink don't do update here, it updates it in if (!recordCounter->recordCountIsZero(accumulators)){}
    // static_cast<HeapValueState<RowData*, VoidNamespace, RowData*> *>(accState)->update(accumulators);
    return true;
}

void GroupAggFunction::ClearEnv(
    omnistream::VectorBatch* input,
    std::vector<RowData*> resultKeys,
    std::vector<RowData*> resultValues,
    std::vector<RowKind> resultRowKinds,
    TimestampedCollector& out,
    std::unordered_map<RowData*, std::vector<RowInfo>> keyToRowIndices)
{
    delete input;
    if (!resultKeys.empty()) {
        resultBatch = createOutputBatch(resultKeys, resultValues, resultRowKinds);
        collectOutputBatch(out, resultBatch);

        // clear bundle
        for (auto& pair : keyToRowIndices) {
            delete pair.first;
        }
        keyToRowIndices.clear();
        // clear resultRows
        resultKeys.clear();
        deleteRowData(resultValues);
        resultRowKinds.clear();
    }
}

void GroupAggFunction::AssembleResultForBatch(
    RowData* accumulators,
    bool isEqual,
    bool firstRow,
    RowData* currentKey,
    std::vector<RowData*>& resultKeys,
    std::vector<RowData*>& resultValues,
    std::vector<RowKind>& resultRowKinds)
{
    if (!recordCounter->recordCountIsZero(accumulators)) {
        if (backend == 2) {
            pendingUpdates.emplace(currentKey, accumulators);
        } else {
            accState->update(accumulators);
        }
        // Flink update accumulators in state here. But since we directly take the RowData* and updates in
        // getAccumulator, the value in statebackend is already updated!
        if (!firstRow) {
            if (EndAssemble(isEqual)) {
                return;
            }
            if (generateUpdateBefore) {
                resultKeys.push_back(currentKey);
                resultValues.push_back(reUsePrevAggValue->copy());
                resultRowKinds.push_back(RowKind::UPDATE_BEFORE);
            }
            resultKeys.push_back(currentKey);
            resultValues.push_back(reUseNewAggValue->copy());
            resultRowKinds.push_back(RowKind::UPDATE_AFTER);
        } else {
            resultKeys.push_back(currentKey);
            resultValues.push_back(reUseNewAggValue->copy());
            resultRowKinds.push_back(RowKind::INSERT);
        }
    } else {
        if (!firstRow) {
            resultKeys.push_back(currentKey);
            resultValues.push_back(reUsePrevAggValue->copy());
            resultRowKinds.push_back(RowKind::DELETE);
        }
        accState->clear();
    }
}

void GroupAggFunction::FillRowIndices(
    omnistream::VectorBatch* input, std::unordered_map<RowData*, std::vector<RowInfo>>& keyToRowIndices, int rowCount)
{
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto key = groupByKeySelector->getKey(input, rowIndex);
        RowKind rowKind = input->getRowKind(rowIndex);
        auto it = keyToRowIndices.find(key);
        if (it != keyToRowIndices.end()) {
            it->second.push_back(RowInfo{rowIndex, rowKind});
            delete key;
        } else {
            keyToRowIndices[key] = {RowInfo{rowIndex, rowKind}};
        }
    }
}

void GroupAggFunction::AssembleResultForElement(
    RowData* accumulators, bool isEqual, bool firstRow, RowData* currentKey, TimestampedCollector& out)
{
    if (!recordCounter->recordCountIsZero(accumulators)) {
        // Flink update accumulators in state here. But since we directly take the RowData* and updates in
        // getAccumulator, the value in statebackend is already updated!
        if (!firstRow) {
            if (EndAssemble(isEqual)) {
                return;
            }
            if (generateUpdateBefore) {
                resultRow->replace(currentKey, reUsePrevAggValue)->setRowKind(RowKind::UPDATE_BEFORE);
                out.collect(resultRow);
            }

            resultRow->replace(currentKey, reUseNewAggValue)->setRowKind(RowKind::UPDATE_AFTER);
        } else {
            resultRow->replace(currentKey, reUseNewAggValue)->setRowKind(RowKind::INSERT);
        }
        out.collect(resultRow);
    } else {
        if (!firstRow) {
            resultRow->replace(currentKey, reUsePrevAggValue)->setRowKind(RowKind::DELETE);
            out.collect(resultRow);
        }
        accState->clear();
    }
}

bool GroupAggFunction::EndAssemble(bool isEqual)
{
    for (auto& function : functions) {
        if (!function->hasAggOutput()) {
            continue;
        }
        if (!function->equaliser(reUsePrevAggValue, reUseNewAggValue)) {
            isEqual = false;
            break;
        }
    }
    if (stateRetentionTime <= 0 && isEqual) {
        return true;
    }
    return false;
}

void GroupAggFunction::UpdateAccumulatorsInRocksDB(std::unordered_map<RowData*, RowData*>& pendingUpdates)
{
    accState->updateByBatch(pendingUpdates);
}
