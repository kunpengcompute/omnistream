/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "ProcTimeUnboundedPrecedingFunction.h"

#include <regex>
#include <utility>

#include "runtime/generated/function/CountFunction.h"
#include "runtime/generated/function/MinMaxFunction.h"
#include "runtime/generated/function/SumFunction.h"
#include "table/runtime/generated/GeneratedAggsHandleFunctionMinMax.h"

namespace {

const char* const PROCESS_FUNCTION_NAME = "ProcTimeUnboundedPrecedingFunction";

// Same shape as GroupAggFunction::extractAggFunction, but restricted to the
// v1 over-aggregate whitelist: SUM / COUNT / MAX / MIN.
std::string extractAggName(const std::string& input)
{
    std::regex aggRegex(R"((MAX|COUNT|SUM|MIN))", std::regex_constants::icase);
    std::smatch match;
    if (std::regex_search(input, match, aggRegex)) {
        return match.str();
    }
    return "NONE";
}

struct OverAggCallMeta {
    int aggFuncIndex = -1;
    std::string aggTypeStr;
    std::string aggType;
    int filterIndex = -1;
    int aggIndex = -1; // -1: COUNT(*)
    std::vector<int> argIndexes;
    std::string aggDataType;
};

} // namespace

ProcTimeUnboundedPrecedingFunction::ProcTimeUnboundedPrecedingFunction(
    long stateRetentionTime, const nlohmann::json& config)
    : stateRetentionTime(stateRetentionTime)
{
    parseDescription(config);

    accumulatorArity = static_cast<int>(accTypes.size());
    aggValueArity = static_cast<int>(aggValueTypes.size());

    // Partition key selector over the grouping columns (empty grouping = one
    // global partition; the selector then produces an empty key row).
    std::vector<int32_t> keyedTypes;
    for (int index : groupingIndices) {
        if (index < 0 || index >= static_cast<int>(inputTypes.size())) {
            throw std::runtime_error(
                "ProcTimeUnboundedPrecedingFunction: grouping index out of range: " + std::to_string(index));
        }
        keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
    }
    groupByKeySelector = new KeySelector<RowData*>(keyedTypes, groupingIndices);
}

ProcTimeUnboundedPrecedingFunction::~ProcTimeUnboundedPrecedingFunction()
{
    for (auto* func : functions) {
        delete func;
    }
    for (auto* row : collectedRows) {
        delete row;
    }
    for (auto* value : collectedAggValues) {
        delete value;
    }
    delete groupByKeySelector;
    delete resultRow;
    delete reuseAggValue;
}

void ProcTimeUnboundedPrecedingFunction::parseDescription(const nlohmann::json& config)
{
    // Defense in depth: the factory only routes here for this very name; a
    // mismatching description must fail fast instead of mis-executing.
    if (!config.contains("processFunction") ||
        config["processFunction"].get<std::string>() != PROCESS_FUNCTION_NAME) {
        throw std::runtime_error(
            "ProcTimeUnboundedPrecedingFunction: description processFunction mismatch: " + config.dump());
    }

    if (!config.contains("inputTypes") || !config["inputTypes"].is_array() || config["inputTypes"].empty()) {
        throw std::runtime_error("ProcTimeUnboundedPrecedingFunction: missing or empty inputTypes");
    }
    inputTypes = config["inputTypes"].get<std::vector<std::string>>();
    for (const auto& typeStr : inputTypes) {
        inputRowTypes.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    if (!config.contains("outputTypes") || !config["outputTypes"].is_array()) {
        throw std::runtime_error("ProcTimeUnboundedPrecedingFunction: missing outputTypes");
    }
    outputTypes = config["outputTypes"].get<std::vector<std::string>>();

    // grouping may be an empty array (OVER () = single global partition).
    if (!config.contains("grouping") || !config["grouping"].is_array()) {
        throw std::runtime_error("ProcTimeUnboundedPrecedingFunction: missing or invalid grouping");
    }
    groupingIndices = config["grouping"].get<std::vector<int>>();

    if (!config.contains("aggInfoList") || !config["aggInfoList"].contains("aggregateCalls") ||
        !config["aggInfoList"]["aggregateCalls"].is_array() ||
        config["aggInfoList"]["aggregateCalls"].empty()) {
        throw std::runtime_error("ProcTimeUnboundedPrecedingFunction: missing or empty aggInfoList.aggregateCalls");
    }
    aggInfoList = config["aggInfoList"];
    accTypes = aggInfoList["accTypes"].get<std::vector<std::string>>();
    aggValueTypes = aggInfoList["aggValueTypes"].get<std::vector<std::string>>();

    // Over output row = (all input columns || one column per aggregate value).
    if (outputTypes.size() != inputTypes.size() + aggValueTypes.size()) {
        throw std::runtime_error(
            "ProcTimeUnboundedPrecedingFunction: outputTypes arity mismatch (expected input + aggValueTypes)");
    }

    // Whitelist check happens here as the last line of defence; the real gate
    // is the OmniAdaptor Validate strategy, which keeps non-whitelisted plans
    // on the Java path so this throw is never reached in production.
    for (const auto& aggCall : aggInfoList["aggregateCalls"]) {
        const std::string name = aggCall["name"].get<std::string>();
        const std::string aggType = extractAggName(name);
        if (aggType == "NONE") {
            throw std::runtime_error("ProcTimeUnboundedPrecedingFunction: unsupported aggregate: " + name);
        }
    }
}

void ProcTimeUnboundedPrecedingFunction::initAggFunctions(int& accStartingIndex, int& aggValueIndex)
{
    functions.clear();

    std::vector<OverAggCallMeta> metas;
    const auto& aggregateCalls = aggInfoList["aggregateCalls"];
    metas.reserve(aggregateCalls.size());
    for (size_t i = 0; i < aggregateCalls.size(); ++i) {
        const auto& aggCall = aggregateCalls[i];
        if (!aggCall.contains("name") || !aggCall["name"].is_string()) {
            throw std::runtime_error("ProcTimeUnboundedPrecedingFunction: aggregateCalls missing 'name'");
        }
        if (!aggCall.contains("filterArg") || !aggCall["filterArg"].is_number_integer()) {
            throw std::runtime_error("ProcTimeUnboundedPrecedingFunction: aggregateCalls missing 'filterArg'");
        }
        if (!aggCall.contains("argIndexes") || !aggCall["argIndexes"].is_array()) {
            throw std::runtime_error("ProcTimeUnboundedPrecedingFunction: aggregateCalls missing 'argIndexes'");
        }

        OverAggCallMeta meta;
        meta.aggFuncIndex = static_cast<int>(i);
        meta.aggTypeStr = aggCall["name"].get<std::string>();
        meta.aggType = extractAggName(meta.aggTypeStr);
        meta.filterIndex = aggCall["filterArg"].get<int>();
        meta.argIndexes = aggCall["argIndexes"].get<std::vector<int>>();
        meta.aggIndex = meta.argIndexes.empty() ? -1 : meta.argIndexes[0];

        if (meta.aggType == "COUNT") {
            if (meta.argIndexes.size() > 1) {
                throw std::runtime_error("ProcTimeUnboundedPrecedingFunction: COUNT takes at most one argument");
            }
        } else { // SUM / MAX / MIN
            if (meta.argIndexes.size() != 1) {
                throw std::runtime_error(
                    "ProcTimeUnboundedPrecedingFunction: " + meta.aggType + " requires exactly one argument");
            }
        }
        meta.aggDataType = meta.aggIndex == -1 ? "BIGINT" : inputTypes[meta.aggIndex];
        metas.emplace_back(std::move(meta));
    }

    for (const auto& meta : metas) {
        AggsHandleFunction* function = nullptr;
        if (meta.aggType == "COUNT") {
            function = new CountFunction(meta.aggIndex, meta.aggDataType, -1, -1, meta.filterIndex);
        } else if (meta.aggType == "MAX") {
            function = new MinMaxFunction(meta.aggIndex, meta.aggDataType, -1, -1, MAX_FUNC, meta.filterIndex);
        } else if (meta.aggType == "MIN") {
            function = new MinMaxFunction(meta.aggIndex, meta.aggDataType, -1, -1, MIN_FUNC, meta.filterIndex);
        } else if (meta.aggType == "SUM") {
            // No WithRetract variants in the v1 over whitelist: the frame never
            // calls retract, so the plain accumulator layout is used.
            function = new SumFunction(meta.aggIndex, meta.aggDataType, -1, -1, meta.filterIndex);
        } else {
            throw std::runtime_error("ProcTimeUnboundedPrecedingFunction: unsupported aggregate: " + meta.aggTypeStr);
        }
        functions.push_back(function);
    }

    // Slot assignment (mirrors GroupAggFunction::InitAggFunctions): every
    // function occupies accumulatorSlots() slots in the shared accumulator row
    // and, when it produces an output value, one slot in the value row.
    int nextAccIndex = accStartingIndex;
    int nextValueIndex = aggValueIndex;
    for (const auto& meta : metas) {
        AggsHandleFunction* function = functions[meta.aggFuncIndex];
        function->bindAccValueIndex(nextAccIndex, nextValueIndex);
        nextAccIndex += function->accumulatorSlots();
        if (function->hasAggOutput()) {
            nextValueIndex++;
        }
    }

    accStartingIndex = nextAccIndex;
    aggValueIndex = nextValueIndex;
}

void ProcTimeUnboundedPrecedingFunction::open(const Configuration& parameters)
{
    LOG("ProcTimeUnboundedPrecedingFunction open() running");

    omnistream::RowType accRowType(true, this->accTypes);
    auto accRowTypeInfo = InternalTypeInfo::ofRowType(&accRowType);
    // State name and layout mirror the Java operator exactly (ValueState<RowData>
    // "accState" holding the accumulator row), keeping the native state backend
    // structurally equivalent to native Flink. No batch/columnar data may ever
    // be written into the state backend.
    auto* accDesc = new ValueStateDescriptor<RowData*>("accState", accRowTypeInfo);
    accDesc->SetStateSerializer(accRowTypeInfo->getTypeSerializer());
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
    initAggFunctions(accStartingIndex, aggValueIndex);

    if (accStartingIndex != accumulatorArity) {
        throw std::runtime_error(
            "ProcTimeUnboundedPrecedingFunction open: acc slot count " + std::to_string(accStartingIndex) +
            " != accTypes arity " + std::to_string(accumulatorArity));
    }
    if (aggValueIndex != aggValueArity) {
        throw std::runtime_error(
            "ProcTimeUnboundedPrecedingFunction open: aggValue slot count " + std::to_string(aggValueIndex) +
            " != aggValueTypes arity " + std::to_string(aggValueArity));
    }

    resultRow = new JoinedRowData();
    reuseAggValue = BinaryRowData::createBinaryRowDataWithMem(aggValueArity);
}

void ProcTimeUnboundedPrecedingFunction::processRowForKey(RowData* input, RowData* key)
{
    // 1. fetch (or create) the key's accumulator
    RowData* accumulators = accState->value();
    if (accumulators == nullptr) {
        accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        for (auto* func : functions) {
            func->createAccumulators(static_cast<BinaryRowData*>(accumulators));
        }
        accState->update(accumulators);
    }

    for (auto* func : functions) {
        func->setAccumulators(accumulators);
        func->setCurrentGroupKey(key);
        func->setBackend(backend);
    }

    // 2. accumulate unconditionally: the Java counterpart does not branch on
    //    the input RowKind, so neither do we (byte-identical behaviour first).
    for (auto* func : functions) {
        func->accumulate(input);
        func->getAccumulators(static_cast<BinaryRowData*>(accumulators));
    }

    // 3. per-row state write-back (Java does accState.update on every row;
    //    mandatory for the RocksDB backend, harmless for heap).
    accState->update(accumulators);

    // 4. snapshot the aggregate values for this row
    materialiseAggValue();

    // 5. queue (input || aggValue) as an INSERT row
    collectedRows.push_back(input);
    collectedAggValues.push_back(reuseAggValue->copy());
    collectedRowKinds.push_back(RowKind::INSERT);
}

void ProcTimeUnboundedPrecedingFunction::materialiseAggValue()
{
    for (auto* func : functions) {
        if (func->hasAggOutput()) {
            func->getValue(reuseAggValue);
        }
    }
}

RowData* ProcTimeUnboundedPrecedingFunction::materialiseRow(omnistream::VectorBatch* batch, int rowId)
{
    // Same pattern as OutputConversionOperator::getEntireRow: materialise one
    // owned BinaryRowData from the columnar batch. v1 test matrix uses
    // non-null BIGINT keys/arguments (Nexmark), null columns in the input are
    // not modelled here yet.
    int colsCount = batch->GetVectorCount();
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(colsCount);
    for (int32_t i = 0; i < colsCount; i++) {
        const std::string& typeStr = inputTypes[i];
        if (typeStr == "BIGINT" || typeStr.find("TIMESTAMP") != std::string::npos) {
            row->setLong(
                i,
                reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(i))->GetValue(rowId));
        } else if (typeStr == "INTEGER") {
            row->setInt(
                i, reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(i))->GetValue(rowId));
        } else if (typeStr == "DOUBLE") {
            row->setLong(
                i, reinterpret_cast<omniruntime::vec::Vector<double>*>(batch->Get(i))->GetValue(rowId));
        } else if (typeStr == "BOOLEAN") {
            row->setInt(
                i, reinterpret_cast<omniruntime::vec::Vector<bool>*>(batch->Get(i))->GetValue(rowId));
        } else if (
            typeStr == "STRING" || typeStr == "VARCHAR" ||
            typeStr == "VARCHAR(2147483647)") {
            auto str = reinterpret_cast<
                omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(i))
                              ->GetValue(rowId);
            auto u32Str = std::make_unique<std::u32string>(str.begin(), str.end());
            row->setString(i, new BinaryStringData(u32Str.get()));
        } else {
            delete row;
            throw std::runtime_error(
                "ProcTimeUnboundedPrecedingFunction: unsupported input column type: " + typeStr);
        }
    }
    row->setRowKind(batch->getRowKind(rowId));
    return row;
}

void ProcTimeUnboundedPrecedingFunction::processElement(
    RowData* input, Context* ctx, TimestampedCollector* out)
{
    // Single-row entry point (unit tests). The production path is processBatch.
    // Copy the input row so the collected rows are always owned by us.
    RowData* owned = input->copy();
    processRowForKey(owned, ctx->getCurrentKey());
    flushCollected(*out);
}

void ProcTimeUnboundedPrecedingFunction::processBatch(
    omnistream::VectorBatch* inputBatch,
    KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx,
    TimestampedCollector& out)
{
    auto rowCount = inputBatch->GetRowCount();
    if (rowCount < 0) {
        delete inputBatch;
        return;
    }

    // v1: row-at-a-time over the batch (correctness first; columnar
    // accumulate could come later). Rows are processed in arrival order so
    // the prefix aggregation matches Java exactly.
    for (int i = 0; i < rowCount; ++i) {
        RowData* key = groupByKeySelector->getKey(inputBatch, i);
        ctx.setCurrentKey(key);
        RowData* row = materialiseRow(inputBatch, i);
        processRowForKey(row, key);
        delete key;
    }

    flushCollected(out);
    delete inputBatch;
}

void ProcTimeUnboundedPrecedingFunction::flushCollected(TimestampedCollector& out)
{
    if (collectedRows.empty()) {
        return;
    }
    omnistream::VectorBatch* outputBatch = createOutputBatch();
    collectOutputBatch(out, outputBatch);
}

omnistream::VectorBatch* ProcTimeUnboundedPrecedingFunction::createOutputBatch()
{
    int numRows = static_cast<int>(collectedRows.size());
    int inputArity = collectedRows[0]->getArity();
    auto* outputBatch = new omnistream::VectorBatch(numRows);

    for (int colIndex = 0; colIndex < static_cast<int>(outputTypes.size()); ++colIndex) {
        // Output row = (input row || aggValue snapshot): the first inputArity
        // columns are read from the collected input rows, the rest from the
        // per-row aggregate-value snapshots (GroupAggFunction::createOutputBatch
        // uses the same two-segment scheme for key||value).
        bool isValueCol = colIndex >= inputArity;
        int srcCol = isValueCol ? colIndex - inputArity : colIndex;
        std::vector<RowData*>& src = isValueCol ? collectedAggValues : collectedRows;

        switch (LogicalType::flinkTypeToOmniTypeId(outputTypes[colIndex])) {
            case DataTypeId::OMNI_LONG:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP: {
                auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (src[rowIndex]->isNullAt(srcCol)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *src[rowIndex]->getLong(srcCol));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_INT: {
                auto* vector = new omniruntime::vec::Vector<int32_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (src[rowIndex]->isNullAt(srcCol)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *src[rowIndex]->getInt(srcCol));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_DOUBLE: {
                auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (src[rowIndex]->isNullAt(srcCol)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *src[rowIndex]->getLong(srcCol));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_BOOLEAN: {
                auto* vector = new omniruntime::vec::Vector<bool>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (src[rowIndex]->isNullAt(srcCol)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *src[rowIndex]->getInt(srcCol));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                using VarcharVector =
                    omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
                auto* vector = new VarcharVector(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (src[rowIndex]->isNullAt(srcCol)) {
                        vector->SetNull(rowIndex);
                    } else {
                        auto value = std::string(src[rowIndex]->getStringView(srcCol));
                        omniruntime::vec::VectorHelper::VectorSetValue<DataTypeId::OMNI_VARCHAR>(
                            vector, rowIndex, (void*)&value);
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            default: {
                delete outputBatch;
                throw std::runtime_error(
                    "ProcTimeUnboundedPrecedingFunction: unsupported output column type: " + outputTypes[colIndex]);
            }
        }
    }

    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        outputBatch->setRowKind(rowIndex, collectedRowKinds[rowIndex]);
    }

    for (auto* row : collectedRows) {
        delete row;
    }
    collectedRows.clear();
    for (auto* value : collectedAggValues) {
        delete value;
    }
    collectedAggValues.clear();
    collectedRowKinds.clear();
    return outputBatch;
}

void ProcTimeUnboundedPrecedingFunction::collectOutputBatch(
    TimestampedCollector& out, omnistream::VectorBatch* outputBatch)
{
    out.collect(outputBatch);
}

JoinedRowData* ProcTimeUnboundedPrecedingFunction::getResultRow()
{
    return resultRow;
}

ValueState<RowData*>* ProcTimeUnboundedPrecedingFunction::getValueState()
{
    return accState;
}

void ProcTimeUnboundedPrecedingFunction::close()
{
}
