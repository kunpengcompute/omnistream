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

#include "GroupTableAggregateFunction.h"

#include <regex>

#include "core/include/common.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "table/types/logical/RowType.h"
#include "table/typeutils/InternalTypeInfo.h"

namespace {
// Copied from GroupAggFunction::getKeyedTypes: map grouping column indexes to omni type ids.
std::vector<int32_t> getKeyedTypes(
    const std::vector<int32_t>& keyedIndex,
    const std::vector<std::string>& inputTypes)
{
    std::vector<int32_t> keyedTypes;
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    return keyedTypes;
}
} // namespace

GroupTableAggregateFunction::GroupTableAggregateFunction(long stateRetentionTime, const nlohmann::json& config)
    : stateRetentionTime(stateRetentionTime),
      description(config)
{
    parseDescription(config);
}

GroupTableAggregateFunction::~GroupTableAggregateFunction()
{
    delete function;
    delete groupByKeySelector;
}

void GroupTableAggregateFunction::parseDescription(const nlohmann::json& config)
{
    generateUpdateBefore = config.value("generateUpdateBefore", false);

    // Sort field (v1: single BIGINT sort key, descending by default).
    const auto sortFieldIndices = config.at("sortFieldIndices").get<std::vector<int>>();
    if (sortFieldIndices.size() != 1) {
        throw std::runtime_error(
            "GroupTableAggregateFunction: v1 supports exactly one sort field, got "
            + std::to_string(sortFieldIndices.size()));
    }
    sortKeyIndex = sortFieldIndices[0];
    if (config.contains("sortAscendingOrders")) {
        const auto orders = config["sortAscendingOrders"].get<std::vector<bool>>();
        if (!orders.empty()) {
            sortAscending = orders[0];
        }
    }

    // Rank range, same "rankStart=1, rankEnd=2" text format as AbstractTopNFunction.
    const std::string rankRangeStr = config.at("rankRange").get<std::string>();
    std::regex pattern(R"(rankStart=(\d+),\s*rankEnd=(\d+))");
    std::smatch match;
    if (!std::regex_search(rankRangeStr, match, pattern)) {
        throw std::runtime_error("GroupTableAggregateFunction: cannot parse rankRange: " + rankRangeStr);
    }
    rankStart = std::stol(match[1].str());
    rankEnd = std::stol(match[2].str());
    if (rankStart != 1) {
        throw std::runtime_error(
            "GroupTableAggregateFunction: v1 requires rankStart == 1 (OFFSET Top-N falls back to Java), got "
            + std::to_string(rankStart));
    }

    // Unique key column: member identity for retract matching and idempotence.
    // Missing / -1 = keyless mode (plain Top2 TableAggregateFunction semantics: the
    // acceptance SQL input row is [grouping key, UDF argument] with no identity column).
    uniqueKeyIndex = config.value("uniqueKeyIndex", -1);

    // Column types come from the input row type.
    const auto inputTypes = config.at("inputTypes").get<std::vector<std::string>>();
    if (sortKeyIndex < 0 || sortKeyIndex >= static_cast<int>(inputTypes.size())) {
        throw std::runtime_error("GroupTableAggregateFunction: sortFieldIndices out of range");
    }
    if (uniqueKeyIndex >= static_cast<int>(inputTypes.size())) {
        throw std::runtime_error("GroupTableAggregateFunction: uniqueKeyIndex out of range");
    }
    sortKeyType = inputTypes[sortKeyIndex];
    uniqueKeyType = (uniqueKeyIndex == -1) ? "" : inputTypes[uniqueKeyIndex];

    // Optional FILTER on the aggregate call.
    if (config.contains("aggInfoList") && config["aggInfoList"].contains("aggregateCalls")) {
        const auto& calls = config["aggInfoList"]["aggregateCalls"];
        if (!calls.empty() && calls[0].contains("filterArg") && calls[0]["filterArg"].is_number_integer()) {
            filterIndex = calls[0]["filterArg"].get<int>();
        }
    }

    // Grouping keys (batch-path key extraction), same as GroupAggFunction.
    keyedIndex = config.at("grouping").get<std::vector<int32_t>>();
    keyedTypes = getKeyedTypes(keyedIndex, inputTypes);
    groupByKeySelector = new KeySelector<RowData*>(keyedTypes, keyedIndex);

    // Output row type [grouping key | value, rank] for the batch-path output batch.
    outputTypes = config.at("outputTypes").get<std::vector<std::string>>();
}

void GroupTableAggregateFunction::open(const Configuration& parameters)
{
    LOG("GroupTableAggregateFunction open() running");

    // Scheme A, Flink state-shape parity: accState holds the accumulator row
    // [first, second, ...] with rankEnd BIGINT slots -- the same ValueState<RowData>
    // that plain Flink's codegen'd Top2 handler persists (Top2Accum as a row), so
    // checkpoints/savepoints are engine-interoperable (Java job <-> native job).
    std::vector<std::string> accTypes(static_cast<size_t>(rankEnd), std::string("BIGINT"));
    omnistream::RowType accRowType(true, accTypes);
    auto accRowTypeInfo = InternalTypeInfo::ofRowType(&accRowType);
    auto* accDesc = new ValueStateDescriptor<RowData*>("accState", accRowTypeInfo);
    accDesc->SetStateSerializer(accRowTypeInfo->getTypeSerializer());
    accState =
        static_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext())->getState<RowData*>(accDesc);
    // Per-backend ownership of accState values (see member comment in the header):
    // under RocksDB with the falcon cache disabled (state-cache-sizeLimit == 0,
    // the default), value() returns a caller-owned heap copy on every call and
    // update() only serializes it -- the caller frees the object after the final
    // update (same contract as GroupAggFunction backend==2 pendingUpdates+delete).
    // Heap backend / falcon-enabled RocksDB keep ownership themselves.
    if (auto* rocksdbState = dynamic_cast<RocksdbValueState<RowData*, VoidNamespace, RowData*>*>(accState)) {
        stateValueCallerOwned_ = rocksdbState->stateCache->getSizeLimit() == 0;
    }
    accumulatorArity = static_cast<int>(rankEnd);

    // v1: single ROW_NUMBER aggregate call; multiple calls extend this into the
    // InitAggFunctions-style if-chain / factory.
    function = new RowNumberFunction(
        sortKeyIndex,
        sortKeyType,
        sortAscending,
        rankEnd,
        uniqueKeyIndex,
        uniqueKeyType,
        /*aggFuncIndex=*/0,
        filterIndex,
        generateUpdateBefore);
    function->open(
        new PerKeyStateDataViewStore(dynamic_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext())));
    // Concrete-type view for the batch path (emitValueToRows is a RowNumberFunction
    // extension, not part of the TableAggsHandleFunction contract).
    rowNumberFunction = static_cast<RowNumberFunction*>(function);
}

void GroupTableAggregateFunction::processElement(RowData* input, Context* ctx, TimestampedCollector* out)
{
    RowData* currentKey = ctx->getCurrentKey();

    // Top2 semantics: the UDF declares no retract, so retraction input is not
    // supported and is dropped silently (plain Flink's codegen would fail on a
    // retract call to a UDF without a retract() method; the Top2 whitelist plan
    // is append-only anyway).
    if (RowDataUtil::isRetractMsg(input->getRowKind())) {
        return;
    }

    bool firstRow = false;
    RowData* accumulators = accState->value();
    if (accumulators == nullptr) {
        firstRow = true;
        if (currentKey != nullptr) {
            RowData* updatedKey = currentKey->copy();
            ctx->setCurrentKey(updatedKey);
            currentKey = updatedKey;
        }
        accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        // Fresh group: initialize the [first, second, ...] slots to the MIN_VALUE
        // sentinel (Flink parity: function.createAccumulators()).
        function->createAccumulators(reinterpret_cast<BinaryRowData*>(accumulators));
        accState->update(accumulators);
    }

    // 1. Load this group's accumulator into the function (per-key switch and the
    //    checkpoint-recovery path both come through here).
    function->setAccumulators(accumulators);

    // 2. Retract the OLD board first: still before accumulate, the function sees
    //    the old ranking here (Flink GroupTableAggFunction emits UPDATE_BEFORE at this point).
    if (!firstRow && generateUpdateBefore) {
        function->emitValue(out, currentKey, true, false);
    }

    // 3. Accumulate the input row (append-only: retract was dropped above).
    function->accumulate(input);

    // 4. Persist the new accumulator (Flink parity: getAccumulators -> accState.update
    //    happens before the new board is emitted).
    function->getAccumulators(reinterpret_cast<BinaryRowData*>(accumulators));
    accState->update(accumulators);
    if (stateValueCallerOwned_) {
        delete accumulators; // falcon-disabled RocksDB: update() only serialized it
    }

    // 5. Emit the NEW board: INSERT on first row, UPDATE_AFTER afterwards. The Top2
    //    accumulator only grows, so the group never dies and accState is kept.
    function->emitValue(out, currentKey, false, firstRow);
}

void GroupTableAggregateFunction::processBatch(
    omnistream::VectorBatch* inputBatch,
    KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx,
    TimestampedCollector& out)
{
    // Native-chain batch path (GroupAggFunction pattern): the chained downstream
    // (e.g. StreamCalcBatch) consumes ONE VectorBatch per operator output. The former
    // per-row delegation (processElement -> emitValue -> row-at-a-time collect)
    // segfaulted in VectorBatch::RearrangeColumns on the first board emission, so all
    // board rows are collected here and delivered as a single assembled batch.
    const auto rowCount = inputBatch->GetRowCount();
    if (rowCount < 0) {
        delete inputBatch;
        return;
    }

    // One key RowData per input row (ownership stays here; resultKeys may repeat
    // the same pointer for the old board + new board of one input row).
    std::vector<RowData*> rowKeys;
    std::vector<RowData*> resultKeys;
    std::vector<RowData*> resultAggRows;
    std::vector<RowKind> resultRowKinds;
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        RowData* currentKey = groupByKeySelector->getKey(inputBatch, rowIndex);
        ctx.setCurrentKey(currentKey);
        RowData* row = inputBatch->extractRowData(rowIndex);
        processElementToRows(row, &ctx, rowKeys, resultKeys, resultAggRows, resultRowKinds);
    }

    if (!resultKeys.empty()) {
        omnistream::VectorBatch* outputBatch = createOutputBatch(resultKeys, resultAggRows, resultRowKinds);
        out.collect(outputBatch);
    }

    delete inputBatch;
    // The aggregate rows were copied into the output batch columns; free them.
    for (auto* aggRow : resultAggRows) {
        delete aggRow;
    }
    resultAggRows.clear();
    resultKeys.clear();
    resultRowKinds.clear();
    // Each input row's key RowData (exactly one allocation per row, tracked in
    // rowKeys; the firstRow copy replaces the selector allocation in place).
    for (auto* key : rowKeys) {
        delete key;
    }
    rowKeys.clear();
}

void GroupTableAggregateFunction::processElementToRows(
    RowData* input,
    Context* ctx,
    std::vector<RowData*>& rowKeys,
    std::vector<RowData*>& resultKeys,
    std::vector<RowData*>& resultAggRows,
    std::vector<RowKind>& resultRowKinds)
{
    RowData* currentKey = ctx->getCurrentKey();

    // Top2 semantics: the UDF declares no retract, so retraction input is not
    // supported and is dropped silently (plain Flink's codegen would fail on a
    // retract call to a UDF without a retract() method; the Top2 whitelist plan
    // is append-only anyway).
    if (RowDataUtil::isRetractMsg(input->getRowKind())) {
        rowKeys.push_back(currentKey); // still owned by this batch loop
        return;
    }

    bool firstRow = false;
    RowData* accumulators = accState->value();
    if (accumulators == nullptr) {
        firstRow = true;
        if (currentKey != nullptr) {
            RowData* updatedKey = currentKey->copy();
            ctx->setCurrentKey(updatedKey);
            delete currentKey; // key-selector allocation replaced by the defensive copy
            currentKey = updatedKey;
        }
        accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        // Fresh group: initialize the [first, second, ...] slots to the MIN_VALUE
        // sentinel (Flink parity: function.createAccumulators()).
        function->createAccumulators(reinterpret_cast<BinaryRowData*>(accumulators));
        accState->update(accumulators);
    }
    // Exactly one key RowData per input row is tracked for cleanup. On firstRow the
    // key-selector allocation was replaced by the defensive copy above; both cases
    // land here with the single live pointer.
    rowKeys.push_back(currentKey);

    // 1. Load this group's accumulator into the function (per-key switch and the
    //    checkpoint-recovery path both come through here).
    function->setAccumulators(accumulators);

    // 2. Retract the OLD board first: still before accumulate, the function sees
    //    the old ranking here (Flink GroupTableAggFunction emits UPDATE_BEFORE at this point).
    if (!firstRow && generateUpdateBefore) {
        const size_t before = resultAggRows.size();
        rowNumberFunction->emitValueToRows(resultAggRows, resultRowKinds, true, false);
        for (size_t i = before; i < resultAggRows.size(); ++i) {
            resultKeys.push_back(currentKey);
        }
    }

    // 3. Accumulate the input row (append-only: retract was dropped above).
    function->accumulate(input);

    // 4. Persist the new accumulator (Flink parity: getAccumulators -> accState.update
    //    happens before the new board is emitted).
    function->getAccumulators(reinterpret_cast<BinaryRowData*>(accumulators));
    accState->update(accumulators);
    if (stateValueCallerOwned_) {
        delete accumulators; // falcon-disabled RocksDB: update() only serialized it
    }

    // 5. Emit the NEW board: INSERT on first row, UPDATE_AFTER afterwards. The Top2
    //    accumulator only grows, so the group never dies and accState is kept.
    const size_t before = resultAggRows.size();
    rowNumberFunction->emitValueToRows(resultAggRows, resultRowKinds, false, firstRow);
    for (size_t i = before; i < resultAggRows.size(); ++i) {
        resultKeys.push_back(currentKey);
    }
}

omnistream::VectorBatch* GroupTableAggregateFunction::createOutputBatch(
    const std::vector<RowData*>& resultKeys,
    const std::vector<RowData*>& resultAggRows,
    const std::vector<RowKind>& resultRowKinds)
{
    // Column layout: [grouping key columns | value, rank]. Key columns read from the
    // key RowData (arity = grouping size), aggregate columns from the emitted
    // [value, rn] GenericRowData. All v1 types (BIGINT/BIGINT/INT) are int64-backed,
    // matching GroupAggFunction's setLong/setInt which both use Vector<int64_t>.
    const int keySize = static_cast<int>(keyedIndex.size());
    const int numColumns = static_cast<int>(outputTypes.size());
    const int numRows = static_cast<int>(resultKeys.size());

    auto* outputBatch = new omnistream::VectorBatch(numRows);
    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (colIndex < keySize) {
                vector->SetValue(rowIndex, *(resultKeys[rowIndex]->getLong(colIndex)));
            } else {
                vector->SetValue(rowIndex, *(resultAggRows[rowIndex]->getLong(colIndex - keySize)));
            }
        }
        outputBatch->Append(vector);
    }

    // RowKind rides on the batch rows (GroupAggFunction::createOutputBatch L671).
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        outputBatch->setRowKind(rowIndex, resultRowKinds[rowIndex]);
    }
    return outputBatch;
}

void GroupTableAggregateFunction::close()
{
    if (function != nullptr) {
        function->close();
    }
}
