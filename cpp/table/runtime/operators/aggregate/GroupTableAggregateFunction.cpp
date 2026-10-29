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
    uniqueKeyIndex = config.at("uniqueKeyIndex").get<int>();

    // Column types come from the input row type.
    const auto inputTypes = config.at("inputTypes").get<std::vector<std::string>>();
    if (sortKeyIndex < 0 || sortKeyIndex >= static_cast<int>(inputTypes.size())) {
        throw std::runtime_error("GroupTableAggregateFunction: sortFieldIndices out of range");
    }
    if (uniqueKeyIndex < 0 || uniqueKeyIndex >= static_cast<int>(inputTypes.size())) {
        throw std::runtime_error("GroupTableAggregateFunction: uniqueKeyIndex out of range");
    }
    sortKeyType = inputTypes[sortKeyIndex];
    uniqueKeyType = inputTypes[uniqueKeyIndex];

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
}

void GroupTableAggregateFunction::open(const Configuration& parameters)
{
    LOG("GroupTableAggregateFunction open() running");

    // The ROW_NUMBER function uses zero BinaryRowData accumulator slots (Scheme B), so the
    // accState row type is empty; the state exists purely for firstRow / group-death
    // bookkeeping (non-null value == group alive).
    std::vector<std::string> accTypes = {};
    omnistream::RowType accRowType(true, accTypes);
    auto accRowTypeInfo = InternalTypeInfo::ofRowType(&accRowType);
    auto* accDesc = new ValueStateDescriptor<RowData*>("accState", accRowTypeInfo);
    accDesc->SetStateSerializer(accRowTypeInfo->getTypeSerializer());
    accState =
        static_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext())->getState<RowData*>(accDesc);
    accumulatorArity = 0;

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
}

void GroupTableAggregateFunction::processElement(RowData* input, Context* ctx, TimestampedCollector* out)
{
    RowData* currentKey = ctx->getCurrentKey();

    bool firstRow = false;
    RowData* accumulators = accState->value();
    if (accumulators == nullptr) {
        // New group: a retract message for a group that never existed is dropped
        // (same policy as GroupAggFunction).
        if (RowDataUtil::isRetractMsg(input->getRowKind())) {
            return;
        }
        firstRow = true;
        if (currentKey != nullptr) {
            RowData* updatedKey = currentKey->copy();
            ctx->setCurrentKey(updatedKey);
            currentKey = updatedKey;
        }
        accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        accState->update(accumulators);
    }

    // 1. Load the current group's ledger (the function rebuilds its board from the MapView).
    function->setAccumulators(accumulators);

    // 2. Retract the OLD board first: still before accumulate/retract, the function sees
    //    the old ranking here (Flink GroupTableAggFunction emits UPDATE_BEFORE at this point).
    if (!firstRow && generateUpdateBefore) {
        function->emitValue(out, currentKey, true, false);
    }

    // 3. Update the ledger.
    if (RowDataUtil::isAccumulateMsg(input->getRowKind())) {
        function->accumulate(input);
    } else {
        function->retract(input);
    }

    // 4. Dead or alive: board empty == group death (Top-N semantics).
    if (!function->isGroupEmpty()) {
        // 5. Emit the NEW board: INSERT on first row, UPDATE_AFTER afterwards.
        function->emitValue(out, currentKey, false, firstRow);
        // accState unchanged (zero-slot accumulator): the group stays alive in state.
    } else {
        // Silent group death: no row is emitted -- the old board was already retracted in
        // step 2 (generateUpdateBefore), matching Flink's clear-without-emit behavior.
        accState->clear();
        function->cleanup();
    }
}

void GroupTableAggregateFunction::processBatch(
    omnistream::VectorBatch* inputBatch,
    KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx,
    TimestampedCollector& out)
{
    // v1 batch path: per-row delegation. KeyedProcessOperator hands the batch to the
    // function without setting a current key, so each row's key is extracted here via the
    // group-by selector (GroupAggFunction::FillRowIndices does the same, grouped).
    const auto rowCount = inputBatch->GetRowCount();
    if (rowCount < 0) {
        delete inputBatch;
        return;
    }
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        RowData* currentKey = groupByKeySelector->getKey(inputBatch, rowIndex);
        ctx.setCurrentKey(currentKey);
        RowData* row = inputBatch->extractRowData(rowIndex);
        processElement(row, &ctx, &out);
    }
}

void GroupTableAggregateFunction::close()
{
    if (function != nullptr) {
        function->close();
    }
}
