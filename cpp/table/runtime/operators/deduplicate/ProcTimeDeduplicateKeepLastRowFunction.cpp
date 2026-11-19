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

#include "ProcTimeDeduplicateKeepLastRowFunction.h"

#include "core/api/common/state/ValueStateDescriptor.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/runtime/operators/deduplicate/RowTimeDeduplicateFunction.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "table/types/logical/LogicalType.h"

using namespace omniruntime::type;
using namespace omniruntime::vec;

ProcTimeDeduplicateKeepLastRowFunction::ProcTimeDeduplicateKeepLastRowFunction(const nlohmann::json& config)
    : inputTypes(config.at("inputTypes").get<std::vector<std::string>>()),
      generateUpdateBefore(config.at("generateUpdateBefore").get<bool>()),
      inputInsertOnly(config.value("inputInsertOnly", false)),
      generateInsert(config.value("generateInsert", true))
{
    for (const auto& type : inputTypes) {
        types.push_back(static_cast<DataTypeId>(LogicalType::flinkTypeToOmniTypeId(type)));
    }
    const auto grouping = config.at("grouping").get<std::vector<int32_t>>();
    keySelector = KeySelector<RowData*>(RowTimeDeduplicateFunction::getKeyedTypes(grouping, inputTypes), grouping);
}

void ProcTimeDeduplicateKeepLastRowFunction::open(const Configuration&)
{
    auto* runtimeContext = static_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext());
    auto* rowType = InternalTypeInfo::ofRowType(new omnistream::RowType(true, inputTypes));
    auto* descriptor = new ValueStateDescriptor<RowData*>("deduplicate-state", rowType);
    descriptor->SetStateSerializer(rowType->getTypeSerializer());
    state = runtimeContext->getState<RowData*>(descriptor);
    heapState = runtimeContext->getStateType() == omnistream::StateType::HEAP;
    rocksdbState = dynamic_cast<RocksdbValueState<RowData*, VoidNamespace, RowData*>*>(state);
    if (!heapState && rocksdbState == nullptr) {
        throw std::logic_error("ProcTimeDeduplicateKeepLastRowFunction requires Heap or RocksDB state");
    }
}

namespace {
struct Change {
    std::unique_ptr<RowData> row;
    RowKind kind;
    int64_t timestamp;
};

void collectChanges(
    const std::vector<Change>& changes,
    const std::vector<omniruntime::type::DataTypeId>& types,
    const std::vector<std::string>& inputTypes,
    TimestampedCollector& out)
{
    if (changes.empty()) {
        return;
    }
    std::unique_ptr<omnistream::VectorBatch> result(omnistream::VectorBatch::CreateVectorBatch(changes.size(), types));
    for (size_t row = 0; row < changes.size(); ++row) {
        const auto& change = changes[row];
        auto* data = static_cast<BinaryRowData*>(change.row.get());
        for (size_t column = 0; column < types.size(); ++column) {
            auto* vector = result->Get(column);
            size_t type = static_cast<size_t>(vector->GetTypeId());
            // TIME/TIMESTAMP variants are stored as int64 millis and share the OMNI_LONG
            // converter. The output vector may report the concrete logical type id (which has no
            // entry in rowDeserializerCenter) even though its payload is an OMNI_LONG; normalize
            // to OMNI_LONG here, mirroring VectorBatch::extractRowData and KeySelector.
            const auto logicalType = static_cast<DataTypeId>(type);
            if (logicalType == OMNI_TIME_WITHOUT_TIME_ZONE || logicalType == OMNI_TIMESTAMP_WITHOUT_TIME_ZONE ||
                logicalType == OMNI_TIMESTAMP_WITH_TIME_ZONE || logicalType == OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                type = static_cast<size_t>(OMNI_LONG);
            }
            if (type >= rowDeserializerCenter.size() || rowDeserializerCenter[type] == nullptr) {
                throw std::runtime_error(
                    "ProcTimeDeduplicateKeepLastRowFunction has no output converter for column " +
                    std::to_string(column) + " (" + inputTypes[column] + ")");
            }
            rowDeserializerCenter[type](vector, row, data, column);
        }
        result->setRowKind(row, change.kind);
        result->setTimestamp(row, change.timestamp);
    }
    out.collect(result.release());
}
} // namespace

void ProcTimeDeduplicateKeepLastRowFunction::processBatch(
    omnistream::VectorBatch* input, Context& ctx, TimestampedCollector& out)
{
    std::unique_ptr<omnistream::VectorBatch> inputGuard(input);
    // inputInsertOnly 区分 Flink 的两个语义分支：ChangelogNormalize（false）走
    // processLastRowOnChangelog，append-only proc-time Deduplicate（true）走
    // processLastRowOnProcTime，与 Flink ProcTimeDeduplicateKeepLastRowFunction 完全对齐。
    if (inputInsertOnly) {
        processBatchInsertOnly(inputGuard.get(), ctx, out);
    } else {
        processBatchChangelog(inputGuard.get(), ctx, out);
    }
}

void ProcTimeDeduplicateKeepLastRowFunction::processBatchChangelog(
    omnistream::VectorBatch* input, Context& ctx, TimestampedCollector& out)
{
    std::vector<Change> changes;
    changes.reserve(input->GetRowCount());
    for (int i = 0; i < input->GetRowCount(); ++i) {
        std::unique_ptr<RowData> key(keySelector.getKey(input, i));
        ctx.setCurrentKey(key.get());
        const bool cacheHit =
            rocksdbState != nullptr && rocksdbState->stateCache->contains(key.get(), rocksdbState->getNamespace());
        RowData* previous = state->value();
        // RocksDB misses return an owned row, while heap and cache hits borrow state.
        std::unique_ptr<RowData> ownedPrevious(rocksdbState != nullptr && !cacheHit ? previous : nullptr);
        const auto kind = input->getRowKind(i);
        const auto timestamp = input->getTimestamp(i);
        if (kind == RowKind::DELETE || kind == RowKind::UPDATE_BEFORE) {
            if (previous != nullptr) {
                changes.push_back({std::unique_ptr<RowData>(previous->copy()), RowKind::DELETE, timestamp});
                state->clear();
            }
            continue;
        }

        std::unique_ptr<RowData> current(input->extractRowData(i));
        const bool firstRow = previous == nullptr;
        // BinaryRowData::operator== memcmp's the whole binary row, including the rowKind byte.
        // This matches Flink's generated RecordEqualiser: for BinaryRowData it uses
        // left.equals(right) (memcmp incl. rowKind), and the generic path first compares
        // getRowKind(). State rows are always stored normalized to INSERT, so only a same-content
        // INSERT input is suppressed; a same-content UPDATE_AFTER (INSERT state vs UA input) is not
        // equal and is forwarded, matching DeduplicateFunctionHelper.processLastRowOnChangelog
        // with state TTL disabled.
        if (!firstRow && *previous == *current) {
            continue;
        }
        if (!firstRow && generateUpdateBefore) {
            changes.push_back({std::unique_ptr<RowData>(previous->copy()), RowKind::UPDATE_BEFORE, timestamp});
        }
        changes.push_back(
            {std::unique_ptr<RowData>(current->copy()), firstRow ? RowKind::INSERT : RowKind::UPDATE_AFTER, timestamp});
        current->setRowKind(RowKind::INSERT);
        if (heapState && !firstRow) {
            state->clear();
        }
        state->update(current.get(), false);
        if (heapState) {
            current.release();
        }
    }

    collectChanges(changes, types, inputTypes, out);
}

void ProcTimeDeduplicateKeepLastRowFunction::processBatchInsertOnly(
    omnistream::VectorBatch* input, Context& ctx, TimestampedCollector& out)
{
    // Flink DeduplicateFunctionHelper.processLastRowOnProcTime（DeduplicateFunctionHelper.java:42-83）。
    const bool stateful = generateUpdateBefore || generateInsert;
    std::vector<Change> changes;
    changes.reserve(input->GetRowCount());
    for (int i = 0; i < input->GetRowCount(); ++i) {
        const auto timestamp = input->getTimestamp(i);
        if (input->getRowKind(i) != RowKind::INSERT) {
            // Flink 的 checkInsertOnly：proc-time deduplicate 只接受 INSERT 输入。
            throw std::logic_error(
                "ProcTimeDeduplicateKeepLastRowFunction (inputInsertOnly=true) requires INSERT-only input");
        }
        if (!stateful) {
            // generateInsert=false && generateUpdateBefore=false：恒发 UPDATE_AFTER，不使用状态。
            std::unique_ptr<RowData> current(input->extractRowData(i));
            changes.push_back({std::unique_ptr<RowData>(current->copy()), RowKind::UPDATE_AFTER, timestamp});
            continue;
        }

        std::unique_ptr<RowData> key(keySelector.getKey(input, i));
        ctx.setCurrentKey(key.get());
        const bool cacheHit =
            rocksdbState != nullptr && rocksdbState->stateCache->contains(key.get(), rocksdbState->getNamespace());
        RowData* previous = state->value();
        // RocksDB misses return an owned row, while heap and cache hits borrow state.
        std::unique_ptr<RowData> ownedPrevious(rocksdbState != nullptr && !cacheHit ? previous : nullptr);
        std::unique_ptr<RowData> current(input->extractRowData(i));
        const bool firstRow = previous == nullptr;
        // 存储行与输入行均为 INSERT kind，因此整段二进制比较等价于内容比较。Flink 在 TTL 开启时
        // 即便相等也会刷新状态并重发消息；Omni 无 state TTL，抑制相等行在可观察行为上完全一致。
        if (!firstRow && *previous == *current) {
            continue;
        }
        if (!firstRow && generateUpdateBefore) {
            changes.push_back({std::unique_ptr<RowData>(previous->copy()), RowKind::UPDATE_BEFORE, timestamp});
        }
        changes.push_back(
            {std::unique_ptr<RowData>(current->copy()), firstRow ? RowKind::INSERT : RowKind::UPDATE_AFTER, timestamp});
        current->setRowKind(RowKind::INSERT);
        if (heapState && !firstRow) {
            state->clear();
        }
        state->update(current.get(), false);
        if (heapState) {
            current.release();
        }
    }

    collectChanges(changes, types, inputTypes, out);
}
