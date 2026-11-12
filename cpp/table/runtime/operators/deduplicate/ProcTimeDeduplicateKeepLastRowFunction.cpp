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
      generateUpdateBefore(config.at("generateUpdateBefore").get<bool>())
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
        throw std::logic_error("ChangelogNormalize requires Heap or RocksDB state");
    }
}

void ProcTimeDeduplicateKeepLastRowFunction::processBatch(
    omnistream::VectorBatch* input, Context& ctx, TimestampedCollector& out)
{
    std::unique_ptr<omnistream::VectorBatch> inputGuard(input);
    struct Change {
        std::unique_ptr<RowData> row;
        RowKind kind;
        int64_t timestamp;
    };
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
        current->setRowKind(RowKind::INSERT);
        const bool firstRow = previous == nullptr;
        if (!firstRow && *previous == *current) {
            continue;
        }
        if (!firstRow && generateUpdateBefore) {
            changes.push_back({std::unique_ptr<RowData>(previous->copy()), RowKind::UPDATE_BEFORE, timestamp});
        }
        changes.push_back(
            {std::unique_ptr<RowData>(current->copy()), firstRow ? RowKind::INSERT : RowKind::UPDATE_AFTER, timestamp});
        if (heapState && !firstRow) {
            state->clear();
        }
        state->update(current.get(), false);
        if (heapState) {
            current.release();
        }
    }

    if (changes.empty()) {
        return;
    }
    std::unique_ptr<omnistream::VectorBatch> result(omnistream::VectorBatch::CreateVectorBatch(changes.size(), types));
    for (size_t row = 0; row < changes.size(); ++row) {
        const auto& change = changes[row];
        auto* data = static_cast<BinaryRowData*>(change.row.get());
        for (size_t column = 0; column < types.size(); ++column) {
            auto* vector = result->Get(column);
            const auto type = static_cast<size_t>(vector->GetTypeId());
            if (type >= rowDeserializerCenter.size() || rowDeserializerCenter[type] == nullptr) {
                throw std::runtime_error(
                    "ChangelogNormalize has no output converter for column " + std::to_string(column) + " (" +
                    inputTypes[column] + ")");
            }
            rowDeserializerCenter[type](vector, row, data, column);
        }
        result->setRowKind(row, change.kind);
        result->setTimestamp(row, change.timestamp);
    }
    out.collect(result.release());
}
