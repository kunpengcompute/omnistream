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

#include "ProcTimeDeduplicateKeepFirstRowFunction.h"

#include <memory>
#include <utility>

#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/heap/HeapValueState.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "table/data/RowKind.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/types/logical/LogicalType.h"

namespace {
constexpr int64_t kUnseenFlag = 0;
constexpr int64_t kSeenFlag = 1;
} // namespace

ProcTimeDeduplicateKeepFirstRowFunction::ProcTimeDeduplicateKeepFirstRowFunction(const nlohmann::json& config)
{
    inputTypes_ = config["inputTypes"].get<std::vector<std::string>>();
    keyIndex_ = config["grouping"].get<std::vector<int32_t>>();
    keyedTypes_ = getKeyedTypes(keyIndex_, inputTypes_);
    groupByKeySelector_ = new KeySelector<RowData*>(keyedTypes_, keyIndex_);
}

ProcTimeDeduplicateKeepFirstRowFunction::~ProcTimeDeduplicateKeepFirstRowFunction()
{
    delete groupByKeySelector_;
    groupByKeySelector_ = nullptr;
}

void ProcTimeDeduplicateKeepFirstRowFunction::open(const Configuration&)
{
    LOG("ProcTimeDeduplicateKeepFirstRowFunction open");
    TypeSerializer* serializer = new LongSerializer();
    auto* seenStateDesc = new ValueStateDescriptor<int64_t>("deduplicate-state", serializer);

    auto* runtimeContext = static_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext());
    this->seenState_ = runtimeContext->getState<int64_t>(seenStateDesc);
    stateType_ = runtimeContext->getStateType();
    if (stateType_ == omnistream::StateType::ROCKSDB) {
        static_cast<RocksdbValueState<RowData*, VoidNamespace, int64_t>*>(seenState_)->setDefaultValue(kUnseenFlag);
    } else if (stateType_ == omnistream::StateType::HEAP) {
        static_cast<HeapValueState<RowData*, VoidNamespace, int64_t>*>(seenState_)->setDefaultValue(kUnseenFlag);
    } else {
        THROW_LOGIC_EXCEPTION("ProcTimeDeduplicateKeepFirstRowFunction only support heap and rocksdb state type");
    }
    LOG("ProcTimeDeduplicateKeepFirstRowFunction open finish");
}

std::vector<int32_t> ProcTimeDeduplicateKeepFirstRowFunction::getKeyedTypes(
    const std::vector<int32_t>& keyedIndex, const std::vector<std::string>& inputTypes)
{
    std::vector<int32_t> keyedTypes;
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    return keyedTypes;
}

void ProcTimeDeduplicateKeepFirstRowFunction::processBatch(
    omnistream::VectorBatch* inputVB, Context& ctx, TimestampedCollector& out)
{
    auto inputVBGuard = std::unique_ptr<omnistream::VectorBatch>(inputVB);
    const int rowCount = inputVBGuard->GetRowCount();
    for (int i = 0; i < rowCount; ++i) {
        // Flink checkInsertOnly: proc-time keep-first only accepts INSERT.
        if (inputVBGuard->getRowKind(i) != RowKind::INSERT) {
            THROW_LOGIC_EXCEPTION(
                "ProcTimeDeduplicateKeepFirstRowFunction requires INSERT-only input");
        }
    }

    // Content equality, not pointer identity: KeySelector allocates a new BinaryRowData per row.
    std::unordered_map<BinaryRowData*, int32_t> firstRowByKey;
    std::vector<std::pair<RowData*, int32_t>> firstRowsInOrder;
    firstRowByKey.reserve(static_cast<size_t>(rowCount));
    firstRowsInOrder.reserve(static_cast<size_t>(rowCount));
    for (int i = 0; i < rowCount; ++i) {
        RowData* key = groupByKeySelector_->getKey(inputVBGuard.get(), i);
        auto* binaryKey = static_cast<BinaryRowData*>(key);
        auto it = firstRowByKey.find(binaryKey);
        if (it == firstRowByKey.end()) {
            firstRowByKey.emplace(binaryKey, i);
            firstRowsInOrder.emplace_back(key, i);
        } else {
            delete key;
        }
    }

    std::vector<int32_t> outputRowIds;
    std::vector<RowData*> firstSeenKeys;
    outputRowIds.reserve(firstRowsInOrder.size());
    firstSeenKeys.reserve(firstRowsInOrder.size());

    for (auto& [key, rowId] : firstRowsInOrder) {
        ctx.setCurrentKey(key);
        if (seenState_->value() == kUnseenFlag) {
            outputRowIds.push_back(rowId);
            firstSeenKeys.push_back(key);
        } else {
            delete key;
        }
    }

    if (!firstSeenKeys.empty()) {
        UpdateSeenState(std::move(firstSeenKeys), ctx);
    }

    // buildNewVectorBatchByRowIds 对 0 行会去拷贝空的 rowKind 数组。没有新 key 时不下发，
    // 避免下游按列下标读到 0 列批。
    if (outputRowIds.empty()) {
        return;
    }
    omnistream::VectorBatch* outputVB =
        VectorBatchUtil::buildNewVectorBatchByRowIds(inputVBGuard.get(), outputRowIds);
    out.collect(outputVB);
}

void ProcTimeDeduplicateKeepFirstRowFunction::UpdateSeenState(std::vector<RowData*> firstSeenKeys, Context& ctx)
{
    if (stateType_ == omnistream::StateType::HEAP) {
        for (auto* key : firstSeenKeys) {
            ctx.setCurrentKey(key);
            seenState_->update(kSeenFlag);
        }
    } else {
        std::unordered_map<RowData*, int64_t> pendingUpdates;
        pendingUpdates.reserve(firstSeenKeys.size());
        for (auto* key : firstSeenKeys) {
            pendingUpdates.emplace(key, kSeenFlag);
        }
        auto* rocksdbState = static_cast<RocksdbValueState<RowData*, VoidNamespace, int64_t>*>(seenState_);
        rocksdbState->updateByBatch(pendingUpdates);
    }
    for (auto* key : firstSeenKeys) {
        delete key;
    }
}
