/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <memory>
#include <type_traits>
#include <vector>

#include "AbstractTopNFunction.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/heap/HeapValueState.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/RowKind.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/runtime/keyselector/KeySelector.h"

/**
 * Native AppendOnlyFirstNFunction.
 *
 * Flink uses this Rank variant when the input is append-only, the single sort
 * field is processing time, and the order is ASC. State is a per-key counter:
 * the first N arriving rows are emitted as INSERT, later rows are dropped.
 * No sort buffer and no retract.
 */
template <typename KeyType>
class AppendOnlyFirstNFunction : public AbstractTopNFunction<KeyType> {
public:
    explicit AppendOnlyFirstNFunction(const nlohmann::json& rankConfig)
        : AbstractTopNFunction<KeyType>(rankConfig),
          partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices)
    {
    }

    ~AppendOnlyFirstNFunction() = default;

    void open(const Configuration& context) override;
    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<KeyType, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override;

    ValueState<KeyType>* getValueState() override
    {
        return nullptr;
    }

private:
    static constexpr int64_t kUnseenRank = 0;

    KeySelector<KeyType> partitionKeySelector;
    ValueState<int64_t>* counterState_ = nullptr;
    omnistream::StateType stateType_ = omnistream::StateType::HEAP;
};

template <typename KeyType>
void AppendOnlyFirstNFunction<KeyType>::open(const Configuration&)
{
    LOG("AppendOnlyFirstNFunction open");
    TypeSerializer* serializer = new LongSerializer();
    auto* counterStateDesc = new ValueStateDescriptor<int64_t>("counterState", serializer);
    auto* runtimeContext = static_cast<StreamingRuntimeContext<KeyType>*>(this->getRuntimeContext());
    this->counterState_ = runtimeContext->template getState<int64_t>(counterStateDesc);
    stateType_ = runtimeContext->getStateType();
    if (stateType_ == omnistream::StateType::ROCKSDB) {
        static_cast<RocksdbValueState<KeyType, VoidNamespace, int64_t>*>(counterState_)->setDefaultValue(kUnseenRank);
    } else if (stateType_ == omnistream::StateType::HEAP) {
        static_cast<HeapValueState<KeyType, VoidNamespace, int64_t>*>(counterState_)->setDefaultValue(kUnseenRank);
    } else {
        THROW_LOGIC_EXCEPTION("AppendOnlyFirstNFunction only support heap and rocksdb state type");
    }
    LOG("AppendOnlyFirstNFunction open finish");
}

template <typename KeyType>
void AppendOnlyFirstNFunction<KeyType>::processBatch(
    omnistream::VectorBatch* inputBatch,
    typename KeyedProcessFunction<KeyType, RowData*, RowData*>::Context& ctx,
    TimestampedCollector& out)
{
    auto inputBatchGuard = std::unique_ptr<omnistream::VectorBatch>(inputBatch);
    this->initRankEnd(nullptr);
    const int rowCount = inputBatchGuard->GetRowCount();
    if (rowCount <= 0) {
        out.collect(new omnistream::VectorBatch(0));
        return;
    }

    std::vector<int32_t> outputRowIds;
    std::vector<int64_t> outputRanks;
    outputRowIds.reserve(static_cast<size_t>(rowCount));
    outputRanks.reserve(static_cast<size_t>(rowCount));
    const bool emitWithRank = this->outputRankNumber || this->hasOffset();

    for (int i = 0; i < rowCount; ++i) {
        KeyType key = partitionKeySelector.getKey(inputBatchGuard.get(), i);
        ctx.setCurrentKey(key);
        int64_t currentRank = counterState_->value();
        if (currentRank >= this->rankEnd) {
            if constexpr (std::is_pointer<KeyType>::value) {
                delete key;
            }
            continue;
        }
        currentRank += 1;
        counterState_->update(currentRank);
        if (!emitWithRank || this->isInRankRange(static_cast<int>(currentRank))) {
            outputRowIds.push_back(i);
            outputRanks.push_back(currentRank);
        }
        if constexpr (std::is_pointer<KeyType>::value) {
            delete key;
        }
    }

    omnistream::VectorBatch* outputBatch = nullptr;
    if (outputRowIds.empty()) {
        outputBatch = new omnistream::VectorBatch(0);
    } else {
        outputBatch = VectorBatchUtil::buildNewVectorBatchByRowIds(inputBatchGuard.get(), outputRowIds);
        for (int i = 0; i < outputBatch->GetRowCount(); ++i) {
            outputBatch->setRowKind(i, RowKind::INSERT);
        }
        if (this->outputRankNumber) {
            auto* rankVec = new omniruntime::vec::Vector<int64_t>(static_cast<int>(outputRanks.size()));
            for (size_t i = 0; i < outputRanks.size(); ++i) {
                rankVec->SetValue(static_cast<int>(i), outputRanks[i]);
            }
            outputBatch->Append(rankVec);
        }
    }
    this->collectOutputBatch(out, outputBatch);
}
