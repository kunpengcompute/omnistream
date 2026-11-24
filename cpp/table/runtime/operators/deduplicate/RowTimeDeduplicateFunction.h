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

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/api/common/state/State.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "core/api/common/state/ValueState.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/JoinedRowData.h"
#include "table/data/RowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/keyselector/KeySelector.h"

class RowTimeDeduplicateFunction
    : public KeyedProcessFunction<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*> {
public:
    explicit RowTimeDeduplicateFunction(const nlohmann::json& config);
    ~RowTimeDeduplicateFunction() override = default;

    void processBatch(omnistream::VectorBatch* inputVB, Context& ctx, TimestampedCollector& out) override;
    void open(const Configuration&) override;

    static std::vector<std::int32_t> getKeyedTypes(
        const std::vector<int32_t>& keyedIndex, const std::vector<std::string>& inputTypes);

    void processElement(omnistream::VectorBatch*, Context*, TimestampedCollector*) override
    {
        NOT_IMPL_EXCEPTION;
    }

    JoinedRowData* getResultRow() override
    {
        return nullptr;
    }

    ValueState<RowData*>* getValueState() override
    {
        return recordState_;
    }

private:
    struct BatchCandidates {
        std::unordered_map<RowData*, int32_t> rowIds;
        std::vector<std::unique_ptr<RowData>> ownedKeys;
    };

    struct PendingUpdate {
        // key borrows from BatchCandidates::ownedKeys until updateState returns.
        RowData* key = nullptr;
        RowData* previousRow = nullptr;
        // RocksDB without cache returns a newly deserialized row to the caller.
        // Heap and RocksDB cache values remain owned by their respective backends.
        std::unique_ptr<RowData> ownedPreviousRow;
        // The selected row remains locally owned until Heap state accepts ownership;
        // RocksDB serializes/deep-copies it and leaves ownership here.
        std::unique_ptr<RowData> currentRow;
        bool isInsert = false;
    };

    BatchCandidates selectBatchCandidates(omnistream::VectorBatch* inputVB);
    bool shouldReplace(RowData* previousRow, omnistream::VectorBatch* currentVB, int32_t currentRowId) const;
    omnistream::VectorBatch* buildOutputBatch(
        const std::vector<RowData*>& rows,
        const std::vector<RowKind>& rowKinds,
        const std::vector<int64_t>& timestamps) const;
    void updateState(std::vector<PendingUpdate>& updates, Context& ctx);

    std::vector<std::string> inputTypes_;
    std::vector<int32_t> inputTypeIds_;
    std::vector<int32_t> keyIndices_;
    std::vector<int32_t> keyedTypes_;

    bool generateUpdateBefore_ = false;
    bool generateInsert_ = false;
    int32_t rowtimeIndex_ = -1;
    bool keepLastRow_ = false;

    // Non-owning: RuntimeContext owns the concrete ValueState instance.
    ValueState<RowData*>* recordState_ = nullptr;
    std::unique_ptr<KeySelector<RowData*>> groupByKeySelector_;
    omnistream::StateType backendType_ = omnistream::StateType::HEAP;
    bool rocksdbCacheEnabled_ = false;
};
