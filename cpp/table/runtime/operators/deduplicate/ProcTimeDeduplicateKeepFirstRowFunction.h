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

#pragma once

#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>
#include "core/api/common/state/ValueState.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/JoinedRowData.h"
#include "table/data/RowData.h"
#include "table/runtime/keyselector/KeySelector.h"

/**
 * Processing-time Deduplicate that keeps the first row per key.
 *
 * Aligns with Flink's ProcTimeDeduplicateKeepFirstRowFunction:
 * - state is a seen-flag (true once a key has been emitted);
 * - the first INSERT of a key is forwarded as INSERT;
 * - later rows of the same key are dropped (no retract / update messages).
 */
class ProcTimeDeduplicateKeepFirstRowFunction
    : public KeyedProcessFunction<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*> {
public:
    explicit ProcTimeDeduplicateKeepFirstRowFunction(const nlohmann::json& config);
    ~ProcTimeDeduplicateKeepFirstRowFunction();

    void open(const Configuration&) override;
    void processBatch(omnistream::VectorBatch* inputVB, Context& ctx, TimestampedCollector& out) override;

    void processElement(omnistream::VectorBatch* input, Context* ctx, TimestampedCollector* out) override
    {
        NOT_IMPL_EXCEPTION;
    }

    JoinedRowData* getResultRow() override
    {
        return nullptr;
    }

    ValueState<RowData*>* getValueState() override
    {
        return nullptr;
    }

private:
    static std::vector<int32_t> getKeyedTypes(
        const std::vector<int32_t>& keyedIndex, const std::vector<std::string>& inputTypes);
    void UpdateSeenState(const std::vector<RowData*>& firstSeenKeys, Context& ctx);

    std::vector<std::string> inputTypes_;
    std::vector<int32_t> keyIndex_;
    std::vector<int32_t> keyedTypes_;
    KeySelector<RowData*>* groupByKeySelector_ = nullptr;

    // 0 = unseen (default), 1 = already emitted the first row for this key.
    ValueState<int64_t>* seenState_ = nullptr;
    omnistream::StateType stateType_ = omnistream::StateType::HEAP;
};
