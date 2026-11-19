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

#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "runtime/state/VoidNamespace.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "table/runtime/keyselector/KeySelector.h"

class ProcTimeDeduplicateKeepLastRowFunction final
    : public KeyedProcessFunction<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*> {
public:
    explicit ProcTimeDeduplicateKeepLastRowFunction(const nlohmann::json& config);

    void open(const Configuration& config) override;
    void processBatch(omnistream::VectorBatch* input, Context& ctx, TimestampedCollector& out) override;
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
        return state;
    }

private:
    std::vector<std::string> inputTypes;
    std::vector<omniruntime::type::DataTypeId> types;
    KeySelector<RowData*> keySelector;
    bool generateUpdateBefore;
    // Flink ProcTimeDeduplicateKeepLastRowFunction 的构造标志：inputInsertOnly 区分
    // ChangelogNormalize（false，走 processLastRowOnChangelog）与 append-only proc-time
    // Deduplicate（true，走 processLastRowOnProcTime）；generateInsert 仅 proc-time 分支使用。
    bool inputInsertOnly = false;
    bool generateInsert = true;
    ValueState<RowData*>* state = nullptr;
    bool heapState = false;
    RocksdbValueState<RowData*, VoidNamespace, RowData*>* rocksdbState = nullptr;

    void processBatchChangelog(omnistream::VectorBatch* input, Context& ctx, TimestampedCollector& out);
    void processBatchInsertOnly(omnistream::VectorBatch* input, Context& ctx, TimestampedCollector& out);
};
