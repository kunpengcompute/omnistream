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
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "OperatorSavepointAdaptor.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/vb/VectorBatchRestoreHooks.h"
#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/data/binary/BinaryRowData.h"

namespace omnistream {

// Writes Omni GroupAgg state in Flink 1.16.3 layout. Omni omits RAW/DataView
// accumulator fields at runtime; the adaptor restores those positions as null
// placeholders while leaving the real DataView keyed states untouched.
class GroupAggSavepointAdaptor : public OperatorSavepointAdaptor,
                                 public VectorBatchSaveHooks,
                                 public VectorBatchRestoreHooks {
public:
    void prepareForSave(const nlohmann::json& operatorDescription) override;
    void prepareForRestore(const nlohmann::json& operatorDescription) override;
    void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;
    void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    void save(
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer) override;

    void restore(SavepointRestoreResultIterator&, RestoreBackendDelegate&) override;

    std::vector<VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan) override;

    template <typename Emit>
    void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry& entry,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan,
        Emit&& output);

    // ===== VectorBatchRestoreFlow Derived hook =====

    // 返回指定 kvStateId 的状态类型：
    //   PQ → PQ, accState → KV_TRANSFORM, 其余 KEY_VALUE → KV
    RestoreStateType getStateType(const StateMetaInfoSnapshot& metaInfo);

    // 构造 Omni 主表 metadata（accState 使用 omniAccSerializer_ 替换 valueSerializer）。
    StateMetaInfoSnapshot buildOmniMainMetaInfo(int kvStateId, const StateMetaInfoSnapshot& flinkMetaInfo);

    // 转换一条 Flink accumulator entry 为 Omni 格式（compactAccumulator）并写入 KV writer。
    void transformKVData(
        const std::vector<int8_t>& key, const std::vector<int8_t>& value, int kvStateId, RestoreKVState* writer);

private:
    static constexpr const char* ACC_STATE_NAME = "accState";

    VectorBatchSavePlan buildSavePlan(FullSnapshotResources& snapshotResources);
    std::unique_ptr<BinaryRowData> expandAccumulator(RowData& source) const;
    std::unique_ptr<BinaryRowData> compactAccumulator(RowData& source) const;
    void prepareAccumulatorTypes(const nlohmann::json& operatorDescription);

    std::vector<std::string> flinkAccTypes_;
    std::vector<std::string> omniAccTypes_;
    std::vector<int> flinkToOmniIndex_;
    std::vector<omniruntime::type::DataTypeId> flinkAccTypeIds_;
    std::unique_ptr<RowDataSerializer> flinkAccSerializer_;
    std::unique_ptr<RowDataSerializer> omniAccSerializer_;

    // KV_TRANSFORM 恢复时，记录 Flink source serializer 用于反序列化 accumulator
    std::unordered_map<int, TypeSerializer*> sourceSerializers_;
};

} // namespace omnistream
