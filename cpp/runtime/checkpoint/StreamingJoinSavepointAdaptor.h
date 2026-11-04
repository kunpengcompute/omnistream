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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

#include "OperatorSavepointAdaptor.h"
#include "core/utils/ByteView.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/vb/VectorBatchRestoreHooks.h"
#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "table/types/logical/LogicalType.h"

namespace omnistream {

// StreamingJoin NoUniqueKey 兼容 savepoint 适配器。
//
// 当前 Omni 和 Flink StreamingJoin 都以完整 RowData 作为 MapState user key：
//   Inner/非保留侧：RowData -> count
//   Outer 保留侧：RowData -> (count, numAssociations)
// 两者的 entry payload 布局一致，差异只在 Omni Heap 快照会把同一 keyed key 下的 MapState
// entry 聚合到一个 value 中。该适配器负责转换 metadata，并在保存 Heap 状态时展开聚合 Map；
// 当前协议不再读取或写入 XXH128、ComboId 和 VectorBatch side table。
class StreamingJoinSavepointAdaptor : public OperatorSavepointAdaptor,
                                      public VectorBatchSaveHooks,
                                      public VectorBatchRestoreHooks {
public:
    // 使用工厂判定出的 adaptorType 创建 StreamingJoin 格式互通适配器，算子描述在 prepare 阶段解析。
    explicit StreamingJoinSavepointAdaptor(FlinkSavepointAdaptorType adaptorType);

    // 适配器不单独持有 serializer、restore backend 等外部资源，使用默认析构逻辑。
    ~StreamingJoinSavepointAdaptor() override = default;

    /*========== OperatorSavepointAdaptor ==========*/

    // 解析 Omni -> Flink save 所需的左右输入类型和 Join 状态布局。
    void prepareForSave(const nlohmann::json& operatorDescription) override;

    // 解析 Flink -> Omni restore 所需的左右输入类型和 Join 状态布局。
    void prepareForRestore(const nlohmann::json& operatorDescription) override;

    // 校验 Omni 源状态包含左右主状态，并验证当前 RowData serializer 布局。
    void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    // 校验 Flink 源状态只包含左右 logical MapState，并验证 Flink serializer 布局。
    void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    // 按 Flink StreamingJoin logical MapState 格式写出兼容 savepoint。
    void save(
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer) override;

    // 将 Flink StreamingJoin logical MapState 恢复为当前 Omni RowData MapState。
    void restore(SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend) override;

    /*========== OperatorSavepointAdaptor ==========*/
    /*========== VectorBatchSaveHooks ==========*/

    // 根据保存计划创建无 VB accessor 的 MapState 转换上下文。
    std::vector<VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan) override;

    // RocksDB source entry 直接透传；Heap 聚合 Map value 展开为多个 Flink MapState entry。
    template <typename Emit>
    void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry& entry,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan,
        Emit&& output);

    /*========== VectorBatchSaveHooks ==========*/
    /*========== Restore ==========*/

    // 返回指定 Flink 状态的 restore 处理类型。
    RestoreStateType getStateType(const StateMetaInfoSnapshot& metaInfo) const;

    // 根据 kvStateId 对应的 Flink 状态元数据构造 Omni 主状态元数据。
    StateMetaInfoSnapshot buildOmniMainMetaInfo(int kvStateId, const StateMetaInfoSnapshot& flinkMetaInfo);

    /*========== Restore ==========*/

private:
    // 单侧 Join 状态的格式转换参数。
    struct SidePlan {
        // 当前侧在 Flink 和 Omni 状态元数据中共同使用的 logical state name。
        std::string stateName;
        // 算子描述中的 Flink 输入逻辑类型名称，用于构造 RowType serializer。
        std::vector<std::string> inputTypeNames;
        // 转换后的 LogicalType DataTypeId 列表。
        std::vector<omniruntime::type::DataTypeId> inputTypeIds;
        // 当前侧 value 是否包含 left outer join 的 numAssociations 字段。
        bool outerJoinState = false;
    };

    // 根据算子描述初始化左右两侧的输入类型、状态名称和 outer join 标记。
    void prepareSidePlans(const nlohmann::json& operatorDescription);

    // 从算子描述的数组字段中解析输入字段类型名称，并拒绝缺失、非数组或非字符串元素。
    void parseInputTypes(SidePlan& sidePlan, const nlohmann::json& description, const std::string& fieldName);

    // 将 Heap 聚合或 RocksDB expanded MapState entry 统一展开并回调输出。
    template <typename Emit>
    void parseSourceMapEntries(
        const KeyValueStateIterator::CurrentEntry& entry, const SidePlan& sidePlan, Emit&& emit) const;

    // 根据源状态元数据构造 VectorBatchSaveFlow 所需的保存计划。
    VectorBatchSavePlan buildSavePlan(FullSnapshotResources& snapshotResources);

    // 根据状态名称返回左右两侧对应的 Join 状态计划。
    const SidePlan& sidePlanForState(const std::string& stateName) const;

    // 创建 adaptor 时确定的 StreamingJoin 兼容类型，用于区分 inner 和 left outer 状态布局。
    FlinkSavepointAdaptorType adaptorType_;

    // 左侧 Join 状态的名称、输入类型和状态布局参数。
    SidePlan leftPlan_;

    // 右侧 Join 状态的名称、输入类型和状态布局参数。
    SidePlan rightPlan_;
};

} // namespace omnistream
