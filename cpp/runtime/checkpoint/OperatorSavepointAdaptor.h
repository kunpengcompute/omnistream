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

#include <memory>
#include <vector>
#include <string>
#include <nlohmann/json.hpp>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "table/types/logical/LogicalType.h"
#include "runtime/state/KeyValueStateIterator.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/CheckpointOptions.h"

class SavepointRestoreResultIterator;

namespace omnistream {
class OmniTaskBridge;
class RestoreBackendDelegate;

class OperatorSavepointAdaptor {
public:
    virtual ~OperatorSavepointAdaptor() = default;

    virtual void prepareForSave(const nlohmann::json& operatorDescription)
    {
    }

    virtual void prepareForRestore(const nlohmann::json& operatorDescription)
    {
    }

    // 保存方向 source state metadata-only 校验（由 CompatibleFullSnapshotAsyncWriter 在 save() 之前调用）。
    // 只做 metadata 级校验：输入状态集合闭合、serializer/meta 前置校验。默认 no-op。
    virtual void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
    {
    }

    // 恢复方向 source state metadata-only 校验。一次 compatible restore 会按 KeyGroupsStateHandle
    // 调用零到多次；每次 metaInfos 只属于当前 handle，且必须独立满足 concrete adaptor 的完整
    // source-state contract。所有调用都发生在 restore() 和任何 backend 写入之前。
    // 实现不得缓存 handle-local kvStateId、metadata 顺序或 KeyGroupRange；如缓存公共 schema，
    // 后续调用必须先校验一致性。默认 no-op。
    virtual void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
    {
    }

    // 执行 compatible savepoint 写出（由 CompatibleFullSnapshotAsyncWriter 调用）。
    // 按 §13.10：Adaptor 不接收 bridge/checkpointId/checkpointOptions，不开关 stream，不封装
    // SnapshotResult；只往已打开的 stream 写 target metadata + entries，并向 keyGroupOffsets 记录偏移。
    virtual void save(
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer) = 0;

    // 执行完整的兼容 Savepoint 恢复（由 *CompatibleFullRestoreOperation 调用）
    // 遍历 restoreIterator，将 Flink 状态转换为 Omni 格式并通过 backend 写入后端。
    virtual void restore(SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend) = 0;

    // 从 JSON 对象中解析指定字段的字符串数组。
    // fieldName: JSON 中的字段名（如 "inputTypes"）
    // 返回解析后的字符串列表，字段不存在或非数组时返回空 vector
    static std::vector<std::string> parseStringArray(const nlohmann::json& json, const std::string& fieldName)
    {
        std::vector<std::string> result;
        if (json.contains(fieldName) && json[fieldName].is_array()) {
            for (const auto& item : json[fieldName]) {
                if (item.is_string()) {
                    result.push_back(item.get<std::string>());
                }
            }
        }
        return result;
    }

    // 将 Flink 类型名称字符串列表转换为 Omni DataTypeId 列表
    static std::vector<omniruntime::type::DataTypeId> convertToDataTypes(const std::vector<std::string>& typeNames)
    {
        std::vector<omniruntime::type::DataTypeId> result;
        result.reserve(typeNames.size());
        for (const auto& name : typeNames) {
            result.push_back(LogicalType::flinkTypeToOmniTypeId(name));
        }
        return result;
    }
};
} // namespace omnistream
