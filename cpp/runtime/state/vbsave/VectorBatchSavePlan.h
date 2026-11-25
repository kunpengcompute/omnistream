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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/KeyGroupRange.h"

namespace omnistream {

// ============================================================================
// 保存流程的状态类型分发
// ============================================================================
enum class VectorBatchStateType {
    KV,           // 普通 KV 状态（无需转换）
    KV_TRANSFORM, // 需要 Adaptor 转换 entry
    PQ,           // PriorityQueue 状态
};

// ============================================================================
// VectorBatchSaveStateContext — 保存阶段按 source kvStateId 下标访问的状态上下文
// 由 Adaptor 在 buildSaveStateContexts() 中构造，VectorBatchSaveFlow 根据 entry.kvStateId 读取。
// stateType 为 KV 时直接透传；KV_TRANSFORM 由 adaptor 转换。
// ============================================================================

struct VectorBatchSaveStateContext {
    bool writable = false;
    int mappedKvStateId = -1;
    std::string logicalStateName;
    TypeSerializer* valueSerializer = nullptr;
    TypeSerializer* sourceValueSerializer = nullptr;
    VectorBatchStateType stateType = VectorBatchStateType::KV;

    bool isValid() const
    {
        if (!writable || mappedKvStateId < 0) {
            return false;
        }
        if (stateType != VectorBatchStateType::KV && stateType != VectorBatchStateType::PQ &&
            valueSerializer == nullptr) {
            return false;
        }
        return true;
    }
};

// ============================================================================
// VectorBatchSavePlan — 由 Adaptor 构造的保存计划
// ============================================================================

struct VectorBatchSavePlan {
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> targetMetaInfos;
    std::vector<int> mainStateIds;
    std::unordered_map<int, int> kvStateIdMapping;

    struct StateContextSpec {
        int sourceKvStateId;
        std::string logicalStateName;
        TypeSerializer* valueSerializer = nullptr;
        VectorBatchStateType stateType = VectorBatchStateType::KV;
        TypeSerializer* sourceValueSerializer = nullptr;
    };
    std::vector<StateContextSpec> stateContextSpecs;

    KeyGroupRange* keyGroupRange = nullptr;
    std::string keySerializerJson;

    // 保存阶段由 Adaptor 持有的 serializer，保证构建 save contexts 后仍有明确的生命周期。
    std::vector<std::unique_ptr<TypeSerializer>> ownedSerializers;
};

// ============================================================================
// ConvertedEntry — 单条 entry 转换结果，供 Flow 和 Hooks 之间传递
// ============================================================================

struct ConvertedEntry {
    const VectorBatchSaveStateContext* context = nullptr;
    std::vector<int8_t> keyBytes;
    std::vector<int8_t> valueBytes;
};

} // namespace omnistream
