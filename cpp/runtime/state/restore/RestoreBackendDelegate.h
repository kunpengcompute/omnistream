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
#include <vector>

#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreKVState.h"
#include "runtime/state/restore/RestorePQState.h"

namespace omnistream {

enum class RestoreStateType {
    KV,             // 普通 KV 状态（源/目标 metadata 相同）
    KV_TRANSFORMED, // payload 直接透传，但使用 Adaptor 构造的目标 metadata
    PQ,             // PriorityQueue 状态
    KV_TRANSFORM,   // 使用 Adaptor 转换 KV payload 和目标 metadata（GroupAgg accumulator）
    UNSUPPORT,      // 未支持的状态
};

// ============================================================================
// RestoreBackendDelegate — 兼容恢复流程的 state writer 工厂
// ============================================================================
//
// 不负责 Adaptor 匹配、source state 白名单或 payload 语义解析。

class RestoreBackendDelegate {
public:
    virtual ~RestoreBackendDelegate() = default;

    // 创建 KV state writer
    virtual std::unique_ptr<RestoreKVState> createKVState(int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo) = 0;

    // 创建 PriorityQueue state
    virtual std::unique_ptr<RestorePQState> createPQState(int kvStateId, const StateMetaInfoSnapshot& metaInfo) = 0;
};

} // namespace omnistream
