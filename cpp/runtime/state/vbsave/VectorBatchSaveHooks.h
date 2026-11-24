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
#include <vector>

#include "common.h"
#include "core/utils/ByteView.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyValueStateIterator.h"

namespace omnistream {

// 兼容 savepoint 保存方向的算子语义扩展接口。
// VectorBatchSaveFlow 负责状态遍历和结果写出，具体 Adaptor 负责 Flink logical key/value 编码。
class VectorBatchSaveHooks {
public:
    virtual ~VectorBatchSaveHooks() = default;

    // 构造按 source kvStateId 下标访问的保存上下文数组。
    // Adaptor 负责设置 stateType、mappedKvStateId 和 serializer。
    virtual std::vector<VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan) = 0;
};

} // namespace omnistream
