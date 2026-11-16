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

#include <cstdint>
#include <stdexcept>
#include <vector>

#include "common.h"

namespace omnistream {

class RestoreKVState;

// 兼容 savepoint 恢复方向的算子语义扩展接口。
// VectorBatchRestoreFlow 负责状态遍历、writer 创建和生命周期管理，具体 Adaptor 负责
// stateType 分发、metadata 转换以及 KV_TRANSFORM 的数据转换逻辑。
class VectorBatchRestoreHooks {
public:
    virtual ~VectorBatchRestoreHooks() = default;

    // VectorBatchRestoreFlow KV_TRANSFORM 钩子：默认不支持，由具体 Adaptor 重写。
    // 对应需数据转换的 KV 状态（如 GroupAgg 的 accumulator compact）。
    // kvStateId: VectorBatchRestoreFlow 内部分配的 kv writer 索引。
    virtual void transformKVData(
        const std::vector<int8_t>& key, const std::vector<int8_t>& value, int kvStateId, RestoreKVState* writer)
    {
        ERROR_RELEASE("transformKVData not implemented by this adaptor.");
        throw std::runtime_error("transformKVData not implemented by this adaptor");
    }
};

} // namespace omnistream
