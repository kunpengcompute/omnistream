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

namespace omnistream {

// ============================================================================
// RestorePQState — PriorityQueue 状态 writer
// ============================================================================

class RestorePQState {
public:
    virtual ~RestorePQState() = default;

    // 写入一条 PQ entry（key + value 字节序列）
    virtual void writeEntry(const std::vector<int8_t>& keyBytes, const std::vector<int8_t>& valueBytes) = 0;

    // flush 攒批的写入
    virtual void flush() = 0;

    // 丢弃未提交的写入资源
    virtual void discard() = 0;
};

} // namespace omnistream
