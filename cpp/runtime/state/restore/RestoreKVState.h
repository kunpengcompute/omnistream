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

#include "core/utils/ByteView.h"

namespace omnistream {

// ============================================================================
// RestoreKVState — 普通 KV 状态 writer
// ============================================================================

class RestoreKVState {
public:
    virtual ~RestoreKVState() = default;

    // 写入一条已经序列化的 key → value entry。
    void writeEntry(const std::vector<int8_t>& keyBytes, ByteView value)
    {
        writeBytesEntry(keyBytes, value);
    }

    // 刷新后端尚未提交的写入。
    virtual void flush() = 0;

    // 丢弃未提交的写入资源，保持异常向外传播
    virtual void discard() = 0;

    // 切换当前 keyGroup（跨 keyGroup 复用 writer 时调用）
    virtual void setKeyGroupId(int keyGroupId) = 0;

protected:
    virtual void writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView value) = 0;
};

} // namespace omnistream
