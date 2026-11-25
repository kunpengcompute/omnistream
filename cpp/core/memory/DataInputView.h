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

#ifndef FLINK_TNEL_DATAINPUTVIEW_H
#define FLINK_TNEL_DATAINPUTVIEW_H

#include <stdexcept>
#include <string>

#include "common.h"
#include "../utils/SysDataInput.h"

class DataInputView : public SysDataInput {
    static constexpr int UNKNOWN_REMAINING = -1;

public:
    virtual void* GetBuffer() = 0;

    // 返回剩余可读字节数；-1 表示输入视图不提供该能力，子类无需强制实现。
    virtual int remaining() const
    {
        return UNKNOWN_REMAINING;
    }

    // 同时校验业务静态上限与输入流动态上限；remaining() 不可用时仅校验静态上限。
    void validateLength(int length, int maxLengthInBytes) const
    {
        const int remainingBytes = remaining();
        if (length <= 0 || length > maxLengthInBytes ||
            (remainingBytes != UNKNOWN_REMAINING && length > remainingBytes)) {
            throw std::runtime_error(
                "Invalid serialized length: " + std::to_string(length) + ", max bytes: " +
                std::to_string(maxLengthInBytes) + ", remaining bytes: " + std::to_string(remainingBytes));
        }
    }

    ~DataInputView() override = default;
};
#endif
