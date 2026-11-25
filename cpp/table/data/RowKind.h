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

#ifndef FLINK_TNEL_ROWKIND_H
#define FLINK_TNEL_ROWKIND_H

#include <cstdint>

enum class RowKind : std::uint8_t {
    INSERT = 0,
    UPDATE_BEFORE = 1,
    UPDATE_AFTER = 2,
    DELETE = 3
};

inline const char* to_string(RowKind kind)
{
    switch (kind) {
        case RowKind::INSERT: return "+I";
        case RowKind::UPDATE_BEFORE: return "-U";
        case RowKind::UPDATE_AFTER: return "+U";
        case RowKind::DELETE: return "-D";
        default: return "UNKNOWN"; // 处理未定义的枚举值
    }
}

#endif
