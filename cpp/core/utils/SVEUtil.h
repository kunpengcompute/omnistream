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
class VectorBatch;

class SVEUtil {
public:
    static void copyU8ArrayByIndices(const uint8_t* source, uint8_t* target, const std::vector<int32_t>& indices);

    static void copyI64ArrayByIndices(const int64_t* source, int64_t* target, const std::vector<int32_t>& indices);

    static void extract16BitsFromU64ToI32(const uint64_t* source, int32_t* target, int32_t num, int32_t offset);

    static void extract32BitsFromU64ToU32(const uint64_t* source, uint32_t* target, int32_t num, int32_t offset);
};
} // namespace omnistream
