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

#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "table/data/vectorbatch/VectorBatchStorageInfo.h"

namespace omnistream {

// ComboId 二进制读写工具，统一处理 savepoint/checkpoint 中 comboId 的 8 字节大端序布局。
class ComboIdUtil {
public:
    // 按 big-endian 字节序读取 ComboId，避免通过 signed long 做中间转换。
    static inline ComboId readComboId(DataInputDeserializer& input)
    {
        ComboId comboId = 0;
        for (int i = 0; i < static_cast<int>(sizeof(ComboId)); ++i) {
            comboId = (comboId << 8) | static_cast<ComboId>(input.readByte());
        }
        return comboId;
    }

    // 按 big-endian 字节序写入 ComboId，保持与 Flink/Omni long 字节布局兼容。
    static inline void writeComboId(DataOutputSerializer& output, ComboId comboId)
    {
        uint8_t bytes[sizeof(ComboId)] = {};
        for (int i = 0; i < static_cast<int>(sizeof(ComboId)); ++i) {
            int shift = (static_cast<int>(sizeof(ComboId)) - 1 - i) * 8;
            bytes[i] = static_cast<uint8_t>((comboId >> shift) & 0xff);
        }
        output.write(bytes, static_cast<int>(sizeof(bytes)), 0, static_cast<int>(sizeof(bytes)));
    }
};

} // namespace omnistream
