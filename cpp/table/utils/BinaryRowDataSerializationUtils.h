/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <cstring>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "table/data/binary/BinaryRowData.h"
using namespace omniruntime::type;
using namespace omniruntime::vec::unsafe;
namespace omnistream {
using namespace std;
class BinaryRowDataSerializationUtils {
public:
    static int calculateBinaryRowSerializableSize(BinaryRowData* rowData)
    {
        // Size = [DataType Tag] + [Arity] + [Payload Length] + [Raw Payload Bytes]
        return sizeof(int8_t) + sizeof(int32_t) + sizeof(int32_t) + rowData->getSizeInBytes();
    }

    static void serializeBinaryRowData(BinaryRowData* rowData, int32_t bufferCapacity, uint8_t*& buffer)
    {
        LOG("serializeBinaryRowData");
        if (rowData == nullptr || buffer == nullptr) {
            throw std::invalid_argument("Null BinaryRowData serialization input");
        }
        errno_t ret;
        int32_t arity = rowData->getArity();
        int32_t length = rowData->getSizeInBytes(); // equivalent to source.readInt() in your reference

        if (length < 0 || bufferCapacity < 0 ||
            static_cast<int64_t>(length) + sizeof(int8_t) + 2 * sizeof(int32_t) > bufferCapacity) {
            throw std::invalid_argument("Insufficient buffer for BinaryRowData");
        }

        // 1. Write DataType Tag (1 byte)
        int8_t dataType = static_cast<int>(StreamElementTag::BINARY_ROW_DATA);
        ret = memcpy_s(buffer, bufferCapacity, &dataType, sizeof(int8_t));
        if (ret != EOK) throw std::runtime_error("memcpy_s failed for dataType");
        buffer += sizeof(int8_t);
        bufferCapacity -= sizeof(int8_t);

        // 2. Write Arity (4 bytes)
        ret = memcpy_s(buffer, bufferCapacity, &arity, sizeof(int32_t));
        if (ret != EOK) throw std::runtime_error("memcpy_s failed for arity");
        buffer += sizeof(int32_t);
        bufferCapacity -= sizeof(int32_t);

        // 3. Write Length (4 bytes)
        ret = memcpy_s(buffer, bufferCapacity, &length, sizeof(int32_t));
        if (ret != EOK) throw std::runtime_error("memcpy_s failed for length");
        buffer += sizeof(int32_t);
        bufferCapacity -= sizeof(int32_t);

        // 4. Write Payload Data (The raw bytes)
        uint8_t* bytes = rowData->getSegment() + rowData->getOffset();
        ret = memcpy_s(buffer, bufferCapacity, bytes, length);
        if (ret != EOK) throw std::runtime_error("memcpy_s failed for rowData memory");
        buffer += length;

        return;
    }

    static BinaryRowData* deserializeBinaryRowData(uint8_t*& buffer, size_t remaining)
    {
        constexpr size_t headerSize = 2 * sizeof(int32_t);
        constexpr int32_t maxRowBytes = 64 * 1024 * 1024;
        if (buffer == nullptr || remaining < headerSize) {
            throw std::invalid_argument("Truncated BinaryRowData header");
        }
        auto* cursor = buffer;
        int32_t arity = 0;
        int32_t length = 0;
        if (memcpy_s(&arity, sizeof(arity), cursor, sizeof(arity)) != EOK ||
            memcpy_s(&length, sizeof(length), cursor + sizeof(arity), sizeof(length)) != EOK) {
            throw std::runtime_error("Cannot read BinaryRowData header");
        }
        // Use wide arithmetic before invoking the int-based BinaryRowData size helpers.
        int64_t fixedBytes = ((static_cast<int64_t>(arity) + 63 + BinaryRowData::HEADER_SIZE_IN_BITS) / 64) * 8 +
                             static_cast<int64_t>(arity) * 8;
        if (arity < 0 || length < fixedBytes || length > maxRowBytes ||
            static_cast<size_t>(length) > remaining - headerSize) {
            throw std::invalid_argument("Invalid BinaryRowData arity or payload length");
        }
        auto row = std::make_unique<BinaryRowData>(arity, length);
        if (memcpy_s(row->getSegment() + row->getOffset(), length, cursor + headerSize, length) != EOK) {
            throw std::runtime_error("Cannot copy BinaryRowData payload");
        }
        row->setSizeInBytes(length);
        buffer = cursor + headerSize + length;
        return row.release();
    }
};
} // namespace omnistream
