#pragma once

#include <cstring>
#include <cstdint>

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
        errno_t ret;
        uint8_t* original = buffer;
        int32_t arity = rowData->getArity();
        int32_t length = rowData->getSizeInBytes(); // equivalent to source.readInt() in your reference

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

    static BinaryRowData* deserializeBinaryRowData(uint8_t*& buffer)
    {
        LOG("----deserializeBinaryRowData start:: " << reinterpret_cast<long>(buffer));

        int32_t arity = 0;
        memcpy_s(&arity, sizeof(arity), buffer, sizeof(arity));

        buffer += sizeof(arity);
        int32_t length = 0;
        memcpy_s(&length, sizeof(length), buffer, sizeof(length));

        buffer += sizeof(length);
        BinaryRowData* row = new BinaryRowData(arity, length);
        uint8_t* destBytes = row->getSegment() + row->getOffset();
        memcpy_s(destBytes, length, buffer, length);

        row->setSizeInBytes(length);

        buffer += length;

        LOG("----deserializeBinaryRowData END:: " << reinterpret_cast<long>(buffer));

        return row;
    }
};
} // namespace omnistream
