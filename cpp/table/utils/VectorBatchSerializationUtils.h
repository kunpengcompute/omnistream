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

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include "OmniOperatorJIT/core/src/vector/vector.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "streaming/runtime/streamrecord/StreamElement.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace omniruntime::vec::unsafe;
using namespace std;

namespace omnistream {

struct SerializedBatchInfo {
    SerializedBatchInfo(uint8_t* bufferAddress, int32_t sizeValue, int type = 0)
        : memorySegmentAddress(bufferAddress),
          dataAddress(bufferAddress),
          dataSize(sizeValue),
          bufferType(type)
    {
    }

    SerializedBatchInfo(uint8_t* segmentAddress, uint8_t* payloadAddress, int32_t sizeValue, int type = 0)
        : memorySegmentAddress(segmentAddress),
          dataAddress(payloadAddress),
          dataSize(sizeValue),
          bufferType(type)
    {
    }

    uint8_t* memorySegmentAddress;
    union {
        uint8_t* dataAddress;
        uint8_t* buffer; // compatibility name for state serialization paths
    };
    union {
        int32_t dataSize;
        int32_t size; // compatibility name for state serialization paths
    };
    int bufferType; // 0 for vector batch, 1 memory segment buffer, 2 memory segment event
};

// we assume all the char ana varchar data are stored
// vector<LargeStringContainer>, other primitive data types are stored in
// vector<xxx>
class VectorBatchSerializationUtils {
public:
    static SerializedBatchInfo serializeVectorBatch(VectorBatch* vectorBatch, int32_t batchSize, uint8_t*& buffer);

    static void serializeTimestampAndRowKinds(VectorBatch* vectorBatch, uint8_t*& buffer, int32_t bufferSize);

    static int32_t calculateVectorBatchSerializableSize(VectorBatch* vectorBatch);
    static int32_t calculateVectorBatchPayloadSize(VectorBatch* vectorBatch);

    static int32_t calculateVectorSerializableSize(BaseVector* baseVector);

    // dataPayload
    static int32_t calculatePrimitiveDataSize(BaseVector* baseVector);

    // stringDataSize--offsetArray--stringData
    static int32_t calculateCharDataSize(BaseVector* baseVector);

    static void serializePrimitiveVector(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize);

    static void serializeCharVector(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize);

    static void serializeInt64(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize);

    static void serializeInt32(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize);

    static void serializeInt16(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize);

    static void serializeDouble(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize);

    static void serializeBool(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize);

    static void serializeDecimal128(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize);

    static int32_t calculateStringDictionarySerializableSize(BaseVector* baseVector);

    static int32_t calculateLargeStringContainerDataSize(
        std::shared_ptr<LargeStringContainer<std::string_view>> stringContainer, int32_t rowCount)
    {
        int32_t stringBodySizeField = sizeof(int32_t);
        int32_t offsetSize = (rowCount + 1) * sizeof(int32_t);
        int32_t dataSize = 0;
        int32_t* offsetArr = UnsafeStringContainer::GetOffsets(stringContainer.get());
        dataSize = offsetArr[rowCount];
        return stringBodySizeField + offsetSize + dataSize * sizeof(char);
    }

    static void serializeStringDictionaryContainerVector(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
    {
        serializeVectorBatchHeader(baseVector, buffer, bufferSize);

        serializeStringDictionaryBody(baseVector, buffer, bufferSize);
    }

    static void serializeVectorBatchHeader(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
    {
        int32_t vectorSize = calculateVectorSerializableSize(baseVector);

        auto ret = memcpy_s(buffer, bufferSize, &vectorSize, sizeof(int32_t));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += sizeof(int32_t);
        bufferSize -= sizeof(int32_t);
        Encoding encoding = baseVector->GetEncoding();
        int8_t codingNumeric = static_cast<int8_t>(encoding);
        ret = memcpy_s(buffer, bufferSize, &codingNumeric, sizeof(int8_t));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += sizeof(int8_t);
        bufferSize -= sizeof(int8_t);
        DataTypeId dataType = baseVector->GetTypeId();
        int8_t numericValue = static_cast<int8_t>(dataType);
        ret = memcpy_s(buffer, bufferSize, &numericValue, sizeof(int8_t));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += sizeof(int8_t);
        bufferSize -= sizeof(int8_t);
    }

    static void serializeStringDictionaryBody(BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
    {
        auto* vec_string_dict_container =
            reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>>*>(baseVector);
        std::shared_ptr<DictionaryContainer<std::string_view>> string_dictionary =
            static_cast<std::shared_ptr<DictionaryContainer<std::string_view>>>(
                unsafe::UnsafeDictionaryVector::GetDictionaryOriginal(vec_string_dict_container));
        std::shared_ptr<LargeStringContainer<std::string_view>> stringContainer =
            unsafe::UnsafeDictionaryContainer::GetStringDictionaryOriginal(string_dictionary.get());

        // get value size
        int32_t valueSize = baseVector->GetSize();
        // get values
        int32_t* valueIndex = unsafe::UnsafeDictionaryContainer::GetIds(string_dictionary.get());
        auto ret = memcpy_s(buffer, bufferSize, valueIndex, sizeof(int32_t) * valueSize);
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += sizeof(int32_t) * valueSize;

        // get dict offset
        int32_t dictOffset = unsafe::UnsafeDictionaryContainer::GetDictOffset(string_dictionary.get());
        ret = memcpy_s(buffer, bufferSize, &dictOffset, sizeof(int32_t));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += sizeof(int32_t);

        // get dict size
        int32_t dictSize = unsafe::UnsafeDictionaryContainer::GetDictSize(string_dictionary.get());
        ret = memcpy_s(buffer, bufferSize, &dictSize, sizeof(int32_t));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += sizeof(int32_t);

        int32_t* offsetArr = UnsafeStringContainer::GetOffsets(stringContainer.get());

        // real data size
        int32_t stringBodySize = offsetArr[dictSize];
        ret = memcpy_s(buffer, bufferSize, &stringBodySize, sizeof(int32_t));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += sizeof(int32_t);
        serializeStringDictionaryTail(
            baseVector, buffer, bufferSize, valueSize, offsetArr, dictSize, stringBodySize, stringContainer);
    }

    // valueSize is the vector's row count, dictSize the number of distinct dictionary entries.
    // The two are unrelated: the null bitmap and value indices are per row, the offset array and
    // string body are per dictionary entry.
    static void serializeStringDictionaryTail(
        BaseVector* baseVector,
        uint8_t*& buffer,
        int32_t bufferSize,
        int32_t valueSize,
        int32_t* offsetArr,
        int32_t dictSize,
        int32_t stringBodySize,
        std::shared_ptr<LargeStringContainer<std::string_view>> stringContainer)
    {
        // nullData
        auto nullData = UnsafeBaseVector::GetNulls(baseVector);
        // Nulls belong to the dictionary vector (ids), not to the dictionary
        // string container. dictSize and valueSize are allowed to differ.
        auto nullByteSize = omniruntime::vec::NullsBuffer::CalculateNbytes(valueSize);

        auto ret = memcpy_s(buffer, bufferSize, nullData, nullByteSize);
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += nullByteSize;

        // offset array
        ret = memcpy_s(buffer, bufferSize, offsetArr, sizeof(int32_t) * (dictSize + 1));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += sizeof(int32_t) * (dictSize + 1);

        // real data
        std::string dataStr(UnsafeStringContainer::GetStringBufferAddr(stringContainer.get()), stringBodySize);
        ret = memcpy_s(buffer, bufferSize, dataStr.data(), stringBodySize * sizeof(char));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += stringBodySize * sizeof(char);
    }

    static void SerializWatermark(long timestamp, int32_t dataSize, uint8_t*& buffer)
    {
        errno_t ret;
        int dataType = static_cast<int>(StreamElementTag::TAG_WATERMARK);
        // dataType--timestamp
        ret = memcpy_s(buffer, dataSize, &dataType, sizeof(int8_t));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += 1;
        ret = memcpy_s(buffer, dataSize, &timestamp, sizeof(timestamp));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
        buffer += sizeof(timestamp);
    }

    static void SerializElementNum(int32_t elementNumber, uint8_t* buffer)
    {
        errno_t ret;
        // elementNum
        ret = memcpy_s(buffer, sizeof(int32_t), &elementNumber, sizeof(int32_t));
        if (ret != EOK) {
            throw std::runtime_error("memcpy_s failed");
        }
    }
};

} // namespace omnistream
