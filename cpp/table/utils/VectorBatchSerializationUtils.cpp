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

#include "VectorBatchSerializationUtils.h"

omnistream::SerializedBatchInfo omnistream::VectorBatchSerializationUtils::serializeVectorBatch(
    VectorBatch* vectorBatch, int32_t batchSize, uint8_t*& buffer)
{
    errno_t ret;
    int32_t vectorCount = vectorBatch->GetVectorCount();
    int32_t rowCnt = vectorBatch->GetRowCount();
    uint8_t* original = buffer;
    int8_t dataType = static_cast<int>(StreamElementTag::VECTOR_BATCH);
    // dataType--batchSize--vectorCount--rowCnt
    ret = memcpy_s(buffer, batchSize, &dataType, sizeof(int8_t));
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += sizeof(int8_t);

    ret = memcpy_s(buffer, batchSize, &batchSize, sizeof(batchSize));
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += sizeof(batchSize);

    ret = memcpy_s(buffer, batchSize, &vectorCount, sizeof(vectorCount));
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += sizeof(vectorCount);

    ret = memcpy_s(buffer, batchSize, &rowCnt, sizeof(rowCnt));
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += sizeof(rowCnt);

    // serialize timestamp and rowKinds
    serializeTimestampAndRowKinds(vectorBatch, buffer, batchSize);

    // serialize vector data
    for (int idx = 0; idx < vectorCount; idx++) {
        BaseVector* colVector = vectorBatch->Get(idx);
        DataTypeId dataType = colVector->GetTypeId();
        Encoding encoding = colVector->GetEncoding();
        if (encoding == OMNI_FLAT) {
            if (dataType == OMNI_CHAR || dataType == OMNI_VARCHAR) {
                serializeCharVector(colVector, buffer, batchSize);
            } else {
                serializePrimitiveVector(colVector, buffer, batchSize);
            }
        } else if (encoding == OMNI_DICTIONARY) {
            if (dataType == OMNI_CHAR || dataType == OMNI_VARCHAR) {
                serializeStringDictionaryContainerVector(colVector, buffer, batchSize);
            } else {
                throw std::runtime_error("Encoding not supported");
            }
        } else {
            throw std::runtime_error("Encoding not supported");
        }
    }
    return {original, original, batchSize};
}

void omnistream::VectorBatchSerializationUtils::serializeTimestampAndRowKinds(
    omnistream::VectorBatch* vectorBatch, uint8_t*& buffer, int32_t bufferSize)
{
    int32_t rowCnt = vectorBatch->GetRowCount();
    int64_t* timestamps = vectorBatch->getTimestamps();
    RowKind* rowKinds = vectorBatch->getRowKinds();
    errno_t ret = memcpy_s(buffer, bufferSize, timestamps, sizeof(int64_t) * rowCnt);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += sizeof(int64_t) * rowCnt;
    ret = memcpy_s(buffer, bufferSize, rowKinds, sizeof(RowKind) * rowCnt);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += sizeof(RowKind) * rowCnt;
}

int32_t omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(
    omnistream::VectorBatch* vectorBatch)
{
    int32_t vectorCount = vectorBatch->GetVectorCount();
    int32_t rowCnt = vectorBatch->GetRowCount();
    // dataType--batchSize--vectorCount--rowCnt
    int32_t headerSize = sizeof(byte) + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t);
    int32_t timestampsSize = rowCnt * sizeof(int64_t);
    int32_t rowKindsSize = rowCnt * sizeof(RowKind);
    int32_t totalSize = headerSize + timestampsSize + rowKindsSize;

    for (int32_t i = 0; i < vectorCount; i++) {
        BaseVector* columnVector = vectorBatch->Get(i);
        totalSize += calculateVectorSerializableSize(columnVector);
    }
    return totalSize;
}

int32_t omnistream::VectorBatchSerializationUtils::calculateVectorSerializableSize(BaseVector* baseVector)
{
    int32_t totalSize = 0;

    if (baseVector == nullptr) {
        INFO_RELEASE("calculateVectorSerializableSize baseVector is nullptr.");
        throw std::runtime_error("calculateVectorSerializableSize baseVector is nullptr.");
    }
    int32_t size = baseVector->GetSize();
    DataTypeId dataType = baseVector->GetTypeId();

    Encoding encoding = baseVector->GetEncoding();
    // vectorSize--encoding--dataType
    int32_t vectorHeaderSize = sizeof(int32_t) + sizeof(int8_t) + sizeof(int8_t);

    auto nullByteSize = omniruntime::vec::NullsBuffer::CalculateNbytes(size);
    totalSize += nullByteSize + vectorHeaderSize;
    int32_t dataSize = 0;

    if (encoding == OMNI_FLAT) {
        if (dataType == OMNI_VARCHAR || dataType == OMNI_CHAR) {
            dataSize = calculateCharDataSize(baseVector);
        } else {
            dataSize = calculatePrimitiveDataSize(baseVector);
        }
    } else if (encoding == OMNI_DICTIONARY) {
        // todo should only exist for string data type
        dataSize = calculateStringDictionarySerializableSize(baseVector);
    } else {
        // todo
        throw std::runtime_error("Encoding not supported");
    }

    totalSize += dataSize;

    return totalSize;
}

int32_t omnistream::VectorBatchSerializationUtils::calculateVectorBatchPayloadSize(omnistream::VectorBatch* vectorBatch)
{
    int32_t vectorCount = vectorBatch->GetVectorCount();
    int32_t rowCnt = vectorBatch->GetRowCount();
    int32_t timestampsSize = rowCnt * sizeof(int64_t);
    int32_t rowKindsSize = rowCnt * sizeof(RowKind);
    int32_t totalSize = timestampsSize + rowKindsSize;

    for (int32_t i = 0; i < vectorCount; i++) {
        BaseVector* columnVector = vectorBatch->Get(i);
        totalSize += calculateVectorSerializableSize(columnVector);
    }
    return totalSize;
}

int32_t omnistream::VectorBatchSerializationUtils::calculatePrimitiveDataSize(BaseVector* baseVector)
{
    DataTypeId dataType = baseVector->GetTypeId();
    int32_t size = baseVector->GetSize();

    switch (dataType) {
        case OMNI_LONG:
        case OMNI_DATE64:
        case OMNI_TIME64:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64: return sizeof(int64_t) * size;

        case OMNI_INT:
        case OMNI_DATE32: return sizeof(int32_t) * size;

        case OMNI_SHORT: return sizeof(int16_t) * size;

        case OMNI_DOUBLE: return sizeof(double) * size;
        case OMNI_BOOLEAN: return sizeof(bool) * size;
        case OMNI_DECIMAL128: return sizeof(Decimal128) * size;

        default:
            LOG("Data type not supported: " << std::to_string(dataType));
            throw std::runtime_error("Data type not supported");
    }
}

int32_t omnistream::VectorBatchSerializationUtils::calculateCharDataSize(BaseVector* baseVector)
{
    Vector<LargeStringContainer<std::string_view>>* charVector =
        dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(baseVector);
    if (charVector == nullptr) {
        throw std::runtime_error("Failed to cast BaseVector to Vector<LargeStringContainer<std::string_view>>");
    }

    int32_t rowCount = baseVector->GetSize();
    int32_t stringBodySizeField = sizeof(int32_t);
    int32_t offsetSize = (rowCount + 1) * sizeof(int32_t);
    int32_t dataSize = 0;
    std::shared_ptr<LargeStringContainer<std::string_view>> stringContainer =
        UnsafeStringVector::GetContainer(charVector);
    int32_t* offsetArr = UnsafeStringContainer::GetOffsets(stringContainer.get());

    dataSize = offsetArr[rowCount];
    return stringBodySizeField + offsetSize + dataSize * sizeof(char);
}

void omnistream::VectorBatchSerializationUtils::serializePrimitiveVector(
    BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
{
    DataTypeId dataType = baseVector->GetTypeId();
    int32_t rowCount = baseVector->GetSize();
    serializeVectorBatchHeader(baseVector, buffer, bufferSize);
    auto nullData = UnsafeBaseVector::GetNulls(baseVector);
    auto nullByteSize = omniruntime::vec::NullsBuffer::CalculateNbytes(rowCount);
    auto ret = memcpy_s(buffer, bufferSize, nullData, nullByteSize);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += nullByteSize;

    switch (dataType) {
        case OMNI_LONG:
        case OMNI_DATE64:
        case OMNI_TIME64:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64: serializeInt64(baseVector, buffer, bufferSize); break;

        case OMNI_INT:
        case OMNI_DATE32: serializeInt32(baseVector, buffer, bufferSize); break;

        case OMNI_SHORT: serializeInt16(baseVector, buffer, bufferSize); break;

        case OMNI_DOUBLE: serializeDouble(baseVector, buffer, bufferSize); break;
        case OMNI_BOOLEAN: serializeBool(baseVector, buffer, bufferSize); break;
        case OMNI_DECIMAL128: serializeDecimal128(baseVector, buffer, bufferSize); break;
        default:
            LOG("Data type not supported: " << std::to_string(dataType));
            throw std::runtime_error("Data type not supported");
    }
}

void omnistream::VectorBatchSerializationUtils::serializeCharVector(
    BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
{
    serializeVectorBatchHeader(baseVector, buffer, bufferSize);

    Vector<LargeStringContainer<std::string_view>>* charVector =
        dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(baseVector);
    if (!charVector) {
        GErrorLog("Failed to cast BaseVector to Vector<LargeStringContainer<std::string_view>>");
        throw std::runtime_error("Failed to cast BaseVector to Vector<LargeStringContainer<std::string_view>>");
    }

    std::shared_ptr<LargeStringContainer<std::string_view>> stringContainer =
        UnsafeStringVector::GetContainer(charVector);
    int32_t* offsetArr = UnsafeStringContainer::GetOffsets(stringContainer.get());

    int32_t rowCount = baseVector->GetSize();

    // real data size
    int32_t stringBodySize = offsetArr[charVector->GetSize()];
    auto ret = memcpy_s(buffer, bufferSize, &stringBodySize, sizeof(int32_t));
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += sizeof(int32_t);

    // nullData
    auto nullData = UnsafeBaseVector::GetNulls(baseVector);
    auto nullByteSize = omniruntime::vec::NullsBuffer::CalculateNbytes(rowCount);
    ret = memcpy_s(buffer, bufferSize, nullData, nullByteSize);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += nullByteSize;

    // offset array
    ret = memcpy_s(buffer, bufferSize, offsetArr, sizeof(int32_t) * (rowCount + 1));
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += sizeof(int32_t) * (rowCount + 1);

    // real data
    std::string dataStr(UnsafeStringContainer::GetStringBufferAddr(stringContainer.get()), stringBodySize);
    ret = memcpy_s(buffer, bufferSize, dataStr.data(), stringBodySize * sizeof(char));
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += stringBodySize * sizeof(char);
}

void omnistream::VectorBatchSerializationUtils::serializeInt64(
    BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
{
    Vector<int64_t>* vector64 = static_cast<Vector<int64_t>*>(baseVector);
    int64_t* data = UnsafeVector::GetRawValues(vector64);
    int32_t size = sizeof(int64_t) * baseVector->GetSize();
    auto ret = memcpy_s(buffer, bufferSize, data, size);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += size;
}

void omnistream::VectorBatchSerializationUtils::serializeInt32(
    BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
{
    Vector<int32_t>* vector32 = static_cast<Vector<int32_t>*>(baseVector);
    int32_t* data = UnsafeVector::GetRawValues(vector32);
    int32_t size = sizeof(int32_t) * baseVector->GetSize();
    auto ret = memcpy_s(buffer, bufferSize, data, size);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += size;
}

void omnistream::VectorBatchSerializationUtils::serializeInt16(
    BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
{
    Vector<int16_t>* vector16 = static_cast<Vector<int16_t>*>(baseVector);
    int16_t* data = UnsafeVector::GetRawValues(vector16);
    int32_t size = sizeof(int16_t) * baseVector->GetSize();
    auto ret = memcpy_s(buffer, bufferSize, data, size);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += size;
}

void omnistream::VectorBatchSerializationUtils::serializeDouble(
    BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
{
    Vector<double>* vectorDouble = static_cast<Vector<double>*>(baseVector);
    double* data = UnsafeVector::GetRawValues(vectorDouble);
    int32_t size = sizeof(double) * baseVector->GetSize();
    auto ret = memcpy_s(buffer, bufferSize, data, size);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += size;
}

void omnistream::VectorBatchSerializationUtils::serializeBool(
    BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
{
    Vector<bool>* vectorBool = static_cast<Vector<bool>*>(baseVector);
    bool* data = UnsafeVector::GetRawValues(vectorBool);
    int32_t size = sizeof(bool) * baseVector->GetSize();
    auto ret = memcpy_s(buffer, bufferSize, data, size);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += size;
}

void omnistream::VectorBatchSerializationUtils::serializeDecimal128(
    BaseVector* baseVector, uint8_t*& buffer, int32_t bufferSize)
{
    Vector<Decimal128>* vectorDecimal128 = static_cast<Vector<Decimal128>*>(baseVector);
    Decimal128* data = UnsafeVector::GetRawValues(vectorDecimal128);
    int32_t size = sizeof(Decimal128) * baseVector->GetSize();
    auto ret = memcpy_s(buffer, bufferSize, data, size);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    buffer += size;
}

int32_t omnistream::VectorBatchSerializationUtils::calculateStringDictionarySerializableSize(BaseVector* baseVector)
{
    int32_t stringDictionarySize = 0;
    auto* vec_string_dict_container =
        reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>>*>(baseVector);
    if (!vec_string_dict_container) {
        throw std::runtime_error("Encoding not supported");
    }
    int32_t size = baseVector->GetSize();

    int32_t valueIndexSize = sizeof(int32_t) * size;
    stringDictionarySize += valueIndexSize;

    // dictOffSetSize --- dictSize
    stringDictionarySize += sizeof(int32_t) + sizeof(int32_t);

    std::shared_ptr<DictionaryContainer<std::string_view>> string_dictionary =
        static_cast<std::shared_ptr<DictionaryContainer<std::string_view>>>(
            unsafe::UnsafeDictionaryVector::GetDictionaryOriginal(vec_string_dict_container));
    std::shared_ptr<LargeStringContainer<std::string_view>> stringContainer =
        unsafe::UnsafeDictionaryContainer::GetStringDictionaryOriginal(string_dictionary.get());
    int32_t dictSize = unsafe::UnsafeDictionaryContainer::GetDictSize(string_dictionary.get());
    int bodySize = calculateLargeStringContainerDataSize(stringContainer, dictSize);
    stringDictionarySize += bodySize;

    return stringDictionarySize;
}
