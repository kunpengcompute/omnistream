/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <vector>
#include "table/data/vectorbatch/VectorBatch.h"
#include "../../data/binary/BinaryRowData.h"
#include "typeinfo/TypeInformation.h"
#include "table/data/rowdata_marshaller.h"
#include "core/utils/key_type_traits.h"
#include "core/utils/type_traits_ext.h"
#include "table/data/StringRefUtil.h"
#include "OmniOperatorJIT/core/src/operator/hashmap/vector_marshaller.h"
#include "runtime/plugable/SerializationDelegate.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"

using namespace omniruntime::vec;

template <typename K>
class KeySelector {
public:
    explicit KeySelector(const std::vector<int32_t>& keyColTypeIds, const std::vector<int32_t>& keyCols);

    KeySelector() = default;

    KeySelector(const KeySelector&) = delete;

    KeySelector& operator=(const KeySelector&) = delete;

    KeySelector(KeySelector&& other) noexcept
        : arenaAllocator(std::move(other.arenaAllocator)),
          keyColTypeIds(std::move(other.keyColTypeIds)),
          keyColIndices(std::move(other.keyColIndices)),
          outIndices(std::move(other.outIndices)),
          serializers(std::move(other.serializers)),
          deserializers(std::move(other.deserializers)),
          rowSerializers(std::move(other.rowSerializers)),
          rowDeserializers(std::move(other.rowDeserializers)),
          reusedKey(other.reusedKey),
          m_canReuseKey(other.m_canReuseKey)
    {
        other.reusedKey = nullptr;
    }

    KeySelector& operator=(KeySelector&& other) noexcept
    {
        if (this != &other) {
            if (reusedKey != nullptr) {
                delete reusedKey;
            }
            arenaAllocator = std::move(other.arenaAllocator);
            keyColTypeIds = std::move(other.keyColTypeIds);
            keyColIndices = std::move(other.keyColIndices);
            outIndices = std::move(other.outIndices);
            serializers = std::move(other.serializers);
            deserializers = std::move(other.deserializers);
            rowSerializers = std::move(other.rowSerializers);
            rowDeserializers = std::move(other.rowDeserializers);
            reusedKey = other.reusedKey;
            m_canReuseKey = other.m_canReuseKey;
            other.reusedKey = nullptr;
        }
        return *this;
    }

    ~KeySelector();

    K getKey(omnistream::VectorBatch* inputBatch, int row, bool enableKeyReuse = false);

    K getKey(IOReadableWritable* record);

    K getKey(RowData* input);

    void fillKeyToVectorBatch(
        StringRef& ref,
        omnistream::VectorBatch* outputBatch,
        int row,
        const std::vector<int32_t>& outColIndex = std::vector<int32_t>());

    inline bool isAnyKeyNull(omnistream::VectorBatch* inputBatch, int row);

    void reset()
    {
        // This clears all memory chunks except the first one.
        arenaAllocator.Reset();
    };

    bool canReuseKey();

    static constexpr bool isRowKey_ = KeyTypeTraits<K>::isRowKey;
    static constexpr bool isSharedRowKey_ = KeyTypeTraits<K>::isSharedRowKey;

private:
    omniruntime::mem::SimpleArenaAllocator arenaAllocator;
    std::vector<int32_t> keyColTypeIds;
    std::vector<int32_t> keyColIndices;
    std::vector<int32_t> outIndices; // If need to deserialize StringRef key into output vectorbatch

    // These are the function pointers for StringRef
    std::vector<omniruntime::op::VectorSerializer> serializers;
    std::vector<omniruntime::op::VectorDeSerializer> deserializers;
    // Varchar is singled out cause it has the possibility of being a dictionary vector
    static void VarcharSerializer(
        BaseVector* baseVector,
        int32_t rowIdx,
        omniruntime::mem::SimpleArenaAllocator& arenaAllocator,
        omniruntime::type::StringRef& result);

    // These are the function pointers for BinaryRowData
    std::vector<VBToRowSerializer> rowSerializers;
    std::vector<RowToVBDeSerializer> rowDeserializers;

    BinaryRowData* reusedKey = nullptr;
    // Only switches to true when `isRowKey_` is true and none of the `keyColTypeIds` is CHAR/VARCHAR
    bool m_canReuseKey = false;
};

template <typename K>
K KeySelector<K>::getKey(omnistream::VectorBatch* inputBatch, int row, bool enableKeyReuse)
{
    if constexpr (std::is_same_v<K, StringRef>) {
        omniruntime::type::StringRef strRef;
        for (int i = 0; i < keyColIndices.size(); ++i) {
            serializers[i](inputBatch->Get(keyColIndices[i]), row, arenaAllocator, strRef);
        }
        return strRef;
    } else if constexpr (isRowKey_) {
        BinaryRowData* key;
        if (enableKeyReuse and m_canReuseKey) {
            key = reusedKey;
        } else {
            key = BinaryRowData::createBinaryRowDataWithMem(keyColTypeIds.size());
        }

        for (size_t i = 0; i < keyColIndices.size(); ++i) {
            rowSerializers[i](inputBatch->Get(keyColIndices[i]), row, key, i);
        }
        return key;
    } else if constexpr (isSharedRowKey_) {
        auto key = std::shared_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(keyColTypeIds.size()));
        for (size_t i = 0; i < keyColIndices.size(); ++i) {
            rowSerializers[i](inputBatch->Get(keyColIndices[i]), row, key.get(), i);
        }
        return key;
    } else {
        if (keyColIndices.size() != 1) {
            THROW_LOGIC_EXCEPTION("KeySelector getKey: keyColIndices size is not 1");
        }
        switch (keyColTypeIds[0]) {
            case OMNI_LONG:
            case OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case OMNI_TIMESTAMP:
                return reinterpret_cast<vec::Vector<int64_t>*>(inputBatch->Get(keyColIndices[0]))->GetValue(row);
                break;
            case OMNI_INT:
                return reinterpret_cast<vec::Vector<int32_t>*>(inputBatch->Get(keyColIndices[0]))->GetValue(row);
                break;
            default: THROW_LOGIC_EXCEPTION("Key type not supported, typeId: " << keyColTypeIds[0]);
        }
    }
}

// todo: 跟上面的VB版本对齐
template <typename K>
K KeySelector<K>::getKey(RowData* input)
{
    if constexpr (std::is_same_v<StringRef, K>) {
        NOT_IMPL_EXCEPTION;
    } else if constexpr (isRowKey_) {
        BinaryRowData* key = BinaryRowData::createBinaryRowDataWithMem(keyColTypeIds.size());
        for (size_t i = 0; i < keyColIndices.size(); ++i) {
            switch (keyColTypeIds[i]) {
                case OMNI_LONG:
                case OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case OMNI_TIMESTAMP: key->setLong(i, input->getLong(keyColIndices[i])); break;
                case OMNI_VARCHAR: {
                    std::string_view sv = input->getStringView(keyColIndices[i]);
                    key->setStringView(i, sv);
                    break;
                }
                case OMNI_INT: key->setInt(i, *input->getInt(keyColIndices[i])); break;
                default: THROW_LOGIC_EXCEPTION("Key type not supported, typeId: " << keyColTypeIds[i]);
            }
        }
        return key;
    } else {
        if (keyColIndices.size() != 1) {
            THROW_LOGIC_EXCEPTION("KeySelector getKey: keyColIndices size is not 1");
        }
        switch (keyColTypeIds[0]) {
            case OMNI_LONG:
            case OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case OMNI_TIMESTAMP: return *input->getLong(keyColIndices[0]); break;
            case OMNI_INT: return *input->getInt(keyColIndices[0]); break;
            default: THROW_LOGIC_EXCEPTION("Key type not supported, typeId: " << keyColTypeIds[0]);
        }
    }
}

template <typename K>
bool KeySelector<K>::isAnyKeyNull(omnistream::VectorBatch* inputBatch, int row)
{
    const auto& vecs = inputBatch->GetVectors();
    return std::any_of(
        keyColIndices.begin(), keyColIndices.end(), [&](int colIndex) { return vecs[colIndex]->IsNull(row); });
}

template <typename K>
KeySelector<K>::KeySelector(const std::vector<int32_t>& keyColTypeIds, const std::vector<int32_t>& keyCols)
    : keyColTypeIds(keyColTypeIds),
      keyColIndices(keyCols)
{
    LOG("Create key selector");
    if constexpr (std::is_same_v<K, StringRef>) {
        for (int i = 0; i < keyColTypeIds.size(); i++) {
            auto typeId = keyColTypeIds[i];
            if (typeId == omniruntime::type::OMNI_VARCHAR || typeId == omniruntime::type::OMNI_CHAR) {
                // varchar and char has the possibility of being a dictionary vector due to StreamCalc filter
                serializers.push_back(this->VarcharSerializer);
                deserializers.push_back(omniruntime::op::vectorDeSerializerCenter[typeId]);
            } else if (
                typeId == omniruntime::type::OMNI_TIME_WITHOUT_TIME_ZONE ||
                typeId == omniruntime::type::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE ||
                typeId == omniruntime::type::OMNI_TIMESTAMP_WITH_TIME_ZONE ||
                typeId == omniruntime::type::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                // TIME/TIMESTAMP variants are stored as int64 millis and share the long serializer.
                serializers.push_back(omniruntime::op::vectorSerializerCenter[omniruntime::type::OMNI_LONG]);
                deserializers.push_back(omniruntime::op::vectorDeSerializerCenter[omniruntime::type::OMNI_LONG]);
            } else if (typeId < omniruntime::type::OMNI_INVALID) {
                // If it is one of the old omniruntime types
                serializers.push_back(omniruntime::op::vectorSerializerCenter[typeId]);
                deserializers.push_back(omniruntime::op::vectorDeSerializerCenter[typeId]);
            } else {
                THROW_LOGIC_EXCEPTION("Key type not supported, typeId: " << typeId);
            }
        }
    } else if constexpr (isRowKey_ || isSharedRowKey_) {
        if (isRowKey_) {
            m_canReuseKey = true;
        }
        for (size_t i = 0; i < keyColTypeIds.size(); i++) {
            auto typeId = keyColTypeIds[i];
            LOG("Key typeId: " << typeId);
            if (typeId == omniruntime::type::OMNI_VARCHAR || typeId == omniruntime::type::OMNI_CHAR) {
                m_canReuseKey = false;
            }
            const int typeIdInt = static_cast<int>(typeId);
            if (typeId == omniruntime::type::OMNI_TIME_WITHOUT_TIME_ZONE ||
                typeId == omniruntime::type::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE ||
                typeId == omniruntime::type::OMNI_TIMESTAMP_WITH_TIME_ZONE ||
                typeId == omniruntime::type::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                // TIME/TIMESTAMP variants are stored as int64 millis and share the long serializer.
                rowSerializers.push_back(rowSerializerCenter[omniruntime::type::OMNI_LONG]);
                rowDeserializers.push_back(rowDeserializerCenter[omniruntime::type::OMNI_LONG]);
            } else if (
                typeIdInt >= 0 && typeIdInt < static_cast<int>(rowSerializerCenter.size()) &&
                rowSerializerCenter[typeIdInt] != nullptr) {
                rowSerializers.push_back(rowSerializerCenter[typeIdInt]);
                rowDeserializers.push_back(rowDeserializerCenter[typeIdInt]);
            } else {
                THROW_LOGIC_EXCEPTION("Key type not supported, typeId: " << typeIdInt);
            }
        }

        if (m_canReuseKey) {
            reusedKey = BinaryRowData::createBinaryRowDataWithMem(keyColTypeIds.size());
        }
    }
}

template <typename K>
KeySelector<K>::~KeySelector()
{
    if (reusedKey != nullptr) {
        delete reusedKey;
        reusedKey = nullptr;
    }
}

template <typename K>
K KeySelector<K>::getKey(IOReadableWritable* record)
{
    SerializationDelegate* serializationDelegate = reinterpret_cast<SerializationDelegate*>(record);
    StreamRecord* streamRecord = reinterpret_cast<StreamRecord*>(serializationDelegate->getInstance());
    return reinterpret_cast<K>(streamRecord->getValue());
}

// This function is specific to StringRef
template <typename K>
void KeySelector<K>::VarcharSerializer(
    BaseVector* baseVector, int32_t rowIdx, SimpleArenaAllocator& arenaAllocator, StringRef& result)
{
    if (baseVector->GetEncoding() == OMNI_FLAT) {
        omniruntime::op::vectorSerializerCenter[omniruntime::type::OMNI_VARCHAR](
            baseVector, rowIdx, arenaAllocator, result);
    } else {
        omniruntime::op::dicVectorSerializerCenter[omniruntime::type::OMNI_VARCHAR](
            baseVector, rowIdx, arenaAllocator, result);
    }
}
template <typename K>
void KeySelector<K>::fillKeyToVectorBatch(
    StringRef& ref, omnistream::VectorBatch* outputBatch, int row, const std::vector<int32_t>& outColIndex)
{
    auto ptr = ref.data;
    if (outColIndex.empty()) {
        // Start filling from col0
        for (int i = 0; i < outputBatch->GetVectorCount(); i++) {
            ptr = deserializers[i](outputBatch->Get(i), row, ptr);
        }
    } else {
        // fill into a select set of collumns
        for (int i = 0; i < outColIndex.size(); i++) {
            ptr = deserializers[i](outputBatch->Get(outColIndex[i]), row, ptr);
        }
    }
}

template <typename K>
bool KeySelector<K>::canReuseKey()
{
    return m_canReuseKey;
}
