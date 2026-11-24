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

#ifndef OMNISTREAM_BSSSTATETABLE_H
#define OMNISTREAM_BSSSTATETABLE_H
#ifdef WITH_OMNISTATESTORE

#include <unordered_map>
#include <vector>

#include "typeutils/LongSerializer.h"
#include "memory/DataInputDeserializer.h"
#include "utils/VectorBatchSerializationUtils.h"
#include "utils/VectorBatchDeserializationUtils.h"
#include "boost_state_table.h"
#include "boost_state_db.h"
#include "table_description.h"
#include "state/HashCode.h"
#include "data/util/SequenceNumberHelper.h"

template <typename K, typename N, typename S>
class BssStateTable {
public:
    BssStateTable(
        InternalKeyContext<K>* keyContext,
        RegisteredKeyValueStateBackendMetaInfo* metaInfo,
        TypeSerializer* keySerializer)
        : keyContext(keyContext),
          metaInfo(metaInfo),
          keySerializer(keySerializer),
          sequenceNumberHelper(keyContext->getNumberOfKeyGroups()) {};

    bool isEmpty()
    {
        return size == 0;
    }

    void createTable(ock::bss::BoostStateDBPtr& _dbPtr)
    {
        this->dbPtr = _dbPtr;
        auto tblDesc = std::make_shared<ock::bss::TableDescription>(
            ock::bss::StateType::VALUE, "dbTable", -1, ock::bss::TableSerializer{}, dbPtr->GetConfig());
        dbTable = std::dynamic_pointer_cast<ock::bss::KVTable>(_dbPtr->GetTableOrCreate(tblDesc));
    };

    S get(N& nameSpace)
    {
        LOG("bss state table get");
        uint32_t keyHashCode;

        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        ock::bss::BinaryData readValue;
        auto res = dbTable->Get(keyHashCode, priKey, readValue);
        if (res != ock::bss::BSS_OK || readValue.Length() == 0) {
            if constexpr (std::is_pointer_v<S>) {
                return nullptr;
            } else {
                return std::numeric_limits<S>::max();
            }
        }
        DataInputDeserializer serializedData(reinterpret_cast<const uint8_t*>(readValue.Data()), readValue.Length(), 0);
        void* resPtr = getStateSerializer()->deserialize(serializedData);
        if constexpr (std::is_pointer_v<S>) {
            return (S)resPtr;
        } else {
            return resPtr == nullptr ? std::numeric_limits<S>::max() : *(S*)resPtr;
        }
    }

    void put(N& nameSpace, const S& state)
    {
        LOG("BSS state table put");
        uint32_t keyHashCode;
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        TypeSerializer* vSerializer = getStateSerializer();
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
        S tmpS = state;
        if constexpr (std::is_same_v<S, int64_t> || std::is_same_v<S, int32_t>) {
            LongSerializer::INSTANCE->serialize(&tmpS, valueOutputSerializer);
        } else if constexpr (std::is_pointer_v<S>) {
            vSerializer->serialize(tmpS, valueOutputSerializer);
        } else {
            vSerializer->serialize(&tmpS, valueOutputSerializer);
        }
        ock::bss::BinaryData priValue(
            valueOutputSerializer.getData(), static_cast<int32_t>(valueOutputSerializer.getPosition()));
        auto res = dbTable->Put(keyHashCode, priKey, priValue);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: put failed");
        }
    }

    void clear(N& nameSpace)
    {
        LOG("BSS state table clear");
        uint32_t keyHashCode;
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        auto res = dbTable->Remove(keyHashCode, priKey);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: clear failed");
        }
    }

    void add(N& nameSpace, const S& value)
    {
        LOG("BSS state table add");
        uint32_t keyHashCode;
        DataOutputSerializer serializer;
        OutputBufferStatus outputBufferStatus;
        serializer.setBackendBuffer(&outputBufferStatus);
        ock::bss::BinaryData priKey = GetPriBinaryData(nameSpace, keyHashCode, serializer);
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
        S tmpS = value;
        TypeSerializer* vSerializer = getStateSerializer();
        if constexpr (std::is_same_v<S, int64_t> || std::is_same_v<S, int32_t>) {
            LongSerializer::INSTANCE->serialize(&tmpS, valueOutputSerializer);
        } else if constexpr (std::is_pointer_v<S>) {
            vSerializer->serialize(tmpS, valueOutputSerializer);
        } else {
            vSerializer->serialize(&tmpS, valueOutputSerializer);
        }
        ock::bss::BinaryData priVal(
            valueOutputSerializer.getData(), static_cast<int32_t>(valueOutputSerializer.getPosition()));
        auto res = dbTable->Add(keyHashCode, priKey, priVal);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: add failed");
        }
    }

    ock::bss::BinaryData GetPriBinaryData(N& nameSpace, uint32_t& keyHashCode, DataOutputSerializer& serializer)
    {
        auto currentKey = keyContext->getCurrentKey();
        // serialize key
        if constexpr (std::is_same_v<K, int64_t> || std::is_same_v<K, int32_t>) {
            LongSerializer::INSTANCE->serialize(&currentKey, serializer);
        } else if constexpr (std::is_pointer_v<K>) {
            keySerializer->serialize(currentKey, serializer);
        } else {
            keySerializer->serialize(&currentKey, serializer);
        }
        // serialize
        if constexpr (std::is_same_v<N, int64_t> || std::is_same_v<N, int32_t>) {
            LongSerializer::INSTANCE->serialize(&nameSpace, serializer);
        } else if constexpr (std::is_pointer_v<N>) {
            getNamespaceSerializer()->serialize(nameSpace, serializer);
        } else {
            getNamespaceSerializer()->serialize(&nameSpace, serializer);
        }
        ock::bss::BinaryData priBinaryData(serializer.getData(), static_cast<int32_t>(serializer.getPosition()));
        keyHashCode = HashCode::Hash(serializer.getData(), static_cast<int32_t>(serializer.getPosition()));
        return priBinaryData;
    }

    uint32_t getNextSequenceNumber(int32_t keyGroup)
    {
        return sequenceNumberHelper.getNextSequenceNumber(keyGroup);
    }

    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
    {
        auto sequenceNumber = sequenceNumberHelper.getNextSequenceNumber(keyGroup);
        LOG("Bss state table addVectorBatch");
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        keyOutputSerializer.writeShort(static_cast<uint16_t>(keyGroup));
        LongSerializer longSerializer;
        long sequenceNumberForSerializer = sequenceNumber;
        longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);
        ock::bss::BinaryData priKey(
            keyOutputSerializer.getData(), static_cast<int32_t>(keyOutputSerializer.getPosition()));
        uint32_t keyHashCode =
            HashCode::Hash(keyOutputSerializer.getData(), static_cast<int32_t>(keyOutputSerializer.getPosition()));
        int batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
        std::vector<uint8_t> bufferStorage(batchSize);
        auto* buffer = bufferStorage.data();
        omnistream::SerializedBatchInfo serializedBatchInfo =
            omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
        ock::bss::BinaryData priVal(serializedBatchInfo.dataAddress, serializedBatchInfo.dataSize);
        auto res = dbTable->Put(keyHashCode, priKey, priVal);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: addVectorBatch failed");
            return;
        }
        sequenceNumberHelper.addNextSequenceNumber(keyGroup);
    }

    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
    {
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;
        for (const auto& [keyGroup, vectorBatch] : vectorBatchByKeyGroup) {
            keyOutputSerializer.clear();
            auto sequenceNumber = sequenceNumberHelper.getNextSequenceNumber(keyGroup);
            keyOutputSerializer.writeShort(static_cast<uint16_t>(keyGroup));
            long sequenceNumberForSerializer = sequenceNumber;
            longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);
            ock::bss::BinaryData priKey(
                keyOutputSerializer.getData(), static_cast<int32_t>(keyOutputSerializer.getPosition()));
            auto keyHashCode = HashCode::Hash(priKey.Data(), static_cast<int32_t>(priKey.Length()));
            auto batchSize =
                omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
            std::vector<uint8_t> bufferStorage(batchSize);
            auto serializedBatchInfo = omnistream::VectorBatchSerializationUtils::serializeVectorBatch(
                vectorBatch, batchSize, bufferStorage.data());
            ock::bss::BinaryData priVal(serializedBatchInfo.buffer, serializedBatchInfo.size);
            if (dbTable->Put(keyHashCode, priKey, priVal) != ock::bss::BSS_OK) {
                THROW_RUNTIME_ERROR("Failed to add VectorBatch to BSS for keyGroup " << keyGroup);
            }
            sequenceNumberHelper.addNextSequenceNumber(keyGroup);
        }
    }

    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
    {
        LOG("Bss state table getVectorBatch");
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        keyOutputSerializer.writeShort(static_cast<uint16_t>(keyGroup));
        LongSerializer longSerializer;
        long sequenceNumberForSerializer = sequenceNumber;
        longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);
        uint32_t keyHashCode =
            HashCode::Hash(keyOutputSerializer.getData(), static_cast<int32_t>(keyOutputSerializer.getPosition()));
        ock::bss::BinaryData priKey(
            keyOutputSerializer.getData(), static_cast<uint32_t>(keyOutputSerializer.getPosition()));

        ock::bss::BinaryData priVal;
        auto res = dbTable->Get(keyHashCode, priKey, priVal);
        if (res != ock::bss::BSS_OK) {
            LOG("Warning: getVectorBatch failed");
            return nullptr;
        }
        uint8_t* address = const_cast<uint8_t*>(priVal.Data() + sizeof(int8_t));
        auto batch = omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(address);
        return batch;
    }

    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup)
    {
        NOT_IMPL_EXCEPTION;
    }

    void clearVectorBatches(int64_t currentTimestamp)
    {
        auto* keyGroupRange = keyContext->getKeyGroupRange();
        for (int32_t keyGroup = keyGroupRange->getStartKeyGroup(); keyGroup <= keyGroupRange->getEndKeyGroup();
             ++keyGroup) {
            auto nextSequenceNumber = getNextSequenceNumber(keyGroup);
            for (uint32_t sequenceNumber = 0; sequenceNumber < nextSequenceNumber; ++sequenceNumber) {
                auto* vectorBatch = getVectorBatch(keyGroup, sequenceNumber);
                if (vectorBatch != nullptr && vectorBatch->isEmpty(currentTimestamp)) {
                    removeVectorBatch(keyGroup, sequenceNumber);
                }
                delete vectorBatch;
            }
        }
    }

    void clearVectorBatches(int32_t keyGroup, const std::vector<uint32_t>& sequenceNumbersToDelete)
    {
        for (auto sequenceNumber : sequenceNumbersToDelete) {
            removeVectorBatch(keyGroup, sequenceNumber);
        }
    }

    TypeSerializer* getNamespaceSerializer()
    {
        return metaInfo->getNamespaceSerializer();
    }

    TypeSerializer* getStateSerializer()
    {
        return metaInfo->getStateSerializer();
    }

    RegisteredKeyValueStateBackendMetaInfo* getMetaInfo()
    {
        return metaInfo;
    }

    void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo* newMetaInfo)
    {
        metaInfo = newMetaInfo;
    }

protected:
    void removeVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
    {
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        keyOutputSerializer.writeShort(static_cast<uint16_t>(keyGroup));
        LongSerializer longSerializer;
        long sequenceNumberForSerializer = sequenceNumber;
        longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);
        ock::bss::BinaryData priKey(
            keyOutputSerializer.getData(), static_cast<uint32_t>(keyOutputSerializer.getPosition()));
        auto keyHashCode = HashCode::Hash(priKey.Data(), static_cast<int32_t>(priKey.Length()));
        dbTable->Remove(keyHashCode, priKey);
    }

    InternalKeyContext<K>* keyContext;
    RegisteredKeyValueStateBackendMetaInfo* metaInfo;
    TypeSerializer* keySerializer;
    ock::bss::BoostStateDBPtr dbPtr;
    ock::bss::ConfigRef config;
    ock::bss::KVTableRef dbTable;
    int size = 0;
    omnistream::SequenceNumberHelper sequenceNumberHelper{}; // only used for VectorBatch storage
};

#endif // OMNISTREAM_BSSSTATETABLE_H
#endif
