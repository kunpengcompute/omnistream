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
#include <unordered_set>
#include <type_traits>
#include <tuple>
#include <functional>
#include "core/typeutils/TypeSerializer.h"
#include "../internal/InternalKvState.h"
#include "../InternalKeyContext.h"
#include "../KeyGroupRange.h"
#include "../RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/binary/BinaryRowData.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/LongSerializer.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "core/include/emhash7.hpp"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "../RocksDBConfigurableOptions.h"
#include "runtime/state/CompositeKeySerializationUtils.h"

#include "basictypes/java_util_Iterator.h"
#include "basictypes/java_util_Map_Entry.h"
#include "utils/VectorBatchDeserializationUtils.h"
#include "utils/VectorBatchSerializationUtils.h"
#include "state/RocksDbKvStateInfo.h"
#include "runtime/state/DefaultConfigurableOptionsFactory.h"
#include "common.h"
#include <sstream>
#include "runtime/state/RocksIteratorWrapper.h"
#include "RocksDbOperationUtils.h"
#include <iomanip>

#include "RocksDbOperationUtils.h"
#include "data/util/SequenceNumberHelper.h"

/* S is the value used in the State,
 * like RowData* for HeapValueState,
 * emhash7<RowData*, int>* for HeapMapState,
 * vector<int64_t>* for List State
 */
template <typename K, typename N, typename UK, typename UV>
class RocksdbMapStateTable {
public:
    RocksdbMapStateTable(
        InternalKeyContext<K>* keyContext,
        std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo,
        TypeSerializer* keySerializer,
        TypeSerializer* userKeySerializer); // 加入db initData

    ~RocksdbMapStateTable();

    bool isEmpty()
    {
        // NOT_IMPL_EXCEPTION
        return size == 0;
    };

    void createTable(
        ROCKSDB_NAMESPACE::DB* db,
        const std::string& cfName,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        const std::function<ROCKSDB_NAMESPACE::ColumnFamilyOptions(const std::string&)>& columnFamilyOptionsFactory)
    {
        LOG("create MapState column family");
        this->rocksDb = db;
        ROCKSDB_NAMESPACE::ColumnFamilyOptions familyOptions = columnFamilyOptionsFactory(cfName);

        // [FALCON]-----------------------------------------------------------------------------------------------
        auto useRangeFilter = reinterpret_cast<Boolean*>(
            Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_RANGE_FILTER));
        auto prefixExtractorLength = reinterpret_cast<Integer*>(
            Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::PREFIX_EXTRACTOR_LENGTH));

        int prefixLen = 13;
        if (prefixExtractorLength != nullptr) {
            prefixLen = prefixExtractorLength->value;
            prefixExtractorLength->putRefCount();
        }

        if (useRangeFilter != nullptr && useRangeFilter->value) {
            // familyOptions.memtable_factory.reset(ROCKSDB_NAMESPACE::NewHashLinkListRepFactory());
            familyOptions.prefix_extractor.reset(ROCKSDB_NAMESPACE::NewCappedPrefixTransform(prefixLen));
            // familyOptions.compression = ROCKSDB_NAMESPACE::CompressionType::kZlibCompression;
            INFO_RELEASE("[FALCON] enable prefix for mapState, prefix length is " << prefixLen << ".");
        }

        if (useRangeFilter != nullptr) {
            useRangeFilter->putRefCount();
        }
        // [FALCON]-----------------------------------------------------------------------------------------------

        ROCKSDB_NAMESPACE::Status s;
        auto it1 = kvStateInformation->find(cfName);
        if (it1 != kvStateInformation->end() && it1->second->columnFamilyHandle_) {
            table = it1->second->columnFamilyHandle_;
        } else {
            s = db->CreateColumnFamily(familyOptions, cfName, &table);
            if (it1 != kvStateInformation->end()) {
                it1->second->setColumnFamilyHandle(table);
            }
        }
        auto it2 = kvStateInformation->find(cfName + "vb");
        if (it2 != kvStateInformation->end() && it2->second->columnFamilyHandle_) {
            VBTable = it2->second->columnFamilyHandle_;

            auto start = std::chrono::high_resolution_clock::now();
            restoreNextSequenceNumberByKeyGroup();
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            INFO_RELEASE("restoreNextSequenceNumberByKeyGroup costs " << duration << " ms.");
        } else {
            s = db->CreateColumnFamily(familyOptions, cfName + "vb", &VBTable);
            if (it2 != kvStateInformation->end()) {
                it2->second->setColumnFamilyHandle(VBTable);
            }
        }
        INFO_RELEASE("rocksdbMapStateTable createTable " << " cfName=" << cfName);
    }

    UV get(const N& nameSpace, const UK& userKey)
    {
        // 和Rocksdb交互的时候要try catch
        LOG("RocksdbMapStateTable value get");

        // outputSerializer free need after Get called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);

        std::string valueInTable;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &valueInTable);
        if (!s.ok() && valueInTable.length() != 0) {
            THROW_RUNTIME_ERROR("rocksdb map state table get failed, status is << " << s.ToString());
        }
        if (!s.ok() || valueInTable.length() == 0) {
            if constexpr (std::is_pointer_v<UV>) {
                return nullptr;
            } else {
                return std::numeric_limits<UV>::max();
            }
        }

        DataInputDeserializer serializedData(
            reinterpret_cast<const uint8_t*>(valueInTable.data()), static_cast<int>(valueInTable.length()), 0);
        bool isNull = serializedData.readBoolean();
        if (isNull) {
            if constexpr (std::is_pointer_v<UV>) {
                return nullptr;
            } else {
                return std::numeric_limits<UV>::max();
            }
        }
        if constexpr (std::is_same_v<UV, Object*>) {
            auto stateSerializer = getStateSerializer();
            auto buffer = stateSerializer->GetBuffer();
            stateSerializer->deserialize(buffer, serializedData);
            return buffer;
        } else {
            void* resPtr = getStateSerializer()->deserialize(serializedData);
            if constexpr (std::is_pointer_v<UV>) {
                return (UV)resPtr;
            } else {
                UV value = *(UV*)resPtr;
                delete (UV*)resPtr;
                return value;
            }
        }
    };

    std::shared_ptr<std::string> getRawBytes(const N& nameSpace, UK& uk)
    {
        // outputSerializer free need after Get called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, uk, nameSpace);

        ROCKSDB_NAMESPACE::PinnableSlice pinSlice;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &pinSlice);
        if (!s.ok() || pinSlice.size() == 0) {
            return nullptr;
        }
        return std::make_shared<std::string>(pinSlice.data(), pinSlice.size());
    };

    void putRaw(const N& nameSpace, UK& userKey, const uint8_t* data, int length)
    {
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);
        ROCKSDB_NAMESPACE::Slice sliceValue(reinterpret_cast<const char*>(data), static_cast<size_t>(length));
        auto s = rocksDb->Put(writeOptions, table, sliceKey, sliceValue);
        if (!s.ok()) {
            THROW_RUNTIME_ERROR("rocksdb map state table put raw failed, status is << " << s.ToString());
        }
    }

    void GetByBatch(
        const N& nameSpace,
        std::unordered_map<K, std::unordered_set<XXH128_hash_t>>& dataToGet,
        std::unordered_map<std::pair<K, XXH128_hash_t>, UV>& result)
    {
        std::vector<std::pair<K, XXH128_hash_t>> kvpairs;
        std::vector<std::string> key_strings;
        for (auto& item : dataToGet) {
            K currentKey = item.first;
            keyContext->setCurrentKey(currentKey);

            for (auto& userKey : item.second) {
                // outputSerializer free need after Delete called
                DataOutputSerializer outputSerializer;
                OutputBufferStatus outputBufferStatus;
                outputSerializer.setBackendBuffer(&outputBufferStatus);
                ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, (UK)userKey, nameSpace);
                key_strings.emplace_back(sliceKey.data(), sliceKey.size());
                kvpairs.push_back({currentKey, userKey});
            }
        }

        std::vector<ROCKSDB_NAMESPACE::Slice> key_slices;
        for (const auto& key_string : key_strings) {
            key_slices.emplace_back(key_string.data(), key_string.size());
        }

        std::vector<rocksdb::Status> statuses(key_slices.size());
        std::vector<rocksdb::PinnableSlice> resultsValue(key_slices.size());
        rocksdb::ReadOptions ro;
        ro.async_io = true;
        rocksDb->MultiGet(
            ro, table, (int)key_slices.size(), key_slices.data(), resultsValue.data(), statuses.data(), false);

        for (size_t i = 0; i < kvpairs.size(); ++i) {
            const auto& logicalPair = kvpairs[i];
            const rocksdb::Status& st = statuses[i];

            if (st.ok()) {
                // Found in RocksDB
                const rocksdb::PinnableSlice& val = resultsValue[i];
                UV value = decodeValueFromPinnable(val);
                result.emplace(logicalPair, value);
            }
        }
    }

    UV decodeValueFromPinnable(const rocksdb::PinnableSlice& val)
    {
        // Build a read-only view into the pinned bytes:
        DataInputDeserializer in(
            reinterpret_cast<const uint8_t*>(val.data()),
            static_cast<int>(val.size()),
            /*position=*/0);

        // Your serializer:
        auto* stateSerializer = getStateSerializer();
        bool isNull = in.readBoolean();
        if (isNull) {
            if constexpr (std::is_pointer_v<UV>) {
                return nullptr;
            } else {
                return std::numeric_limits<UV>::max();
            }
        }
        if constexpr (std::is_same_v<UV, Object*>) {
            // Special path that deserializes into an existing "buffer" the serializer owns.
            auto buffer = stateSerializer->GetBuffer();
            stateSerializer->deserialize(buffer, in);
            // Ownership of 'buffer' follows your serializer contract.
            return buffer;
        } else {
            // Generic path that returns a heap pointer to the decoded object.
            void* resPtr = stateSerializer->deserialize(in);

            if constexpr (std::is_pointer_v<UV>) {
                // Return the pointer as-is. Caller must know who frees it.
                return static_cast<UV>(resPtr);
            } else {
                // Copy out the object and free the temporary heap allocation.
                UV value = *static_cast<UV*>(resPtr);
                delete static_cast<UV*>(resPtr);
                return value;
            }
        }
    }

    ROCKSDB_NAMESPACE::Slice GetKeyNameSpaceSlice(DataOutputSerializer& outputSerializer, N& nameSpace)
    {
        auto currentKey = keyContext->getCurrentKey();
        CompositeKeySerializationUtils::writeKeyGroup(
            keyContext->getCurrentKeyGroupIndex(), keyGroupPrefixBytes_, outputSerializer);

        if constexpr (std::is_same_v<K, int64_t> || std::is_same_v<K, int32_t>) {
            LongSerializer::INSTANCE->serialize(&currentKey, outputSerializer);
        } else if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, outputSerializer);
        } else if constexpr (is_shared_ptr_v<K>) {
            getKeySerializer()->serialize(currentKey.get(), outputSerializer);
        } else {
            getKeySerializer()->serialize(&currentKey, outputSerializer);
        }

        // serializer namespace
        if constexpr (std::is_pointer_v<N>) {
            getNamespaceSerializer()->serialize(nameSpace, outputSerializer);
        } else {
            getNamespaceSerializer()->serialize(&nameSpace, outputSerializer);
        }

        return ROCKSDB_NAMESPACE::Slice(
            reinterpret_cast<const char*>(outputSerializer.getData()), outputSerializer.length());
    }

    void put(const N& nameSpace, const UK& userKey, const UV& state)
    {
        // 存入
        LOG("RocksDB put");
        // outputSerializer free need after Put called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);

        // valueOutputSerializer free need after Put called
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(valueOutputSerializer, state);
        auto s = rocksDb->Put(writeOptions, table, sliceKey, sliceValue);
        if (!s.ok()) {
            THROW_RUNTIME_ERROR("rocksdb map state table put failed, status is << " << s.ToString());
        }
    };

    // Current key is already set. Appends each user key into the caller's batch.
    void putAllCurrentKey(
        ROCKSDB_NAMESPACE::WriteBatch& putBatch, const N& nameSpace, const std::vector<std::pair<UK, UV>>& entries)
    {
        for (const auto& item : entries) {
            DataOutputSerializer outputSerializer;
            OutputBufferStatus outputBufferStatus;
            outputSerializer.setBackendBuffer(&outputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, item.first, nameSpace);

            DataOutputSerializer valueOutputSerializer;
            OutputBufferStatus valueOutputBufferStatus;
            valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(valueOutputSerializer, item.second);
            putBatch.Put(table, sliceKey, sliceValue);
        }
    }

    void writeBatch(ROCKSDB_NAMESPACE::WriteBatch& batch, bool memtableInsertHint, const char* action)
    {
        if (memtableInsertHint) {
            writeOptions.memtable_insert_hint_per_batch = true;
        }
        auto ret = rocksDb->Write(writeOptions, &batch);
        if (!ret.ok()) {
            THROW_RUNTIME_ERROR("rocksdb map state table " << action << " failed, status is << " << ret.ToString());
        }
    }

    // Current key is already set. Appends each user key delete into the caller's batch.
    void removeAllCurrentKey(
        ROCKSDB_NAMESPACE::WriteBatch& deleteBatch, const N& nameSpace, const std::vector<UK>& userKeys)
    {
        for (const auto& userKey : userKeys) {
            DataOutputSerializer outputSerializer;
            OutputBufferStatus outputBufferStatus;
            outputSerializer.setBackendBuffer(&outputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);
            deleteBatch.Delete(table, sliceKey);
        }
    }

    void putByBatch(const N& nameSpace, const K& key, const std::unordered_map<UK, UV>& dataToAdd)
    {
        keyContext->setCurrentKey(key);
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        for (auto& item : dataToAdd) {
            UK userKey = item.first;
            auto value = item.second;
            // outputSerializer free need after Put called
            DataOutputSerializer outputSerializer;
            OutputBufferStatus outputBufferStatus;
            outputSerializer.setBackendBuffer(&outputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, (UK)userKey, nameSpace);

            // valueOutputSerializer free need after Put called
            DataOutputSerializer valueOutputSerializer;
            OutputBufferStatus valueOutputBufferStatus;
            valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(valueOutputSerializer, (UV)value);
            putBatch.Put(table, sliceKey, sliceValue);
        }
        auto ret = rocksDb->Write(writeOptions, &putBatch);
    }

    void putByBatch(const N& nameSpace, std::unordered_map<K, std::unordered_map<UK, UV>>& dataToAdd)
    {
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        for (auto& item : dataToAdd) {
            K key = item.first;
            auto userKeyValues = item.second;
            keyContext->setCurrentKey(key);
            for (auto& item : userKeyValues) {
                UK userKey = item.first;
                auto value = item.second;
                // outputSerializer free need after Put called
                DataOutputSerializer outputSerializer;
                OutputBufferStatus outputBufferStatus;
                outputSerializer.setBackendBuffer(&outputBufferStatus);
                ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, (UK)userKey, nameSpace);

                // valueOutputSerializer free need after Put called
                DataOutputSerializer valueOutputSerializer;
                OutputBufferStatus valueOutputBufferStatus;
                valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
                ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(valueOutputSerializer, (UV)value);
                putBatch.Put(table, sliceKey, sliceValue);
            }
        }
        writeOptions.memtable_insert_hint_per_batch = true;
        auto ret = rocksDb->Write(writeOptions, &putBatch);
    }

    void putByBatch(
        const N& nameSpace, std::vector<std::shared_ptr<std::tuple<K, UK, std::shared_ptr<std::string>>>>& dataToAdd)
    {
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        for (auto& item : dataToAdd) {
            K key = std::get<0>(*item);
            UK ukey = std::get<1>(*item);
            std::shared_ptr<std::string> strPtr = std::get<2>(*item);
            // outputSerializer free need after Put called
            keyContext->setCurrentKey(key);
            DataOutputSerializer outputSerializer;
            OutputBufferStatus outputBufferStatus;
            outputSerializer.setBackendBuffer(&outputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, ukey, nameSpace);

            // valueOutputSerializer free need after Put called
            DataOutputSerializer valueOutputSerializer;
            OutputBufferStatus valueOutputBufferStatus;
            valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
            // Write exact byte length instead of relying on C-string termination.
            ROCKSDB_NAMESPACE::Slice sliceValue(strPtr->data(), strPtr->size());
            putBatch.Put(table, sliceKey, sliceValue);
        }
        writeOptions.memtable_insert_hint_per_batch = true;
        auto ret = rocksDb->Write(writeOptions, &putBatch);
    }

    void putByBatch(const N& nameSpace, std::vector<std::shared_ptr<std::tuple<K, UK, UV>>>& dataToAdd)
    {
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        for (auto& item : dataToAdd) {
            K key = std::get<0>(*item);
            UK ukey = std::get<1>(*item);
            UV value = std::get<2>(*item);
            keyContext->setCurrentKey(key);

            DataOutputSerializer outputSerializer;
            OutputBufferStatus outputBufferStatus;
            outputSerializer.setBackendBuffer(&outputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, ukey, nameSpace);

            DataOutputSerializer valueOutputSerializer;
            OutputBufferStatus valueOutputBufferStatus;
            valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(valueOutputSerializer, value);
            putBatch.Put(table, sliceKey, sliceValue);
        }
        writeOptions.memtable_insert_hint_per_batch = true;
        auto ret = rocksDb->Write(writeOptions, &putBatch);
    }

    void putByBatch(const N& nameSpace, std::unordered_map<K, std::vector<std::tuple<UK, UV>>>& dataToAdd)
    {
        ROCKSDB_NAMESPACE::WriteBatch putBatch;

        DataOutputSerializer keyPrefixOutputSerializer;
        OutputBufferStatus keyPrefixOutputBufferStatus;
        keyPrefixOutputSerializer.setBackendBuffer(&keyPrefixOutputBufferStatus);

        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus keyOutputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&keyOutputBufferStatus);

        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

        for (auto& item : dataToAdd) {
            const K& key = item.first;
            auto& userKeyValues = item.second;
            if (userKeyValues.empty()) {
                continue;
            }
            keyContext->setCurrentKey(key);

            keyPrefixOutputSerializer.clear();
            serializerKeyAndNamespace(keyPrefixOutputSerializer, nameSpace);
            const int keyPrefixLength = keyPrefixOutputSerializer.length();

            keyOutputSerializer.clear();
            keyOutputSerializer.write(keyPrefixOutputSerializer.getData(), keyPrefixLength, 0, keyPrefixLength);

            for (const auto& keyValue : userKeyValues) {
                const UK& userKey = std::get<0>(keyValue);
                const UV& value = std::get<1>(keyValue);

                keyOutputSerializer.setPosition(keyPrefixLength);
                if constexpr (std::is_pointer_v<UK>) {
                    getUserKeySerializer()->serialize(userKey, keyOutputSerializer);
                } else if constexpr (is_shared_ptr_v<UK>) {
                    getUserKeySerializer()->serialize(userKey.get(), keyOutputSerializer);
                } else {
                    UK mutableUserKey = userKey;
                    getUserKeySerializer()->serialize(&mutableUserKey, keyOutputSerializer);
                }
                ROCKSDB_NAMESPACE::Slice sliceKey(
                    reinterpret_cast<const char*>(keyOutputSerializer.getData()), keyOutputSerializer.length());

                valueOutputSerializer.clear();
                ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(valueOutputSerializer, value);
                putBatch.Put(table, sliceKey, sliceValue);
            }
        }
        writeOptions.memtable_insert_hint_per_batch = true;
        auto ret = rocksDb->Write(writeOptions, &putBatch);
    }

    void putByBatch(const N& nameSpace, std::vector<std::tuple<K, UK, UV>>& dataToAdd)
    {
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

        for (auto& item : dataToAdd) {
            K key = std::get<0>(item);
            UK ukey = std::get<1>(item);
            UV value = std::get<2>(item);
            keyContext->setCurrentKey(key);

            outputSerializer.clear();
            ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, ukey, nameSpace);

            valueOutputSerializer.clear();
            ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(valueOutputSerializer, value);
            putBatch.Put(table, sliceKey, sliceValue);
        }
        writeOptions.memtable_insert_hint_per_batch = true;
        auto ret = rocksDb->Write(writeOptions, &putBatch);
    }

    void remove(const N& nameSpace, const UK& userKey)
    {
        // 删除
        // outputSerializer free need after Delete called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);
        auto s = rocksDb->Delete(writeOptions, table, sliceKey);
        if (!s.ok()) {
            THROW_RUNTIME_ERROR("rocksdb map state table remove failed, status is << " << s.ToString());
        }
    };

    void removeByBatch(const N& nameSpace, std::unordered_map<K, std::unordered_set<UK>>& dataToRemove)
    {
        ROCKSDB_NAMESPACE::WriteBatch deleteBatch;
        for (auto& item : dataToRemove) {
            K currentKey = item.first;
            keyContext->setCurrentKey(currentKey);

            for (auto& userKey : item.second) {
                // outputSerializer free need after Delete called
                DataOutputSerializer outputSerializer;
                OutputBufferStatus outputBufferStatus;
                outputSerializer.setBackendBuffer(&outputBufferStatus);
                ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);
                deleteBatch.Delete(table, sliceKey);
            }
        }
        auto ret = rocksDb->Write(writeOptions, &deleteBatch);
    }

    void addByBatch(const N& nameSpace, std::unordered_map<K, std::unordered_set<RowData*>>& dataToAdd)
    {
        ROCKSDB_NAMESPACE::WriteBatch putBatch;
        for (auto& item : dataToAdd) {
            K& currentKey = item.first;
            keyContext->setCurrentKey(currentKey);

            for (auto& userKey : item.second) {
                // outputSerializer free need after Put called
                DataOutputSerializer outputSerializer;
                OutputBufferStatus outputBufferStatus;
                outputSerializer.setBackendBuffer(&outputBufferStatus);
                ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, (UK)userKey, nameSpace);

                // valueOutputSerializer free need after Put called
                DataOutputSerializer valueOutputSerializer;
                OutputBufferStatus valueOutputBufferStatus;
                valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
                ROCKSDB_NAMESPACE::Slice sliceValue = serializerValue(valueOutputSerializer, (UV)userKey);
                putBatch.Put(table, sliceKey, sliceValue);
            }
        }
        writeOptions.memtable_insert_hint_per_batch = true;
        auto ret = rocksDb->Write(writeOptions, &putBatch);
    }

    bool contains(const N& nameSpace, const UK& userKey)
    {
        // 和Rocksdb交互的时候要try catch
        LOG("RocksdbMapStateTable value contains");
        // outputSerializer free need after Get called
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = serializerKeyAndUserKey(outputSerializer, userKey, nameSpace);

        std::string valueInTable;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &valueInTable);
        if (!s.ok() || valueInTable.length() == 0) {
            return false;
        }
        return true;
    }

    std::unordered_map<K, emhash7::HashMap<UK, UV>> entriesCache;
    // WARNING:
    // This function returns cached data but not fresh data (same data for the same key).
    // If you want to get fresh data, you MUST call `clearEntriesCache()` manually before calling this method again.
    emhash7::HashMap<UK, UV>* entries(const N& nameSpace)
    {
        // auto* resultMap = new emhash7::HashMap<UK, UV>();
        auto currentKey = keyContext->getCurrentKey();
        if (auto it = entriesCache.find(currentKey); it != entriesCache.end()) {
            return it->second.size() > 0 ? &it->second : nullptr;
        }

        emhash7::HashMap<UK, UV> resultMap;

        // 序列化key
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        CompositeKeySerializationUtils::writeKeyGroup(
            keyContext->getCurrentKeyGroupIndex(), keyGroupPrefixBytes_, keyOutputSerializer);
        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, keyOutputSerializer);
        } else if constexpr (is_shared_ptr_v<K>) {
            getKeySerializer()->serialize(currentKey.get(), keyOutputSerializer);
        } else {
            getKeySerializer()->serialize(&currentKey, keyOutputSerializer);
        }
        // serializer namespace
        if constexpr (std::is_pointer_v<N>) {
            getNamespaceSerializer()->serialize(nameSpace, keyOutputSerializer);
        } else {
            getNamespaceSerializer()->serialize(&nameSpace, keyOutputSerializer);
        }

        ROCKSDB_NAMESPACE::Slice sliceKey(
            reinterpret_cast<const char*>(keyOutputSerializer.getData()), keyOutputSerializer.length());

        // [FALCON]------------------------------------------------------------------------------------------------
        ROCKSDB_NAMESPACE::Iterator* iterator = nullptr;
        auto useRangeFilter = reinterpret_cast<Boolean*>(
            Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_RANGE_FILTER));
        auto prefixExtractorLength = reinterpret_cast<Integer*>(
            Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::PREFIX_EXTRACTOR_LENGTH));

        int prefixLen = 13;
        if (prefixExtractorLength != nullptr) {
            prefixLen = prefixExtractorLength->value;
            prefixExtractorLength->putRefCount();
        }

        if (useRangeFilter != nullptr && useRangeFilter->value) {
            ROCKSDB_NAMESPACE::ReadOptions readOption;
            if (sliceKey.size() < prefixLen) {
                readOption.total_order_seek = true;
            } else {
                readOption.total_order_seek = false;
            }
            // INFO_RELEASE("[FALCON] performing prefix length check.") // avoid too much log print

            iterator = rocksDb->NewIterator(readOption, table);
        } else {
            iterator = rocksDb->NewIterator(ROCKSDB_NAMESPACE::ReadOptions(), table);
        }

        if (useRangeFilter != nullptr) {
            useRangeFilter->putRefCount();
        }
        // [FALCON]------------------------------------------------------------------------------------------------

        if (iterator == nullptr) {
            THROW_LOGIC_EXCEPTION("iterator from db is null");
        }
        iterator->Seek(sliceKey);
        // 遍历以指定前缀开头的键值对
        for (; iterator->Valid() && iterator->key().starts_with(sliceKey); iterator->Next()) {
            ROCKSDB_NAMESPACE::Slice key = iterator->key();
            key.remove_prefix(sliceKey.size());
            ROCKSDB_NAMESPACE::Slice value = iterator->value();
            UK entryKey;
            UV entryValue;

            DataInputDeserializer serializedData(reinterpret_cast<const uint8_t*>(key.data()), key.size(), 0);
            if constexpr (std::is_same_v<UK, Object*>) {
                auto buffer = userKeySerializer->GetBuffer();
                userKeySerializer->deserialize(buffer, serializedData);
                entryKey = buffer;
            } else {
                void* resPtr = getUserKeySerializer()->deserialize(serializedData);
                if constexpr (std::is_pointer_v<UK>) {
                    UK temp = (UK)resPtr;
                    BinaryRowData* rd = dynamic_cast<BinaryRowData*>(temp);
                    if (rd != nullptr) {
                        entryKey = (UK)(rd->copy());
                    } else {
                        entryKey = (UK)resPtr;
                    }
                } else {
                    entryKey = *(UK*)resPtr;
                    delete resPtr;
                }
            }

            serializedData = DataInputDeserializer(reinterpret_cast<const uint8_t*>(value.data()), value.size(), 0);
            bool isNull = serializedData.readBoolean();
            if (isNull) {
                if constexpr (std::is_pointer_v<UV>) {
                    entryValue = nullptr;
                } else {
                    entryValue = std::numeric_limits<UV>::max();
                }
            } else {
                if constexpr (std::is_same_v<UV, Object*>) {
                    auto stateSerializer = getStateSerializer();
                    auto buffer = stateSerializer->GetBuffer();
                    stateSerializer->deserialize(buffer, serializedData);
                    entryValue = buffer;
                } else {
                    void* resPtr = getStateSerializer()->deserialize(serializedData);
                    if constexpr (std::is_pointer_v<UV>) {
                        entryValue = (UV)resPtr;
                    } else {
                        entryValue = *(UV*)resPtr;
                        delete resPtr;
                    }
                }
            }
            resultMap.emplace(std::move(entryKey), std::move(entryValue));
        }

        delete iterator;

        auto [it, inserted] = entriesCache.emplace(currentKey, std::move(resultMap));
        return it->second.size() > 0 ? &it->second : nullptr;
    }

    java_util_Iterator* iterator(const N& nameSpace)
    {
        auto it = new RocksDBMapIterator(this, nameSpace);
        return it;
    }

    std::unique_ptr<typename MapState<UK, UV>::IteratorV2> iteratorV2(const N& nameSpace)
    {
        auto keyPrefixBytes = serializeCurrentKeyWithGroupAndNamespace(nameSpace);
        auto dataInputView = std::make_unique<DataInputDeserializer>();
        return std::make_unique<RocksDBMapIteratorV2>(this, keyPrefixBytes, std::move(dataInputView));
    }

    typename InternalKvState<K, N, UV>::StateIncrementalVisitor* getStateIncrementalVisitor(
        int recommendedMaxNumberOfReturnedRecords)
    {
        return nullptr;
    };

    std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> getMetaInfo()
    {
        return std::move(metaInfo);
    }

    void setMetaInfo(std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> newMetaInfo)
    {
        metaInfo = std::move(newMetaInfo);
    }

    // Not implemented
    std::vector<K>* getKeys(const N& nameSpace);

    std::vector<std::tuple<K, N>>* getKeysAndNamespace();

    TypeSerializer* getKeySerializer()
    {
        return keySerializer;
    }

    TypeSerializer* getUserKeySerializer()
    {
        return userKeySerializer;
    }

    TypeSerializer* getStateSerializer()
    {
        return metaInfo->getStateSerializer();
    }

    TypeSerializer* getNamespaceSerializer()
    {
        return metaInfo->getNamespaceSerializer();
    }

    uint32_t getNextSequenceNumber(int32_t keyGroup)
    {
        return sequenceNumberHelper_.getNextSequenceNumber(keyGroup);
    }

    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
    {
        if (vectorBatch == nullptr) {
            THROW_RUNTIME_ERROR("vectorBatch is nullptr");
        }
        auto sequenceNumber = sequenceNumberHelper_.getNextSequenceNumber(keyGroup);
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);

        CompositeKeySerializationUtils::writeKeyGroup(keyGroup, keyGroupPrefixBytes_, keyOutputSerializer);

        LongSerializer longSerializer;
        auto sequenceNumberI64 = static_cast<int64_t>(sequenceNumber);
        longSerializer.serialize(&sequenceNumberI64, keyOutputSerializer);

        ROCKSDB_NAMESPACE::Slice key(
            reinterpret_cast<const char*>(keyOutputSerializer.getData()), (int32_t)(keyOutputSerializer.getPosition()));
        int batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
        std::vector<uint8_t> buf(batchSize);
        auto* buffer = buf.data();
        omnistream::SerializedBatchInfo serializedBatchInfo =
            omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
        ROCKSDB_NAMESPACE::Slice vbValue(
            reinterpret_cast<const char*>(serializedBatchInfo.buffer), serializedBatchInfo.size);

        auto status = rocksDb->Put(writeOptions, VBTable, key, vbValue);
        if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to add VectorBatch to RocksDB: " << status.ToString());
        }
        sequenceNumberHelper_.addNextSequenceNumber(keyGroup);
    }

    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
    {
        if (vectorBatchByKeyGroup.empty()) {
            return;
        }
        ROCKSDB_NAMESPACE::WriteBatch writeBatch;

        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        LongSerializer longSerializer;

        for (auto& [keyGroup, vectorBatch] : vectorBatchByKeyGroup) {
            if (vectorBatch == nullptr) {
                THROW_RUNTIME_ERROR("vectorBatch is nullptr");
            }
            keyOutputSerializer.clear();
            CompositeKeySerializationUtils::writeKeyGroup(keyGroup, keyGroupPrefixBytes_, keyOutputSerializer);

            auto sequenceNumber = sequenceNumberHelper_.getNextSequenceNumber(keyGroup);
            auto sequenceNumberI64 = static_cast<int64_t>(sequenceNumber);
            longSerializer.serialize(&sequenceNumberI64, keyOutputSerializer);

            ROCKSDB_NAMESPACE::Slice key(
                reinterpret_cast<const char*>(keyOutputSerializer.getData()),
                (int32_t)(keyOutputSerializer.getPosition()));
            int batchSize =
                omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
            std::vector<uint8_t> buf(batchSize);
            auto* buffer = buf.data();
            omnistream::SerializedBatchInfo serializedBatchInfo =
                omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
            ROCKSDB_NAMESPACE::Slice vbValue(
                reinterpret_cast<const char*>(serializedBatchInfo.buffer), serializedBatchInfo.size);
            writeBatch.Put(VBTable, key, vbValue);
        }

        ROCKSDB_NAMESPACE::WriteOptions batchWriteOptions = writeOptions;
        batchWriteOptions.memtable_insert_hint_per_batch = true;
        auto status = rocksDb->Write(batchWriteOptions, &writeBatch);

        if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to add VectorBatches to RocksDB: " << status.ToString());
        }
        for (auto& [keyGroup, vectorBatch] : vectorBatchByKeyGroup) {
            sequenceNumberHelper_.addNextSequenceNumber(keyGroup);
        }
    }

    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
    {
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);

        CompositeKeySerializationUtils::writeKeyGroup(keyGroup, keyGroupPrefixBytes_, keyOutputSerializer);

        LongSerializer longSerializer;
        auto sequenceNumberI64 = static_cast<int64_t>(sequenceNumber);
        longSerializer.serialize(&sequenceNumberI64, keyOutputSerializer);

        ROCKSDB_NAMESPACE::Slice key(
            reinterpret_cast<const char*>(keyOutputSerializer.getData()), (int32_t)(keyOutputSerializer.getPosition()));

        std::string valueInTable;
        auto s = rocksDb->Get(readOptions, VBTable, key, &valueInTable);
        if (!s.ok()) {
            return nullptr;
        } else {
            uint8_t* address = reinterpret_cast<uint8_t*>(valueInTable.data()) + sizeof(int8_t);
            auto batch = omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(address);
            return batch;
        }
    }

    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup)
    {
        NOT_IMPL_EXCEPTION;
    }

    void clearVectorBatches(int64_t currentTimestamp)
    {
        ROCKSDB_NAMESPACE::WriteBatch batchToDelete;
        std::unique_ptr<ROCKSDB_NAMESPACE::Iterator> it(rocksDb->NewIterator(readOptions, VBTable));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            ROCKSDB_NAMESPACE::Slice valueSlice = it->value();
            uint8_t* address = reinterpret_cast<uint8_t*>(const_cast<char*>(valueSlice.data())) + sizeof(int8_t);
            auto batch = omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(address);

            if (batch) {
                if (batch->isEmpty(currentTimestamp)) {
                    batchToDelete.Delete(VBTable, it->key());
                }
                delete batch;
            }
        }

        if (!it->status().ok()) {
            INFO_RELEASE("ROCKSDB WARNING: Delete iterator error: " << it->status().ToString());
        }

        auto status = rocksDb->Write(writeOptions, &batchToDelete);
        if (!status.ok()) {
            INFO_RELEASE("ROCKSDB WARNING: Failed to delete batches: " << status.ToString());
        }
    }

    void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete)
    {
        if (sequenceNumbersToDelete.empty()) {
            return;
        }

        ROCKSDB_NAMESPACE::WriteBatch batchToDelete;
        LongSerializer longSerializer;

        for (auto sequenceNumber : sequenceNumbersToDelete) {
            // Recreate serializers inside the loop to ensure clean buffers for each key
            DataOutputSerializer keyOutputSerializer;
            OutputBufferStatus outputBufferStatus;
            keyOutputSerializer.setBackendBuffer(&outputBufferStatus);

            CompositeKeySerializationUtils::writeKeyGroup(keyGroup, keyGroupPrefixBytes_, keyOutputSerializer);
            auto sequenceNumberI64 = static_cast<int64_t>(sequenceNumber);
            longSerializer.serialize(&sequenceNumberI64, keyOutputSerializer);

            ROCKSDB_NAMESPACE::Slice key(
                reinterpret_cast<const char*>(keyOutputSerializer.getData()),
                (int32_t)(keyOutputSerializer.getPosition()));

            batchToDelete.Delete(VBTable, key);
        }

        auto status = rocksDb->Write(writeOptions, &batchToDelete);
        if (!status.ok()) {
            INFO_RELEASE("ROCKSDB WARNING: Failed to batch delete vectors: " << status.ToString());
        }
    }

    class RocksDBMapEntry : public MapEntry {
    public:
        explicit RocksDBMapEntry(
            RocksdbMapStateTable<K, N, UK, UV>* stateTable,
            int32_t userKeyOffset,
            const ROCKSDB_NAMESPACE::Slice& sliceKey,
            const ROCKSDB_NAMESPACE::Slice& sliceValue)
            : stateTable(stateTable),
              userKeyOffset(userKeyOffset)
        {
            this->key.resize(sliceKey.size());
            std::copy(sliceKey.data(), sliceKey.data() + sliceKey.size(), this->key.begin());
            this->value.resize(sliceValue.size());
            std::copy(sliceValue.data(), sliceValue.data() + sliceValue.size(), this->value.begin());
        }

        ~RocksDBMapEntry() override
        {
            if constexpr (std::is_same_v<UK, Object*>) {
                if (userKey != nullptr) {
                    static_cast<Object*>(userKey)->putRefCount();
                }
            }
            if constexpr (std::is_same_v<UV, Object*>) {
                if (userValue != nullptr) {
                    static_cast<Object*>(userValue)->putRefCount();
                }
            }
        }

        void remove()
        {
            deleted = true;
            ROCKSDB_NAMESPACE::Slice sliceKey(key.data(), key.size());
            auto s = stateTable->rocksDb->Delete(stateTable->writeOptions, stateTable->table, sliceKey);
            if (!s.ok()) {
                THROW_RUNTIME_ERROR("rocksdb map state table remove failed, status is << " << s.ToString());
            }
        }

        UK getKey() override
        {
            if (userKey == nullptr) {
                ROCKSDB_NAMESPACE::Slice sliceKey(key.data(), key.size());
                sliceKey.remove_prefix(userKeyOffset);

                DataInputDeserializer serializedData(
                    reinterpret_cast<const uint8_t*>(sliceKey.data()), sliceKey.size(), 0);
                if constexpr (std::is_same_v<UK, Object*>) {
                    auto buffer = stateTable->userKeySerializer->GetBuffer();
                    stateTable->userKeySerializer->deserialize(buffer, serializedData);
                    userKey = buffer;
                } else {
                    void* resPtr = stateTable->userKeySerializer->deserialize(serializedData);
                    if constexpr (std::is_pointer_v<UK>) {
                        userKey = (UK)resPtr;
                    } else {
                        userKey = *(UK*)resPtr;
                    }
                }
            }
            return userKey;
        }

        UV getValue() override
        {
            if (deleted) {
                return nullptr;
            } else {
                if (userValue == nullptr) {
                    DataInputDeserializer serializedData =
                        DataInputDeserializer(reinterpret_cast<const uint8_t*>(value.data()), value.size(), 0);
                    bool isNull = serializedData.readBoolean();
                    if (isNull) {
                        if constexpr (std::is_pointer_v<UV>) {
                            return nullptr;
                        } else {
                            return std::numeric_limits<UV>::max();
                        }
                    }
                    if constexpr (std::is_same_v<UV, Object*>) {
                        auto stateSerializer = stateTable->getStateSerializer();
                        auto buffer = stateSerializer->GetBuffer();
                        stateSerializer->deserialize(buffer, serializedData);
                        userValue = buffer;
                    } else {
                        void* resPtr = stateTable->getStateSerializer()->deserialize(serializedData);
                        if constexpr (std::is_pointer_v<UV>) {
                            userValue = (UV)resPtr;
                        } else {
                            userValue = *(UV*)resPtr;
                        }
                    }
                }
                return userValue;
            }
        }

        void setValue(Object* val) override
        {
            if (deleted) {
                THROW_LOGIC_EXCEPTION("setValue IllegalStateException: The value has already been deleted.");
            }

            val->getRefCount();
            if constexpr (std::is_same_v<UV, Object*>) {
                if (userValue != nullptr) {
                    static_cast<Object*>(userValue)->putRefCount();
                }
            }
            userValue = val;
            DataOutputSerializer valueOutputSerializer;
            OutputBufferStatus valueOutputBufferStatus;
            valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
            ROCKSDB_NAMESPACE::Slice sliceValue = stateTable->serializerValue(valueOutputSerializer, userValue);
            ROCKSDB_NAMESPACE::Slice sliceKey(key.data(), key.size());
            stateTable->rocksDb->Put(stateTable->writeOptions, stateTable->table, sliceKey, sliceValue);
        }

    public:
        RocksdbMapStateTable<K, N, UK, UV>* stateTable;
        int32_t userKeyOffset;
        std::vector<char> key;
        std::vector<char> value;
        bool deleted = false;
        UK userKey = nullptr;
        UV userValue = nullptr;
    };

    class RocksDBMapIterator : public java_util_Iterator {
    public:
        explicit RocksDBMapIterator(RocksdbMapStateTable<K, N, UK, UV>* stateTable, const N& nameSpace)
            : nameSpace_(nameSpace),
              stateTable_(stateTable)
        {
            auto currentKey = stateTable_->keyContext->getCurrentKey();

            // 序列化key
            keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
            CompositeKeySerializationUtils::writeKeyGroup(
                stateTable->keyContext->getCurrentKeyGroupIndex(),
                stateTable->keyGroupPrefixBytes_,
                keyOutputSerializer);
            if constexpr (std::is_pointer_v<K>) {
                stateTable->getKeySerializer()->serialize(currentKey, keyOutputSerializer);
            } else if constexpr (is_shared_ptr_v<K>) {
                stateTable->getKeySerializer()->serialize(currentKey.get(), keyOutputSerializer);
            } else {
                stateTable->getKeySerializer()->serialize(&currentKey, keyOutputSerializer);
            }

            // serializer namespace
            if constexpr (std::is_pointer_v<N>) {
                stateTable->getNamespaceSerializer()->serialize(nameSpace_, keyOutputSerializer);
            } else {
                stateTable->getNamespaceSerializer()->serialize(&nameSpace_, keyOutputSerializer);
            }

            keyPrefixBytes = ROCKSDB_NAMESPACE::Slice(
                reinterpret_cast<const char*>(keyOutputSerializer.getData()), keyOutputSerializer.length());

            // [FALCON]------------------------------------------------------------------------------------------------
            internalTableIterator = nullptr;
            auto useRangeFilter = reinterpret_cast<Boolean*>(
                Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_RANGE_FILTER));
            auto prefixExtractorLength = reinterpret_cast<Integer*>(
                Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::PREFIX_EXTRACTOR_LENGTH));

            int prefixLen = 13;
            if (prefixExtractorLength != nullptr) {
                prefixLen = prefixExtractorLength->value;
                prefixExtractorLength->putRefCount();
            }

            if (useRangeFilter != nullptr && useRangeFilter->value) {
                ROCKSDB_NAMESPACE::ReadOptions readOption = stateTable_->readOptions;
                if (keyPrefixBytes.size() < prefixLen || currentEntry != nullptr) {
                    readOption.total_order_seek = true;
                } else {
                    readOption.total_order_seek = false;
                }
                internalTableIterator = stateTable_->rocksDb->NewIterator(readOption, stateTable_->table);
            } else {
                internalTableIterator = stateTable_->rocksDb->NewIterator(stateTable_->readOptions, stateTable_->table);
            }

            if (useRangeFilter != nullptr) {
                useRangeFilter->putRefCount();
            }
            // [FALCON]------------------------------------------------------------------------------------------------
        }

        ~RocksDBMapIterator() override
        {
            for (size_t i = 0; i < cacheEntries.size(); ++i) {
                delete cacheEntries[i];
            }
            cacheEntries.clear();
            delete internalTableIterator;
        }

        bool hasNext() override
        {
            loadCache();
            return (cacheIndex < cacheEntries.size());
        }

        Object* next() override
        {
            RocksDBMapEntry* entry = nextEntry();
            if (entry != nullptr) {
                entry->getRefCount();
            }
            return entry;
        }

        void remove() override
        {
            if (currentEntry == nullptr || currentEntry->deleted) {
                THROW_LOGIC_EXCEPTION("The remove operation must be called after a valid next operation.");
            }

            currentEntry->remove();
        }

        RocksDBMapEntry* nextEntry()
        {
            loadCache();
            if (cacheIndex == cacheEntries.size()) {
                if (!expired) {
                    THROW_LOGIC_EXCEPTION("nextEntry IllegalStateException");
                }

                return nullptr;
            }

            currentEntry = cacheEntries[cacheIndex];
            cacheIndex++;
            return currentEntry;
        }

        void loadCache()
        {
            if (cacheIndex > cacheEntries.size()) {
                THROW_LOGIC_EXCEPTION("loadCache IllegalStateException");
            }

            // Load cache entries only when the cache is empty and there still exist unread entries
            if (cacheIndex < cacheEntries.size() || expired) {
                return;
            }

            ROCKSDB_NAMESPACE::Slice sliceKey =
                currentEntry != nullptr ? ROCKSDB_NAMESPACE::Slice(currentEntry->key.data(), currentEntry->key.size())
                                        : keyPrefixBytes;

            internalTableIterator->Seek(sliceKey);

            // free memory
            for (size_t i = 0; i < cacheEntries.size(); ++i) {
                delete cacheEntries[i];
                cacheEntries[i] = nullptr;
            }
            cacheEntries.clear();
            cacheIndex = 0;
            /*
             * If the entry pointing to the current position is not removed, it will be the first entry in the
             * new iterating. Skip it to avoid redundant access in such cases.
             */
            if (currentEntry != nullptr && !currentEntry->deleted) {
                internalTableIterator->Next();
            }

            while (true) {
                if (!internalTableIterator->Valid() || !internalTableIterator->key().starts_with(keyPrefixBytes)) {
                    expired = true;
                    break;
                }

                if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
                    break;
                }

                ROCKSDB_NAMESPACE::Slice key = internalTableIterator->key();
                ROCKSDB_NAMESPACE::Slice value = internalTableIterator->value();
                auto* entry = new RocksDBMapEntry(stateTable_, keyPrefixBytes.size(), key, value);
                cacheEntries.push_back(entry);
                internalTableIterator->Next();
            }
        }

    protected:
        size_t CACHE_SIZE_LIMIT = 128;
        RocksdbMapStateTable<K, N, UK, UV>* stateTable_;
        bool expired = false;
        std::vector<RocksDBMapEntry*> cacheEntries;
        size_t cacheIndex = 0;
        RocksDBMapEntry* currentEntry = nullptr;
        const N& nameSpace_;
        ROCKSDB_NAMESPACE::Slice keyPrefixBytes;
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        ROCKSDB_NAMESPACE::Iterator* internalTableIterator = nullptr;
    };

    class RocksDBMapIteratorV2;
    class RocksDBMapEntryV2 : public omnistream::utils::Map<UK, UV>::Entry {
        friend class RocksDBMapIteratorV2;

    public:
        explicit RocksDBMapEntryV2(
            RocksdbMapStateTable<K, N, UK, UV>* stateTable,
            const int32_t userKeyOffset,
            std::unique_ptr<std::vector<uint8_t>> rawKeyBytes,
            std::unique_ptr<std::vector<uint8_t>> rawValueBytes,
            DataInputDeserializer* const dataInputView)
            : stateTable_(stateTable),
              userKeyOffset_(userKeyOffset),
              rawKeyBytes_(std::move(rawKeyBytes)),
              rawValueBytes_(std::move(rawValueBytes)),
              dataInputView_(dataInputView)
        {
        }

        ~RocksDBMapEntryV2() override
        {
            if constexpr (std::is_pointer_v<UK>) {
                if (userKey_.has_value()) {
                    delete userKey_.value();
                }
            }
            if constexpr (std::is_pointer_v<UV>) {
                if (userValue_.has_value()) {
                    delete userValue_.value();
                }
            }
        }

        void remove()
        {
            deleted_ = true;
            ROCKSDB_NAMESPACE::Slice sliceKey(
                reinterpret_cast<const char*>(rawKeyBytes_->data()), rawKeyBytes_->size());
            auto s = stateTable_->rocksDb->Delete(stateTable_->writeOptions, stateTable_->table, sliceKey);
            if (!s.ok()) {
                THROW_RUNTIME_ERROR("Error while removing data from RocksDB, status: " << s.ToString());
            }
        }

        std::optional<UK> getKey() override
        {
            if (!userKey_.has_value()) {
                try {
                    userKey_ = deserializeUserKey(
                        dataInputView_, userKeyOffset_, *rawKeyBytes_, stateTable_->getUserKeySerializer());
                } catch (const std::exception& e) {
                    THROW_RUNTIME_ERROR("Error while deserializing the user key, error: " << e.what());
                }
            }
            return userKey_;
        }

        std::optional<UV> getValue() override
        {
            if (deleted_) {
                return std::nullopt;
            }

            if (!userValue_.has_value()) {
                try {
                    userValue_ =
                        deserializeUserValue(dataInputView_, *rawValueBytes_, stateTable_->getStateSerializer());
                } catch (const std::exception& e) {
                    THROW_RUNTIME_ERROR("Error while deserializing the user value, error: " << e.what());
                }
            }
            return userValue_;
        }

        void setValue(std::optional<UV> value) override
        {
            if (deleted_) {
                THROW_RUNTIME_ERROR("The value has already been deleted.");
            }
            try {
                userValue_ = value;
                stateTable_->outputView_.clear();
                if (userValue_.has_value()) {
                    stateTable_->serializerValue(stateTable_->outputView_, userValue_.value());
                }
                rawValueBytes_ = std::unique_ptr<std::vector<uint8_t>>(stateTable_->outputView_.getCopyOfBuffer());

                ROCKSDB_NAMESPACE::Slice sliceKey(
                    reinterpret_cast<const char*>(rawKeyBytes_->data()), rawKeyBytes_->size());
                ROCKSDB_NAMESPACE::Slice sliceValue(
                    reinterpret_cast<const char*>(rawValueBytes_->data()), rawValueBytes_->size());
                auto s = stateTable_->rocksDb->Put(stateTable_->writeOptions, stateTable_->table, sliceKey, sliceValue);
                if (!s.ok()) {
                    THROW_RUNTIME_ERROR("Error while putting data into RocksDB, status: " << s.ToString());
                }
            } catch (const std::exception& e) {
                THROW_RUNTIME_ERROR("Error while setting value into RocksDB, error: " << e.what());
            }
        }

    private:
        RocksdbMapStateTable<K, N, UK, UV>* const stateTable_{};
        const int32_t userKeyOffset_;
        std::unique_ptr<std::vector<uint8_t>> rawKeyBytes_{};
        std::unique_ptr<std::vector<uint8_t>> rawValueBytes_{};
        bool deleted_ = false;
        std::optional<UK> userKey_{};
        std::optional<UV> userValue_{};
        DataInputDeserializer* const dataInputView_;
    };

    class RocksDBMapIteratorV2 : public MapState<UK, UV>::IteratorV2 {
    public:
        explicit RocksDBMapIteratorV2(
            RocksdbMapStateTable<K, N, UK, UV>* stateTable,
            const std::vector<uint8_t>* const keyPrefixBytes,
            std::unique_ptr<DataInputDeserializer> dataInputView)
            : stateTable_(stateTable),
              keyPrefixBytes_(keyPrefixBytes),
              keyPrefixSlice_(reinterpret_cast<const char*>(keyPrefixBytes_->data()), keyPrefixBytes_->size()),
              dataInputView_(std::move(dataInputView))
        {
        }

        ~RocksDBMapIteratorV2() override
        {
            for (size_t i = 0; i < cacheEntries_.size(); ++i) {
                delete cacheEntries_[i];
            }
            delete keyPrefixBytes_;
        }

        bool hasNext() override
        {
            loadCache();
            return cacheIndex_ < cacheEntries_.size();
        }

        RocksDBMapEntryV2& next() override
        {
            RocksDBMapEntryV2* entry = nextEntry();
            return *entry;
        }

        void remove() override
        {
            if (currentEntry_ == nullptr || currentEntry_->deleted_) {
                THROW_RUNTIME_ERROR("The remove operation must be called after a valid next operation.");
            }
            currentEntry_->remove();
        }

        RocksDBMapEntryV2* nextEntry()
        {
            loadCache();
            if (cacheIndex_ == cacheEntries_.size()) {
                if (!expired_) {
                    THROW_RUNTIME_ERROR("Error while loading cache, cache is not expired but has no more cache entry.");
                }
                return nullptr;
            }

            currentEntry_ = cacheEntries_[cacheIndex_];
            cacheIndex_++;
            return currentEntry_;
        }

        void loadCache()
        {
            if (cacheIndex_ > cacheEntries_.size()) {
                THROW_RUNTIME_ERROR("Error while loading cache, cacheIndex_ > cacheEntries_.size()");
            }

            // Load cache entries only when the cache is empty and there still exist unread entries
            if (cacheIndex_ < cacheEntries_.size() || expired_) {
                return;
            }

            auto iterator = RocksDbOperationUtils::getRocksIterator(
                stateTable_->rocksDb, stateTable_->table, stateTable_->readOptions);

            /*
             * The iteration starts from the prefix bytes at the first loading. After #nextEntry() is called,
             * the currentEntry points to the last returned entry, and at that time, we will start
             * the iterating from currentEntry if reloading cache is needed.
             */
            auto* startBytes = currentEntry_ == nullptr ? keyPrefixBytes_ : currentEntry_->rawKeyBytes_.get();

            ROCKSDB_NAMESPACE::Slice sliceKey =
                ROCKSDB_NAMESPACE::Slice(reinterpret_cast<const char*>(startBytes->data()), startBytes->size());
            iterator->seek(sliceKey);

            /*
             * If the entry pointing to the current position is not removed, it will be the first entry in the
             * new iterating. Skip it to avoid redundant access in such cases.
             * Record this flag before currentEntry_ is deleted.
             */
            const bool skipCurrent = currentEntry_ != nullptr && !currentEntry_->deleted_;

            for (size_t i = 0; i < cacheEntries_.size(); ++i) {
                delete cacheEntries_[i];
            }
            cacheEntries_.clear();
            cacheIndex_ = 0;
            currentEntry_ = nullptr;

            if (skipCurrent) {
                iterator->next();
            }

            while (true) {
                if (!iterator->isValid() || !iterator->keySlice().starts_with(keyPrefixSlice_)) {
                    expired_ = true;
                    break;
                }

                if (cacheEntries_.size() >= CACHE_SIZE_LIMIT) {
                    break;
                }

                ROCKSDB_NAMESPACE::Slice key = iterator->keySlice();
                ROCKSDB_NAMESPACE::Slice value = iterator->valueSlice();
                auto keyBytes = std::make_unique<std::vector<uint8_t>>(key.data(), key.data() + key.size());
                auto valueBytes = std::make_unique<std::vector<uint8_t>>(value.data(), value.data() + value.size());
                auto* entry = new RocksDBMapEntryV2(
                    stateTable_,
                    keyPrefixBytes_->size(),
                    std::move(keyBytes),
                    std::move(valueBytes),
                    dataInputView_.get());
                cacheEntries_.push_back(entry);
                iterator->next();
            }
        }

    protected:
        size_t CACHE_SIZE_LIMIT = 128;
        RocksdbMapStateTable<K, N, UK, UV>* const stateTable_{};
        const std::vector<uint8_t>* const keyPrefixBytes_{};
        const ROCKSDB_NAMESPACE::Slice keyPrefixSlice_{};
        bool expired_ = false;
        std::vector<RocksDBMapEntryV2*> cacheEntries_{};
        size_t cacheIndex_ = 0;
        RocksDBMapEntryV2* currentEntry_{};
        std::unique_ptr<DataInputDeserializer> dataInputView_;
    };

    class StateEntryIterator : public InternalKvState<K, N, UV>::StateIncrementalVisitor {
    public:
        UV nextEntries() override;

        StateEntryIterator(int recommendedMaxNumberOfReturnedRecords, RocksdbMapStateTable<K, N, UK, UV>* table);

        bool hasNext() override;

        void remove(const K& key, const N& nameSpace, const UV& state) override;

    private:
        int recommendedMaxNumberOfReturnedRecords;
        int keyGroupIndex;
        typename InternalKvState<K, N, UV>::StateIncrementalVisitor* stateIncrementalVisitor;
        RocksdbMapStateTable<K, N, UK, UV>* table;

        void init();
    };

    ROCKSDB_NAMESPACE::ColumnFamilyHandle* getColumnFamily()
    {
        return table;
    }

    ROCKSDB_NAMESPACE::DB* getRocksDB()
    {
        return rocksDb;
    }

    ROCKSDB_NAMESPACE::ReadOptions& getReadOptions()
    {
        return readOptions;
    }

    ROCKSDB_NAMESPACE::WriteOptions* getWriteOptions()
    {
        return &writeOptions;
    };

private:
    // Variables
    InternalKeyContext<K>* keyContext;

    template <typename T>
    int computeKeyGroup(T id) const
    {
        return keyContext->getKeyGroupRange()->getStartKeyGroup();
    }

    void restoreNextSequenceNumberByKeyGroup()
    {
        auto* keyGroupRange = keyContext->getKeyGroupRange();
        if (keyGroupRange == nullptr) {
            THROW_RUNTIME_ERROR("KeyGroupRange is nullptr while restoring sequenceNumber.");
        }

        std::unique_ptr<RocksIteratorWrapper> iterator =
            RocksDbOperationUtils::getRocksIterator(rocksDb, VBTable, readOptions);
        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus outputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
        for (int32_t keyGroup = keyGroupRange->getStartKeyGroup(); keyGroup <= keyGroupRange->getEndKeyGroup();
             ++keyGroup) {
            keyOutputSerializer.clear();
            CompositeKeySerializationUtils::writeKeyGroup(keyGroup + 1, keyGroupPrefixBytes_, keyOutputSerializer);
            ROCKSDB_NAMESPACE::Slice upperBound(
                reinterpret_cast<const char*>(keyOutputSerializer.getData()),
                (int32_t)(keyOutputSerializer.getPosition()));
            iterator->seekForPrev(upperBound);
            if (!iterator->isValid()) {
                continue;
            }

            ROCKSDB_NAMESPACE::Slice key = iterator->keySlice();
            if (key.size() != static_cast<size_t>(keyGroupPrefixBytes_) + sizeof(int64_t)) {
                THROW_RUNTIME_ERROR("Invalid VectorBatch key size while restoring sequenceNumber.");
            }

            const auto* keyData = reinterpret_cast<const uint8_t*>(key.data());
            bool matchesKeyGroup = true;
            for (int32_t i = 0; i < keyGroupPrefixBytes_; ++i) {
                auto expectedByte =
                    CompositeKeySerializationUtils::extractByteAtPosition(keyGroup, keyGroupPrefixBytes_ - i - 1);
                if (keyData[i] != expectedByte) {
                    matchesKeyGroup = false;
                    break;
                }
            }
            if (!matchesKeyGroup) {
                continue;
            }

            DataInputDeserializer in(keyData, static_cast<int>(key.size()), keyGroupPrefixBytes_);
            int64_t curSequenceNumber = in.readLong();
            if (curSequenceNumber < 0 ||
                curSequenceNumber > static_cast<int64_t>(omnistream::SequenceNumberHelper::MAX_SEQUENCE_NUMBER)) {
                THROW_RUNTIME_ERROR("Invalid VectorBatch sequenceNumber while restoring: " << curSequenceNumber);
            }
            sequenceNumberHelper_.updateNextSequenceNumber(keyGroup, static_cast<uint32_t>(curSequenceNumber + 1));
        }
    }

    TypeSerializer* keySerializer;
    TypeSerializer* userKeySerializer;
    // KeyGroupRange *keyGroupRange;
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* table; // 是不是编程columnsFamily
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* VBTable;
    std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo;
    int size = 0;
    omnistream::SequenceNumberHelper sequenceNumberHelper_{}; // only used for VectorBatch storage
    ROCKSDB_NAMESPACE::DB* rocksDb;
    int keyGroupPrefixBytes_ = 1;
    ROCKSDB_NAMESPACE::ReadOptions readOptions;
    ROCKSDB_NAMESPACE::WriteOptions writeOptions;
    DataOutputSerializer outputView_ = DataOutputSerializer(128);

    ROCKSDB_NAMESPACE::Slice serializerKeyAndUserKey(
        DataOutputSerializer& outputSerializer, UK userKey, const N& nameSpace)
    {
        auto currentKey = keyContext->getCurrentKey();

        // 序列化key, userKey
        CompositeKeySerializationUtils::writeKeyGroup(
            keyContext->getCurrentKeyGroupIndex(), keyGroupPrefixBytes_, outputSerializer);
        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, outputSerializer);
        } else if constexpr (is_shared_ptr_v<K>) {
            getKeySerializer()->serialize(currentKey.get(), outputSerializer);
        } else {
            getKeySerializer()->serialize(&currentKey, outputSerializer);
        }
        // serializer namespace
        if constexpr (std::is_pointer_v<N>) {
            getNamespaceSerializer()->serialize(nameSpace, outputSerializer);
        } else {
            getNamespaceSerializer()->serialize(&nameSpace, outputSerializer);
        }

        if constexpr (std::is_pointer_v<UK>) {
            getUserKeySerializer()->serialize(userKey, outputSerializer);
        } else if constexpr (is_shared_ptr_v<UK>) {
            getUserKeySerializer()->serialize(userKey.get(), outputSerializer);
        } else {
            getUserKeySerializer()->serialize(&userKey, outputSerializer);
        }

        return ROCKSDB_NAMESPACE::Slice(
            reinterpret_cast<const char*>(outputSerializer.getData()), outputSerializer.length());
    }

    ROCKSDB_NAMESPACE::Slice serializerKey(DataOutputSerializer& outputSerializer)
    {
        auto currentKey = keyContext->getCurrentKey();

        // 序列化key, userKey
        CompositeKeySerializationUtils::writeKeyGroup(
            keyContext->getCurrentKeyGroupIndex(), keyGroupPrefixBytes_, outputSerializer);
        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, outputSerializer);
        } else if constexpr (is_shared_ptr_v<K>) {
            getKeySerializer()->serialize(currentKey.get(), outputSerializer);
        } else {
            getKeySerializer()->serialize(&currentKey, outputSerializer);
        }

        return ROCKSDB_NAMESPACE::Slice(
            reinterpret_cast<const char*>(outputSerializer.getData()), outputSerializer.length());
    }

    ROCKSDB_NAMESPACE::Slice serializerKeyAndNamespace(DataOutputSerializer& outputSerializer, const N& nameSpace)
    {
        auto currentKey = keyContext->getCurrentKey();

        // 序列化key, userKey
        CompositeKeySerializationUtils::writeKeyGroup(
            keyContext->getCurrentKeyGroupIndex(), keyGroupPrefixBytes_, outputSerializer);
        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, outputSerializer);
        } else if constexpr (is_shared_ptr_v<K>) {
            getKeySerializer()->serialize(currentKey.get(), outputSerializer);
        } else {
            getKeySerializer()->serialize(&currentKey, outputSerializer);
        }
        // serializer namespace
        if constexpr (std::is_pointer_v<N>) {
            getNamespaceSerializer()->serialize(nameSpace, outputSerializer);
        } else {
            getNamespaceSerializer()->serialize(&nameSpace, outputSerializer);
        }

        return ROCKSDB_NAMESPACE::Slice(
            reinterpret_cast<const char*>(outputSerializer.getData()), outputSerializer.length());
    }

    ROCKSDB_NAMESPACE::Slice serializerValue(DataOutputSerializer& valueOutputSerializer, const UV& userValue)
    {
        // value序列化
        TypeSerializer* vSerializer = getStateSerializer();

        if constexpr (std::is_pointer_v<UV>) {
            valueOutputSerializer.writeBoolean(userValue == nullptr);
            vSerializer->serialize(userValue, valueOutputSerializer);
        } else if constexpr (is_shared_ptr_v<UV>) {
            valueOutputSerializer.writeBoolean(userValue == nullptr);
            vSerializer->serialize(userValue.get(), valueOutputSerializer);
        } else {
            valueOutputSerializer.writeBoolean(&userValue == nullptr);
            vSerializer->serialize(&userValue, valueOutputSerializer);
        }

        return ROCKSDB_NAMESPACE::Slice(
            reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());
    }

    static std::optional<UK> deserializeUserKey(
        DataInputDeserializer* dataInputView,
        int userKeyOffset,
        const std::vector<uint8_t>& rawKeyBytes,
        TypeSerializer* keySerializer)
    {
        dataInputView->setBuffer(
            rawKeyBytes.data(),
            static_cast<int>(rawKeyBytes.size()),
            userKeyOffset,
            static_cast<int>(rawKeyBytes.size() - userKeyOffset));
        if constexpr (std::is_pointer_v<UK>) {
            auto userKey = static_cast<UK>(keySerializer->deserialize(*dataInputView));
            return userKey;
        } else if constexpr (is_shared_ptr_v<UK>) {
            using BaseType = unwrap_shared_ptr_t<UK>;
            auto* deserialized = keySerializer->deserialize(*dataInputView);
            if constexpr (std::is_same_v<BaseType, RowData>) {
                // BinaryRowDataSerializer returns a reusable RowData*, so we need to copy
                auto* copy = static_cast<RowData*>(deserialized)->copy();
                return UK(copy);
            } else {
                NOT_IMPL_EXCEPTION;
            }
        } else {
            UK* userKeyPtr = static_cast<UK*>(keySerializer->deserialize(*dataInputView));
            UK userKey = std::move(*userKeyPtr);
            if (!keySerializer->isReusable()) {
                delete userKeyPtr;
            }
            return userKey;
        }
    }

    static std::optional<UV> deserializeUserValue(
        DataInputDeserializer* dataInputView,
        const std::vector<uint8_t>& rawValueBytes,
        TypeSerializer* valueSerializer)
    {
        dataInputView->setBuffer(
            rawValueBytes.data(), static_cast<int>(rawValueBytes.size()), 0, static_cast<int>(rawValueBytes.size()));
        if (dataInputView->readBoolean()) {
            return std::nullopt;
        }

        if constexpr (std::is_pointer_v<UV>) {
            auto userValue = static_cast<UV>(valueSerializer->deserialize(*dataInputView));
            return userValue;
        } else {
            UV* userValuePtr = static_cast<UV*>(valueSerializer->deserialize(*dataInputView));
            UV userValue = *userValuePtr;
            if (!valueSerializer->isReusable()) {
                delete userValuePtr;
            }
            return userValue;
        }
    }

    std::vector<uint8_t>* serializeCurrentKeyWithGroupAndNamespace(const N& nameSpace)
    {
        // todo: This function differs from original Flink and may need to be modified
        auto currentKey = keyContext->getCurrentKey();
        outputView_.clear();
        CompositeKeySerializationUtils::writeKeyGroup(
            keyContext->getCurrentKeyGroupIndex(), keyGroupPrefixBytes_, outputView_);
        if constexpr (std::is_pointer_v<K>) {
            getKeySerializer()->serialize(currentKey, outputView_);
        } else if constexpr (is_shared_ptr_v<K>) {
            getKeySerializer()->serialize(currentKey.get(), outputView_);
        } else {
            getKeySerializer()->serialize(&currentKey, outputView_);
        }

        if constexpr (std::is_pointer_v<N>) {
            getNamespaceSerializer()->serialize(nameSpace, outputView_);
        } else {
            getNamespaceSerializer()->serialize(&nameSpace, outputView_);
        }

        return outputView_.getCopyOfBuffer();
    }
};

template <typename K, typename N, typename UK, typename UV>
RocksdbMapStateTable<K, N, UK, UV>::RocksdbMapStateTable(
    InternalKeyContext<K>* keyContext,
    std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo,
    TypeSerializer* keySerializer,
    TypeSerializer* userKeySerializer)
    : keyContext(keyContext),
      metaInfo(std::move(metaInfo)),
      keySerializer(keySerializer),
      userKeySerializer(userKeySerializer)
{
    keyGroupPrefixBytes_ =
        CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(keyContext->getNumberOfKeyGroups());
    writeOptions.disableWAL = true;
    sequenceNumberHelper_ = omnistream::SequenceNumberHelper(keyContext->getNumberOfKeyGroups());
}

template <typename K, typename N, typename UK, typename UV>
RocksdbMapStateTable<K, N, UK, UV>::~RocksdbMapStateTable()
{
    delete table;
    delete VBTable;
}

template <typename K, typename N, typename UK, typename UV>
std::vector<K>* RocksdbMapStateTable<K, N, UK, UV>::getKeys(const N& nameSpace)
{
    return nullptr;
}

template <typename K, typename N, typename UK, typename UV>
std::vector<std::tuple<K, N>>* RocksdbMapStateTable<K, N, UK, UV>::getKeysAndNamespace()
{
    return nullptr;
}
