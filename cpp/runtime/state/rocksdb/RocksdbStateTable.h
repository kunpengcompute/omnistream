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

#ifndef OMNISTREAM_ROCKSDBSTATETABLE_H
#define OMNISTREAM_ROCKSDBSTATETABLE_H

#include <vector>
#include <type_traits>
#include <tuple>
#include <functional> // for std::hash
#include "core/typeutils/TypeSerializer.h"
#include "../StateTransformationFunction.h"
#include "../internal/InternalKvState.h"
#include "../InternalKeyContext.h"
#include "../KeyGroupRange.h"
#include "../RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/binary/BinaryRowData.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/DefaultConfigurableOptionsFactory.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "state/rocksdb/RocksDbStringAppendOperator.h"

#include "table/utils/VectorBatchSerializationUtils.h"
#include "table/utils/VectorBatchDeserializationUtils.h"

#include "../../../core/utils/MathUtils.h"
#include "state/RocksDbKvStateInfo.h"
#include "RocksDbOperationUtils.h"
#include "../RocksDBConfigurableOptions.h"
#include "data/util/SequenceNumberHelper.h"
#include "runtime/state/RocksIteratorWrapper.h"
#include "runtime/state/CompositeKeySerializationUtils.h"

/* S is the value used in the State,
 * like RowData* for HeapValueState,
 * emhash7<RowData*, int>* for HeapMapState,
 * vector<int64_t>* for List State
 */
template <typename K, typename N, typename S>
class RocksdbStateTable {
public:
    RocksdbStateTable(
        InternalKeyContext<K>* keyContext,
        std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo,
        TypeSerializer* keySerializer); // 加入db initData

    virtual ~RocksdbStateTable();

    bool isEmpty()
    {
        // NOT_IMPL_EXCEPTION
        return size == 0;
    };

    void createTable(
        ROCKSDB_NAMESPACE::DB* db,
        std::string cfName,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        const std::function<ROCKSDB_NAMESPACE::ColumnFamilyOptions(const std::string&)>& columnFamilyOptionsFactory)
    {
        this->rocksDb = db;
        ROCKSDB_NAMESPACE::ColumnFamilyOptions familyOptions = columnFamilyOptionsFactory(cfName);
        // set merge method, listState need
        familyOptions.merge_operator.reset(new RocksDbStringAppendOperator(','));

        // [FALCON] -----------------------------------------------------------------------------------------------
        auto useHashMemTable = reinterpret_cast<Boolean*>(
            Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_HASH_MEMTABLE));
        auto prefixExtractorLength = reinterpret_cast<Integer*>(
            Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::PREFIX_EXTRACTOR_LENGTH));

        int prefixLen = 13;
        if (prefixExtractorLength != nullptr) {
            prefixLen = prefixExtractorLength->value;
            prefixExtractorLength->putRefCount();
        }

        if (useHashMemTable != nullptr && useHashMemTable->value) {
            if (metaInfo->getStateType() == StateDescriptor::Type::VALUE) {
                // modify columnFamily option and read option for current columnFamily
                familyOptions.memtable_factory.reset(ROCKSDB_NAMESPACE::NewHashLinkListRepFactory());
                familyOptions.prefix_extractor.reset(ROCKSDB_NAMESPACE::NewCappedPrefixTransform(prefixLen));
                readOptions.total_order_seek = true;
                INFO_RELEASE("[FALCON] enable hash memTable for valueState, prefix length is " << prefixLen << ".");
            }
        }
        if (useHashMemTable != nullptr) {
            useHashMemTable->putRefCount();
        }

        int64_t blockCacheBytes = 0;
        auto blockCacheSize =
            reinterpret_cast<String*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::BLOCK_CACHE_SIZE));
        if (blockCacheSize != nullptr) {
            blockCacheBytes = static_cast<int64_t>(MemorySize::parseBytes(blockCacheSize->getData()));
            blockCacheSize->putRefCount();
        }
        stateMemoryBytes_ = static_cast<int64_t>(familyOptions.max_write_buffer_number) *
                                static_cast<int64_t>(familyOptions.write_buffer_size) +
                            blockCacheBytes;

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
        INFO_RELEASE("rocksdbStateTable createTable" << " cfName=" << cfName);
    }

    // per-state RocksDB memory estimate (bytes), captured in createTable. Read on the
    // metric-reporter thread (plain int64 load of a value written once on the task thread).
    int64_t getStateMemoryBytes() const
    {
        return stateMemoryBytes_;
    }

    int getHashCode(K key)
    {
        if constexpr (std::is_same_v<K, RowData*>) {
            // std::hash<RowData*> uses a simpler hasher. But here we need to keep it same as java
            int hashCode = key->hashCode();
            return MathUtils::murmurHash(hashCode);
        } else {
            int hashCode = std::hash<K>{}(key);
            return MathUtils::murmurHash(hashCode);
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

    S get(N& nameSpace)
    {
        // 和Rocksdb交互的时候要try catch
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

        std::string valueInTable;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &valueInTable);
        if (!s.ok() && valueInTable.length() != 0) {
            THROW_RUNTIME_ERROR("rocksdb state table get failed, status is << " << s.ToString());
        }
        if (!s.ok() || valueInTable.length() == 0) {
            if constexpr (std::is_pointer_v<S>) {
                return nullptr;
            } else {
                return std::numeric_limits<S>::max();
            }
        }

        DataInputDeserializer serializedData(
            reinterpret_cast<const uint8_t*>(valueInTable.data()), valueInTable.length(), 0);
        if constexpr (std::is_same_v<S, Object*>) {
            auto stateSerializer = getStateSerializer();
            auto buffer = stateSerializer->GetBuffer();
            stateSerializer->deserialize(buffer, serializedData);
            return buffer;
        } else {
            void* resPtr = getStateSerializer()->deserialize(serializedData);
            if constexpr (std::is_pointer_v<S>) {
                return (S)resPtr;
            } else {
                if (resPtr == nullptr) {
                    return std::numeric_limits<S>::max();
                } else {
                    auto res = *static_cast<S*>(resPtr);
                    delete static_cast<S*>(resPtr);
                    return res;
                }
            }
        }
    };

    template <typename UV>
    UV getRes(void* res)
    {
        if constexpr (std::is_pointer_v<UV>) {
            return (UV)res;
        } else {
            return *(UV)res;
        }
    }

    void putByBatch(N& nameSpace, std::unordered_map<K, S>& pendingUpdates)
    {
        if (pendingUpdates.empty()) {
            return;
        }

        ROCKSDB_NAMESPACE::WriteBatch putBatch;

        TypeSerializer* vSerializer = getStateSerializer();

        DataOutputSerializer keyOutputSerializer;
        OutputBufferStatus keyOutputBufferStatus;
        keyOutputSerializer.setBackendBuffer(&keyOutputBufferStatus);

        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

        for (auto& entry : pendingUpdates) {
            keyContext->setCurrentKey(entry.first);

            keyOutputSerializer.clear();
            ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(keyOutputSerializer, nameSpace);

            valueOutputSerializer.clear();

            if constexpr (std::is_pointer_v<S>) {
                vSerializer->serialize(entry.second, valueOutputSerializer);
            } else {
                vSerializer->serialize(&entry.second, valueOutputSerializer);
            }

            ROCKSDB_NAMESPACE::Slice sliceValue(
                reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());
            putBatch.Put(table, sliceKey, sliceValue);
        }
        ROCKSDB_NAMESPACE::WriteOptions batchWriteOptions = writeOptions;
        batchWriteOptions.memtable_insert_hint_per_batch = true;
        auto s3 = rocksDb->Write(batchWriteOptions, &putBatch);

        if (!s3.ok()) {
            THROW_RUNTIME_ERROR("rocksdb state table put by batch failed, status is << " << s3.ToString());
        }
    };

    void put(N& nameSpace, const S& state)
    {
        // 存入
        LOG("RocksDB put");
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

        // value序列化
        TypeSerializer* vSerializer = getStateSerializer();
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

        S tmpS = state;

        if constexpr (std::is_pointer_v<S>) {
            vSerializer->serialize(tmpS, valueOutputSerializer);
        } else {
            vSerializer->serialize(&tmpS, valueOutputSerializer);
        }

        ROCKSDB_NAMESPACE::Slice sliceValue(
            reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());
        auto s3 = rocksDb->Put(writeOptions, table, sliceKey, sliceValue);
        if (!s3.ok()) {
            THROW_RUNTIME_ERROR("rocksdb state table put failed, status is << " << s3.ToString());
        }
    };

    void clear(N& nameSpace)
    {
        // 存入
        LOG("RocksDB value state clear");
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);
        rocksDb->Delete(writeOptions, table, sliceKey);
    }

    // for list state
    void add(N& nameSpace, const S& value)
    {
        // 存入
        LOG("RocksDB list value state add");
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

        // value序列化
        TypeSerializer* vSerializer = getStateSerializer();
        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

        S tmpS = value;

        if constexpr (std::is_pointer_v<S>) {
            vSerializer->serialize(tmpS, valueOutputSerializer);
        } else {
            vSerializer->serialize(&tmpS, valueOutputSerializer);
        }

        ROCKSDB_NAMESPACE::Slice sliceValue(
            reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());

        const rocksdb::Status& status = rocksDb->Merge(writeOptions, table, sliceKey, sliceValue);
        if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to add value to RocksDB: " << status.ToString());
        }
    }

    std::vector<S>* getList(N& nameSpace)
    {
        LOG("RocksDB list value state get");
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

        std::string valueInTable;
        ROCKSDB_NAMESPACE::Status s = rocksDb->Get(readOptions, table, sliceKey, &valueInTable);
        if (!s.ok() || valueInTable.length() == 0) {
            return nullptr;
        }
        DataInputDeserializer serializedData(
            reinterpret_cast<const uint8_t*>(valueInTable.data()), valueInTable.length(), 0);
        return deserializeList(serializedData);
    }

    void addAll(N& nameSpace, const vector<S>& values)
    {
        if (values.empty()) {
            WARN_RELEASE("values is emtpy when addAll to rocksdb");
            return;
        }
        // 存入
        LOG("RocksDB list value state addAll");
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);
        ROCKSDB_NAMESPACE::Slice sliceKey = GetKeyNameSpaceSlice(outputSerializer, nameSpace);

        DataOutputSerializer valueOutputSerializer;
        OutputBufferStatus valueOutputBufferStatus;
        valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

        const rocksdb::Slice slice = serializeList(valueOutputSerializer, values);
        const rocksdb::Status& status = rocksDb->Merge(writeOptions, table, sliceKey, slice);
        if (!status.ok()) {
            THROW_RUNTIME_ERROR("Failed to addAll to RocksDB: " << status.ToString());
        }
    }

    typename InternalKvState<K, N, S>::StateIncrementalVisitor* getStateIncrementalVisitor(
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
            reinterpret_cast<const char*>(serializedBatchInfo.dataAddress), serializedBatchInfo.dataSize);

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
                reinterpret_cast<const char*>(serializedBatchInfo.dataAddress), serializedBatchInfo.dataSize);
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
        // todo: add和clear的序列化可能逻辑不一致，待确认
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
            int64_t sequenceNumberI64 = static_cast<int64_t>(sequenceNumber);
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

    // [FALCON] function which will be called in RocksdbValueState
    K getCurrentKey()
    {
        return keyContext->getCurrentKey();
    }
    void setCurrentKey(K newKey)
    {
        keyContext->setCurrentKey(newKey);
    }

    class StateEntryIterator : public InternalKvState<K, N, S>::StateIncrementalVisitor {
    public:
        S nextEntries() override;

        StateEntryIterator(int recommendedMaxNumberOfReturnedRecords, RocksdbStateTable<K, N, S>* table);

        bool hasNext() override;

    private:
        int recommendedMaxNumberOfReturnedRecords;
        int keyGroupIndex;
        typename InternalKvState<K, N, S>::StateIncrementalVisitor* stateIncrementalVisitor;
        RocksdbStateTable<K, N, S>* table;

        void init();
    };

protected:
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
            if (key.size() < static_cast<size_t>(keyGroupPrefixBytes_) + sizeof(int64_t)) {
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
    // KeyGroupRange *keyGroupRange;
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* table; // 是不是编程columnsFamily
    ROCKSDB_NAMESPACE::ColumnFamilyHandle* VBTable;
    std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo;
    // per-state RocksDB memory estimate (bytes), set in createTable.
    int64_t stateMemoryBytes_ = 0;
    int size = 0;
    omnistream::SequenceNumberHelper sequenceNumberHelper_{}; // only used for VectorBatch storage
    ROCKSDB_NAMESPACE::DB* rocksDb;
    int keyGroupPrefixBytes_ = 1;
    byte DELIMITER = (byte)',';
    ROCKSDB_NAMESPACE::ReadOptions readOptions;
    ROCKSDB_NAMESPACE::WriteOptions writeOptions;

    ROCKSDB_NAMESPACE::Slice serializeList(DataOutputSerializer& valueOutputSerializer, const std::vector<S>& values)
    {
        TypeSerializer* vSerializer = getStateSerializer();
        bool first = true;
        for (const auto& item : values) {
            if (first) {
                first = false;
            } else {
                valueOutputSerializer.write((uint32_t)DELIMITER);
            }

            if constexpr (std::is_pointer_v<S>) {
                if (item == nullptr) {
                    THROW_RUNTIME_ERROR("You cannot add null to a value list.");
                }
                vSerializer->serialize(item, valueOutputSerializer);
            } else {
                vSerializer->serialize((void*)&item, valueOutputSerializer);
            }
        }
        return ROCKSDB_NAMESPACE::Slice(
            reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());
    }

    std::vector<S>* deserializeList(DataInputDeserializer serializedData)
    {
        auto* result = new std::vector<S>();
        while (serializedData.Available() > 0) {
            void* resPtr = getStateSerializer()->deserialize(serializedData);
            if constexpr (std::is_pointer_v<S>) {
                if constexpr (std::is_base_of_v<RowData, std::remove_pointer_t<S>>) {
                    auto* br = dynamic_cast<BinaryRowData*>(static_cast<RowData*>(resPtr));
                    if (br != nullptr) {
                        result->push_back(static_cast<S>(br->copy()));
                    } else {
                        result->push_back((S)resPtr);
                    }
                } else {
                    result->push_back((S)resPtr);
                }
            } else {
                if (resPtr == nullptr) {
                    result->push_back(std::numeric_limits<S>::max());
                } else {
                    auto res = *static_cast<S*>(resPtr);
                    delete static_cast<S*>(resPtr);
                    result->push_back(res);
                }
            }
            if (serializedData.Available() > 0) {
                serializedData.readByte();
            }
        }
        return result;
    }
};

template <typename K, typename N, typename S>
RocksdbStateTable<K, N, S>::RocksdbStateTable(
    InternalKeyContext<K>* keyContext,
    std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> metaInfo,
    TypeSerializer* keySerializer)
{
    this->keyContext = keyContext;
    this->metaInfo = std::move(metaInfo);
    this->keySerializer = keySerializer;
    this->keyGroupPrefixBytes_ =
        CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(keyContext->getNumberOfKeyGroups());
    writeOptions.disableWAL = true;
    sequenceNumberHelper_ = omnistream::SequenceNumberHelper(keyContext->getNumberOfKeyGroups());
}

template <typename K, typename N, typename S>
RocksdbStateTable<K, N, S>::~RocksdbStateTable()
{
}

template <typename K, typename N, typename S>
std::vector<K>* RocksdbStateTable<K, N, S>::getKeys(const N& nameSpace)
{
    return nullptr;
}

template <typename K, typename N, typename S>
std::vector<std::tuple<K, N>>* RocksdbStateTable<K, N, S>::getKeysAndNamespace()
{
    return nullptr;
}

#endif // OMNISTREAM_ROCKSDBSTATETABLE_H
