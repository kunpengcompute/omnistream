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
#ifdef WITH_OMNISTATESTORE

#include "api/common/state/MapState.h"
#include "BssMapStateTable.h"
#include "api/common/state/StateDescriptor.h"
#include "state/internal/InternalKvState.h"

template <typename K, typename N, typename UK, typename UV>
class BssMapState : public MapState<UK, UV>, public InternalKvState<K, N, emhash7::HashMap<UK, UV>*> {
public:
    BssMapState(
        BssMapStateTable<K, N, UK, UV>* stateTable,
        TypeSerializer* keySerializer,
        TypeSerializer* valueSerializer,
        TypeSerializer* namespaceSerializer)
        : stateTable(stateTable),
          keySerializer(keySerializer),
          valueSerializer(valueSerializer),
          namespaceSerializer(namespaceSerializer)
    {
    }

    ~BssMapState() override = default;

    [[nodiscard]] TypeSerializer* getKeySerializer() const
    {
        return keySerializer;
    };

    [[nodiscard]] TypeSerializer* getNamespaceSerializer() const
    {
        return namespaceSerializer;
    };

    [[nodiscard]] TypeSerializer* getValueSerializer() const
    {
        return valueSerializer;
    };

    void setNamespaceSerializer(TypeSerializer* serializer)
    {
        namespaceSerializer = serializer;
    };

    void setValueSerializer(TypeSerializer* serializer)
    {
        valueSerializer = serializer;
    };

    std::optional<UV> get(const UK& userKey) override
    {
        LOG("BSS MapState get");
        UV userValue = stateTable->get(currentNamespace, userKey);
        if constexpr (std::is_pointer_v<UV>) {
            if (userValue == nullptr) {
                return std::nullopt;
            }
        } else {
            if (userValue == std::numeric_limits<UV>::max()) {
                return std::nullopt;
            }
        }
        return std::make_optional<UV>(userValue);
    };

    Object* Get(Object* userKey) override
    {
        return nullptr;
    };

    java_util_Iterator* iterator() override
    {
        return nullptr;
    };

    void put(const UK& userKey, const UV& userValue) override
    {
        LOG("BSS MapState put");
        stateTable->put(currentNamespace, userKey, userValue);
    };

    void remove(const UK& userKey) override
    {
        LOG("BSS MapState remove");
        stateTable->remove(currentNamespace, userKey);
    };

    bool contains(const UK& userKey) override
    {
        return false;
    };

    void update(const UK& key, const UV& value) override
    {
        stateTable->put(currentNamespace, key, value);
    };

    void setCurrentNamespace(N nameSpace) override
    {
        this->currentNamespace = nameSpace;
    };

    void clear() override {};

    static BssMapState<K, N, UK, UV>* create(
        StateDescriptor* stateDesc, BssMapStateTable<K, N, UK, UV>* stateTable, TypeSerializer* keySerializer)
    {
        return new BssMapState<K, N, UK, UV>(
            stateTable, keySerializer, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer());
    };

    static BssMapState<K, N, UK, UV>* update(
        StateDescriptor* stateDesc,
        BssMapStateTable<K, N, UK, UV>* stateTable,
        BssMapState<K, N, UK, UV>* existingState)
    {
        existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
        existingState->setValueSerializer(stateTable->getStateSerializer());
        return existingState;
    };

    // This gets the pointer to the actual map (a value for state),
    // like Join's emhash<RowData*, int> with currentNamespace and currentKey
    emhash7::HashMap<UK, UV>* entries() override
    {
        return stateTable->entries(currentNamespace);
    };

    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch) override
    {
        stateTable->addVectorBatch(keyGroup, vectorBatch);
    };

    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup) override
    {
        stateTable->addVectorBatches(vectorBatchByKeyGroup);
    };

    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber) override
    {
        return stateTable->getVectorBatch(keyGroup, sequenceNumber);
    };

    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup) override
    {
        return stateTable->getVectorBatches(keyGroup);
    };

    uint32_t getNextSequenceNumber(int32_t keyGroup) override
    {
        return stateTable->getNextSequenceNumber(keyGroup);
    };

    void clearVectorBatches(int64_t currentTimestamp) override
    {
        stateTable->clearVectorBatches(currentTimestamp);
    }

    void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete) override
    {
        stateTable->clearVectorBatches(keyGroup, sequenceNumbersToDelete);
    }

    void CreateTable(ock::bss::BoostStateDBPtr& _dbPtr)
    {
        stateTable->createTable(_dbPtr);
    };

private:
    BssMapStateTable<K, N, UK, UV>* stateTable;
    TypeSerializer* keySerializer;
    TypeSerializer* valueSerializer;
    TypeSerializer* namespaceSerializer;
    N currentNamespace;
};

#endif
