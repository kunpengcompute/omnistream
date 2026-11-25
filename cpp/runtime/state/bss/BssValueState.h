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

#include "runtime/state/internal/InternalValueState.h"
#include "BssStateTable.h"
#include "api/common/state/StateDescriptor.h"

template <typename K, typename N, typename V>
class BssValueState : public InternalValueState<K, N, V> {
public:
    TypeSerializer* getKeySerializer()
    {
        return keySerializer;
    };
    TypeSerializer* getNamespaceSerializer()
    {
        return namespaceSerializer;
    };
    TypeSerializer* getValueSerializer()
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
    void setCurrentNamespace(N nameSpace) override
    {
        currentNamespace = nameSpace;
    };
    V value() override
    {
        auto result = stateTable->get(currentNamespace);
        if constexpr (std::is_pointer<V>::value) {
            return result == nullptr ? defaultValue : result;
        } else {
            return result == std::numeric_limits<V>::max() ? defaultValue : result;
        }
    };
    void update(const V& value, bool copyKey = false) override
    {
        stateTable->put(currentNamespace, value);
    };

    void setDefaultValue(V value)
    {
        defaultValue = value;
    };
    static BssValueState<K, N, V>* create(
        StateDescriptor* stateDesc, BssStateTable<K, N, V>* stateTable, TypeSerializer* keySerializer)
    {
        return new BssValueState<K, N, V>(
            stateTable, keySerializer, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer(), V());
    };

    static BssValueState<K, N, V>* updateState(
        StateDescriptor* stateDesc, BssStateTable<K, N, V>* stateTable, BssValueState<K, N, V>* existingState)
    {
        existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
        existingState->setValueSerializer(stateTable->getStateSerializer());
        return existingState;
    }

    BssValueState(
        BssStateTable<K, N, V>* stateTable,
        TypeSerializer* keySerializer,
        TypeSerializer* valueSerializer,
        TypeSerializer* namespaceSerializer,
        V defaultValue)
        : stateTable(stateTable),
          keySerializer(keySerializer),
          valueSerializer(valueSerializer),
          namespaceSerializer(namespaceSerializer),
          defaultValue(defaultValue) {};

    ~BssValueState()
    {
        delete stateTable;
    };

    void CreateTable(ock::bss::BoostStateDBPtr& _dbPtr)
    {
        stateTable->createTable(_dbPtr);
    }

    void clearVectors(int64_t currentTimestamp)
    {
    }

    void clear() override
    {
        stateTable->clear(currentNamespace);
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

private:
    BssStateTable<K, N, V>* stateTable;
    TypeSerializer* keySerializer;
    TypeSerializer* valueSerializer;
    TypeSerializer* namespaceSerializer;
    V defaultValue;
    N currentNamespace;
};

#endif
