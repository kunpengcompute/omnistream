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

#ifndef FLINK_TNEL_ABSTRACTKEYEDSTATEBACKEND_H
#define FLINK_TNEL_ABSTRACTKEYEDSTATEBACKEND_H
#include <emhash7.hpp>
#include <emhash7.hpp>
#include "InternalKeyContext.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "heap/StateTable.h"
#include "KeyedStateBackend.h"
#include "state/ttl/TtlStateFactory.h"
#include "CheckpointableKeyedStateBackend.h"
#include "runtime/checkpoint/SnapshotType.h"
#include "runtime/checkpoint/CheckpointListener.h"

namespace omnistream {
class OperatorStateMetricGroup;
}

template <typename K>
class AbstractKeyedStateBackend : public CheckpointableKeyedStateBackend<K>, public CheckpointListener {
public:
    ~AbstractKeyedStateBackend()
    {
        LOG("AbstractKeyedStateBackend");
        delete keySerializer;
        keySerializer = nullptr;
    }

    AbstractKeyedStateBackend(TypeSerializer* keySerializer, InternalKeyContext<K>* context)
        : context(context),
          keySerializer(keySerializer) {};

    void setCurrentKey(K newKey);
    K getCurrentKey();

    /**
     * N: The data type that the serializer serializes.
     * S: The type of the State objects created from this {@code StateDescriptor}.
     * V: The type of the value of the state object described by this state descriptor.
     * */
    template <typename N, typename S, typename V>
    S* getOrCreateKeyedState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDescriptor);
    TypeSerializer* getKeySerializer()
    {
        return keySerializer;
    };
    template <typename N, typename S, typename V>
    S* getPartitionedState(N nameSpace, TypeSerializer* namespaceSerializer, StateDescriptor* stateDescriptor);

    KeyGroupRange* getKeyGroupRange() override;

    void dispose() override;

    virtual bool requiresLegacySynchronousTimerSnapshots(SnapshotType* checkpointType)
    {
        return false;
    }

protected:
    InternalKeyContext<K>* context;
    TypeSerializer* keySerializer = nullptr;
    std::string lastName;
    // This state is InternalKvState
    uintptr_t lastState;

    // This State* is pointer to a InternalKvState<K, N, S> such as HeapMapState or HeapValueState
    emhash7::HashMap<std::string, uintptr_t> keyValueStatesByName;
};

template <typename K>
void AbstractKeyedStateBackend<K>::setCurrentKey(K newKey)
{
    context->setCurrentKey(newKey);
}

template <typename K>
inline K AbstractKeyedStateBackend<K>::getCurrentKey()
{
    return this->context->getCurrentKey();
}

template <typename K>
inline KeyGroupRange* AbstractKeyedStateBackend<K>::getKeyGroupRange()
{
    return context->getKeyGroupRange();
}

template <typename K>
inline void AbstractKeyedStateBackend<K>::dispose()
{
    lastName = "";
    lastState = 0;
    keyValueStatesByName.clear();
}

template <typename K>
template <typename N, typename S, typename V>
S* AbstractKeyedStateBackend<K>::getOrCreateKeyedState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDescriptor)
{
    S* kvState;
    auto it = keyValueStatesByName.find(stateDescriptor->getName());
    if (it == keyValueStatesByName.end()) {
        kvState =
            TtlStateFactory<K, N, S, V>::createStateAndWrapWithTtlIfEnabled(namespaceSerializer, stateDescriptor, this);
        keyValueStatesByName[stateDescriptor->getName()] = reinterpret_cast<uintptr_t>(kvState);
    } else {
        kvState = reinterpret_cast<S*>(it->second);
    }
    return kvState;
}

template <typename K>
template <typename N, typename S, typename V>
S* AbstractKeyedStateBackend<K>::getPartitionedState(
    N nameSpace, TypeSerializer* namespaceSerializer, StateDescriptor* stateDescriptor)
{
    // S here is InternalKvState<K, N, V>
    std::string name = stateDescriptor->getName();
    if (!lastName.empty() && lastName == name) {
        S* lastStateAsS = reinterpret_cast<S*>(lastState);
        lastStateAsS->setCurrentNamespace(nameSpace);
        return lastStateAsS;
    }
    uintptr_t previous;
    auto it = keyValueStatesByName.find(name);
    if (it != keyValueStatesByName.end()) {
        previous = it->second;
        lastState = previous;
        reinterpret_cast<S*>(lastState)->setCurrentNamespace(nameSpace);
        lastName = name;
        return reinterpret_cast<S*>(previous);
    }

    S* state = getOrCreateKeyedState<N, S, V>(namespaceSerializer, stateDescriptor);
    S* kvState = reinterpret_cast<S*>(state);

    lastName = name;
    lastState = reinterpret_cast<uintptr_t>(state);
    kvState->setCurrentNamespace(nameSpace);

    return state;
}
#endif // FLINK_TNEL_ABSTRACTKEYEDSTATEBACKEND_H
