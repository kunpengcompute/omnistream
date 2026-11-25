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

#ifndef OMNISTREAM_STATEBACKEND
#define OMNISTREAM_STATEBACKEND

#include "AbstractKeyedStateBackend.h"
#include "OperatorStateBackend.h"
#include "runtime/state/heap/HeapKeyedStateBackendBuilder.h"
#include "runtime/execution/OmniEnvironment.h"

class StateBackend {
public:
    virtual ~StateBackend() = default;

    template <typename K>
    AbstractKeyedStateBackend<K>* createKeyedStateBackend(
        omnistream::EnvironmentV2* env,
        std::set<KeyedStateHandle> stateHandles,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        int numberOfKeyGroups);

    template <typename K>
    AbstractKeyedStateBackend<K>* createKeyedStateBackend(
        omnistream::EnvironmentV2* env,
        std::string operatorIdentifier,
        std::set<std::shared_ptr<KeyedStateHandle>> stateHandles,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        int numberOfKeyGroups,
        int alternativeIdx) {};

    OperatorStateBackend* createOperatorStateBackend(
        omnistream::EnvironmentV2* env,
        std::string operatorIdentifier,
        std::set<std::shared_ptr<OperatorStateHandle>> stateHandles);
};

#endif // OMNISTREAM_STATEBACKEND
