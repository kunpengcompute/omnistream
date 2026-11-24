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

#include "HashMapStateBackend.h"

template <typename K>
AbstractKeyedStateBackend<K>* HashMapStateBackend::createKeyedStateBackend(
    omnistream::EnvironmentV2* env,
    std ::set<KeyedStateHandle> stateHandles,
    KeyGroupRange* keyGroupRange,
    TypeSerializer* keySerializer,
    int numberOfKeyGroups)
{
    // TTODO
    // Get recovery config
    restoreState();
    HeapKeyedStateBackendBuilder<K> builder =
        HeapKeyedStateBackendBuilder<K>(keySerializer, numberOfKeyGroups, keyGroupRange);
    return builder.build();
}

OperatorStateBackend* HashMapStateBackend::createOperatorStateBackend(
    omnistream::EnvironmentV2* env,
    std::string operatorIdentifier,
    std::set<std::shared_ptr<OperatorStateHandle>> stateHandles)
{
    std::vector<std::shared_ptr<OperatorStateHandle>> stateVector(stateHandles.begin(), stateHandles.end());
    auto bridge = env->getTaskStateManager()->getTaskStateManagerBridge();
    auto omniTaskBridge = env->getTaskStateManager()->getOmniTaskBridge();

    const bool asynchronousSnapshots = true;
    DefaultOperatorStateBackendBuilder builder(
        asynchronousSnapshots, operatorIdentifier, stateVector, bridge, omniTaskBridge);
    return builder.build();
}

void HashMapStateBackend::restoreState()
{
    // restore states
}
