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

#include "StateBackend.h"
#include "hashmap/HashMapStateBackend.h"

template <typename K>
AbstractKeyedStateBackend<K>* StateBackend::createKeyedStateBackend(
    omnistream::EnvironmentV2* env,
    std ::set<KeyedStateHandle> stateHandles,
    KeyGroupRange* keyGroupRange,
    TypeSerializer* keySerializer,
    int numberOfKeyGroups)
{
    HashMapStateBackend* hashMapCasted = dynamic_cast<HashMapStateBackend*>(this);
    if (hashMapCasted) {
        return hashMapCasted->template createKeyedStateBackend<K>(
            env, stateHandles, keyGroupRange, keySerializer, numberOfKeyGroups);
    }
    return nullptr;
}

OperatorStateBackend* StateBackend::createOperatorStateBackend(
    omnistream::EnvironmentV2* env,
    std::string operatorIdentifier,
    std::set<std::shared_ptr<OperatorStateHandle>> stateHandles)
{
    HashMapStateBackend* hashMapCasted = dynamic_cast<HashMapStateBackend*>(this);
    if (hashMapCasted) {
        return hashMapCasted->createOperatorStateBackend(env, operatorIdentifier, stateHandles);
    }
    return nullptr;
}
