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

#ifndef OMNISTREAM_HASHMAPSTATEBACKEND
#define OMNISTREAM_HASHMAPSTATEBACKEND

#include <set>
#include "runtime/state/StateBackend.h"
#include "runtime/state/DefaultOperatorStateBackendBuilder.h"

class HashMapStateBackend : public StateBackend {
public:
    HashMapStateBackend() {};

    template <typename K>
    AbstractKeyedStateBackend<K>* createKeyedStateBackend(
        omnistream::EnvironmentV2* env,
        std::set<KeyedStateHandle> stateHandles,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        int numberOfKeyGroups);

    OperatorStateBackend* createOperatorStateBackend(
        omnistream::EnvironmentV2* env,
        std::string operatorIdentifier,
        std::set<std::shared_ptr<OperatorStateHandle>> stateHandles);

private:
    void restoreState();
};

#endif // OMNISTREAM_HASHMAPSTATEBACKEND
