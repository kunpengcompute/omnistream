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

#ifndef OMNISTREAM_ROCKSDBSTATEBACKEND
#define OMNISTREAM_ROCKSDBSTATEBACKEND

#include "EmbeddedRocksDBStateBackend.h"
#include "OperatorStateBackend.h"
#include <set>
#include "runtime/state/StateBackend.h"

class RocksDBStateBackend : public StateBackend {
public:
    explicit RocksDBStateBackend(TernaryBoolean enableIncrementalCheckpoint)
    {
        rocksDbStateBackend = new EmbeddedRocksDBStateBackend(enableIncrementalCheckpoint);
    };

    explicit RocksDBStateBackend(TaskInformationPOD taskConfiguration)
    {
        rocksDbStateBackend = new EmbeddedRocksDBStateBackend(taskConfiguration);
    };
    ~RocksDBStateBackend();

    template <typename K>
    AbstractKeyedStateBackend<K>* createKeyedStateBackend(
        omnistream::EnvironmentV2* env,
        std::set<KeyedStateHandle> stateHandles,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        int numberOfKeyGroups)
    {
    }

    template <typename K>
    AbstractKeyedStateBackend<K>* createKeyedStateBackend(
        omnistream::EnvironmentV2* env,
        std::string operatorIdentifier,
        std::set<std::shared_ptr<KeyedStateHandle>> stateHandles,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        int numberOfKeyGroups,
        int alternativeIdx)
    {
        return rocksDbStateBackend->template createKeyedStateBackend<K>(
            env, operatorIdentifier, stateHandles, keyGroupRange, keySerializer, numberOfKeyGroups, alternativeIdx);
    };

    OperatorStateBackend* createOperatorStateBackend(
        omnistream::EnvironmentV2* env_,
        std::string operatorIdentifier_,
        std::set<std::shared_ptr<OperatorStateHandle>> stateHandles_)
    {
        return rocksDbStateBackend->createOperatorStateBackend(env_, operatorIdentifier_, stateHandles_);
    }

private:
    EmbeddedRocksDBStateBackend* rocksDbStateBackend;
};

#endif // OMNISTREAM_ROCKSDBSTATEBACKEND
