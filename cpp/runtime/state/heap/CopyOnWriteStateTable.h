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

#include "StateTable.h"
#include "CopyOnWriteStateMap.h"

// No snapshotting features implemented
template <typename K, typename N, typename S>
class CopyOnWriteStateTable : public StateTable<K, N, S> {
public:
    CopyOnWriteStateTable(
        InternalKeyContext<K>* keyContext,
        RegisteredKeyValueStateBackendMetaInfo* metaInfo,
        TypeSerializer* keySerializer);

    virtual ~CopyOnWriteStateTable() override;

    void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo* metaInfo);
    int computeKeyGroupForKeyHash(K key);

protected:
    StateMap<K, N, S>* createStateMap() override;
    void initialize() override;
};

template <typename K, typename N, typename S>
inline StateMap<K, N, S>* CopyOnWriteStateTable<K, N, S>::createStateMap()
{
    return new omnistream::CopyOnWriteStateMap<K, N, S>(this->keySerializer);
}

template <typename K, typename N, typename S>
inline CopyOnWriteStateTable<K, N, S>::CopyOnWriteStateTable(
    InternalKeyContext<K>* keyContext, RegisteredKeyValueStateBackendMetaInfo* metaInfo, TypeSerializer* keySerializer)
    : StateTable<K, N, S>(keyContext, metaInfo, keySerializer)
{
    initialize();
}

template <typename K, typename N, typename S>
CopyOnWriteStateTable<K, N, S>::~CopyOnWriteStateTable()
{
}

template <typename K, typename N, typename S>
inline void CopyOnWriteStateTable<K, N, S>::initialize()
{
    for (int i = 0; i < this->keyGroupRange->getNumberOfKeyGroups(); i++) {
        this->keyGroupedStateMaps.push_back(createStateMap());
    }
}

template <typename K, typename N, typename S>
inline void CopyOnWriteStateTable<K, N, S>::setMetaInfo(RegisteredKeyValueStateBackendMetaInfo* metaInfo)
{
}

template <typename K, typename N, typename S>
inline int CopyOnWriteStateTable<K, N, S>::computeKeyGroupForKeyHash(K key)
{
    std::hash<K> keyHash;
    int group = keyHash(key) % StateTable<K, N, S>::keyContext->getNumberOfKeyGroups();
    return group;
}
