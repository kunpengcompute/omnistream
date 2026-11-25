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

#ifndef OMNISTREAM_FULLSNAPSHOTRESOURCES
#define OMNISTREAM_FULLSNAPSHOTRESOURCES
#include <memory>
#include <string>
#include <vector>

#include "KeyGroupRange.h"
#include "state/KeyGroupRange.h"
#include "state/KeyValueStateIterator.h"
#include "state/SnapshotResources.h"
class FullSnapshotResources : public SnapshotResources {
public:
    virtual const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() = 0;
    virtual KeyGroupRange* getKeyGroupRange() = 0;
    virtual TypeSerializer* getKeySerializer() = 0;
    virtual std::shared_ptr<KeyValueStateIterator> createKVStateIterator() = 0;
    virtual bool isHeapPriorityQueueStateId(int /*kvStateId*/) const
    {
        return false;
    }
    virtual int getKeyGroupPrefixBytes() const
    {
        return 1;
    }
    virtual void cleanup() = 0;
};
#endif
