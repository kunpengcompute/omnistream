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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/rocksdb/iterator/SingleStateIterator.h"

class HeapPriorityQueueSnapshotRestoreWrapperBase {
public:
    virtual ~HeapPriorityQueueSnapshotRestoreWrapperBase() = default;

    virtual std::shared_ptr<StateMetaInfoSnapshot> snapshotMetaInfo() = 0;

    virtual const std::string& getStateName() const
    {
        static const std::string emptyName;
        return emptyName;
    }

    virtual std::unique_ptr<SingleStateIterator> createSnapshotIterator(int kvStateId, int keyGroupPrefixBytes) = 0;

    /**
     * Restore one full-snapshot PRIORITY_QUEUE entry. The key is encoded as:
     *   [key-group-prefix] + [serialized priority queue element]
     * and the value is empty. This mirrors Flink heap PQ restore semantics.
     */
    virtual void restoreSerializedElement(const std::vector<int8_t>& serializedKey, int keyGroupPrefixBytes) = 0;
};
