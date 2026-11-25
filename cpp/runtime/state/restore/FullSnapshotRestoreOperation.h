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

#ifndef OMNISTREAM_FULLSNAPSHOTRESTOREOPERATION_H
#define OMNISTREAM_FULLSNAPSHOTRESTOREOPERATION_H

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <filesystem>
#include <functional>

#include "RocksDBRestoreOperation.h"
#include "SavepointRestoreResult.h"
#include "SavepointRestoreResultIterator.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "typeutils/TypeSerializer.h"
#include "typeutils/TypeSerializerSnapshot.h"

namespace fs = std::filesystem;
/**
 * Full snapshot restore operation for managing complete state restoration
 *
 * This class implements the unified binary format restoration process that all
 * state backends should support. It handles:
 * - Keyed backend meta information restoration
 * - State meta information processing
 * - Key group data iteration
 * - Serialization compatibility checks
 * - Stream compression/decompression
 *
 */
template <typename K>
class FullSnapshotRestoreOperation {
public:
    /**
     * Constructor for FullSnapshotRestoreOperation
     *
     * @param keyGroupRange range of key groups for this restore operation
     * @param restoreStateHandles collection of state handles to restore from
     * @param keySerializerProvider provider for key serialization
     */
    FullSnapshotRestoreOperation(
        KeyGroupRange* keyGroupRange,
        const std::vector<std::shared_ptr<KeyedStateHandle>>& restoreStateHandles,
        std::shared_ptr<TypeSerializer> keySerializerProvider,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge);

    ~FullSnapshotRestoreOperation();

    std::unique_ptr<SavepointRestoreResultIterator> restore();

private:
    KeyGroupRange* keyGroupRange_;
    std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles_;
    std::shared_ptr<TypeSerializer> keySerializerProvider_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
    bool isKeySerializerCompatibilityChecked_;
};

// Template implementation
template <typename K>
std::unique_ptr<SavepointRestoreResultIterator> FullSnapshotRestoreOperation<K>::restore()
{
    return std::make_unique<SavepointRestoreResultIterator>(restoreStateHandles_, omniTaskBridge_);
}

template <typename K>
FullSnapshotRestoreOperation<K>::FullSnapshotRestoreOperation(
    KeyGroupRange* keyGroupRange,
    const std::vector<std::shared_ptr<KeyedStateHandle>>& restoreStateHandles,
    std::shared_ptr<TypeSerializer> keySerializerProvider,
    std::shared_ptr<OmniTaskBridge> omniTaskBridge)
    : keyGroupRange_(keyGroupRange),
      restoreStateHandles_(restoreStateHandles),
      keySerializerProvider_(keySerializerProvider),
      omniTaskBridge_(omniTaskBridge),
      isKeySerializerCompatibilityChecked_(false)
{
    // Filter out null state handles
    restoreStateHandles_.erase(
        std::remove(restoreStateHandles_.begin(), restoreStateHandles_.end(), nullptr), restoreStateHandles_.end());
}

template <typename K>
FullSnapshotRestoreOperation<K>::~FullSnapshotRestoreOperation()
{
}

#endif // OMNISTREAM_FULLSNAPSHOTRESTOREOPERATION_H
