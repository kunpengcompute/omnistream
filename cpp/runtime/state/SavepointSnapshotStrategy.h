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

#ifndef OMNISTREAM_SAVEPOINTSNAPSHOTSTRATEGY_H
#define OMNISTREAM_SAVEPOINTSNAPSHOTSTRATEGY_H
#include "KeyedStateHandle.h"
#include "SnapshotStrategy.h"
#include "checkpoint/SavepointType.h"
#include "state/FullSnapshotResources.h"
#include "state/FullSnapshotAsyncWriter.h"
class SavepointSnapshotStrategy : public SnapshotStrategy<KeyedStateHandle, FullSnapshotResources> {
private:
    std::shared_ptr<FullSnapshotResources> savepointResources_;

public:
    SavepointSnapshotStrategy(const std::shared_ptr<FullSnapshotResources>& savepointResources)
        : savepointResources_(savepointResources)
    {
    }
    std::shared_ptr<FullSnapshotResources> syncPrepareResources(long checkpointId) override
    {
        return savepointResources_;
    }
    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<FullSnapshotResources>& snapshotResources,
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer = "") override
    {
        if (savepointResources_->getMetaInfoSnapshots().empty()) {
            struct EmptySnapshotResourceSupplier : public SnapshotResultSupplier<KeyedStateHandle> {
                std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(
                    std::shared_ptr<omnistream::OmniTaskBridge> bridge) override
                {
                    return SnapshotResult<KeyedStateHandle>::Empty();
                }
            };
            return std::make_shared<EmptySnapshotResourceSupplier>();
        }
        return std::make_shared<FullSnapshotAsyncWriter>(
            SavepointType::savepoint(SavepointFormatType::CANONICAL),
            checkpointOptions,
            checkpointId,
            snapshotResources,
            keySerializer);
    }
};
#endif
