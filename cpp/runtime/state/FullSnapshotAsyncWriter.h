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

#ifndef OMNISTREAM_FULLSNAPSHOTASYNCWRITER
#define OMNISTREAM_FULLSNAPSHOTASYNCWRITER
#include "bridge/OmniTaskBridge.h"
#include "state/KeyedStateHandle.h"
#include "state/FullSnapshotResources.h"
#include "state/SnapshotStrategy.h"
#include "checkpoint/SnapshotType.h"
#include "checkpoint/CheckpointOptions.h"
class FullSnapshotAsyncWriter : public SnapshotResultSupplier<KeyedStateHandle> {
private:
    std::shared_ptr<FullSnapshotResources> snapshotResources_;
    long checkpointId_;
    std::shared_ptr<SnapshotType> snapshotType_;
    CheckpointOptions* checkpointOptions_;
    std::string keySerializer_;

public:
    FullSnapshotAsyncWriter(
        std::shared_ptr<SnapshotType> snapshotType,
        CheckpointOptions* checkpointOptions,
        long checkpointId,
        const std::shared_ptr<FullSnapshotResources>& snapshotResources,
        std::string keySerializer);
    std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(std::shared_ptr<omnistream::OmniTaskBridge> bridge) override;
};
#endif
