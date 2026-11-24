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

#ifndef OMNISTREAM_CHECKPOINT_STORAGE_ACCESS_H
#define OMNISTREAM_CHECKPOINT_STORAGE_ACCESS_H

#include "runtime/state/CompletedCheckpointStorageLocation.h"
#include "runtime/state/CheckpointStorageLocation.h"
#include "runtime/state/CheckpointStorageWorkerView.h"

namespace omnistream {

class CheckpointStorageAccess : public CheckpointStorageWorkerView {
public:
    virtual ~CheckpointStorageAccess() = default;

    virtual CheckpointStorageLocation* initializeLocationForCheckpoint(long checkpointId) = 0;
    virtual CheckpointStorageLocation* initializeLocationForSavepoint(
        long checkpointId, const std::string& externalPath) = 0;

    virtual bool hasDefaultSavepointLocation() = 0;

    virtual std::shared_ptr<CheckpointStreamFactory> resolveCheckpointStorageLocation(
        int64_t checkpointId, std::shared_ptr<CheckpointStorageLocationReference> reference) = 0;
    virtual std::shared_ptr<CheckpointStreamFactory> resolveCheckpointStorageLocation(long checkpointId) = 0;
    virtual void initializeBaseLocations() = 0;
};

} // namespace omnistream

#endif // OMNISTREAM_CHECKPOINT_STORAGE_ACCESS_H
