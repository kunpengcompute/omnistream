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

#ifndef OMNISTREAM_CHECKPOINT_STORAGE_H
#define OMNISTREAM_CHECKPOINT_STORAGE_H

#include <string>
#include "CheckpointStreamFactory.h"
#include "CheckpointStorageAccess.h"
#include "core/fs/Path.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "CheckpointStorageLocation.h"
#include "CompletedCheckpointStorageLocation.h"
#include "filesystem/FsCheckpointStorageAccess.h"

namespace omnistream {
class CheckpointStorage {
public:
    virtual ~CheckpointStorage() = default;

    virtual std::shared_ptr<CheckpointStorageAccess> createCheckpointStorage(const JobIDPOD& jobId) = 0;

    std::shared_ptr<CheckpointStorageAccess> createCheckpointStorage(std::string tmpWorkingDirectory)
    {
        // Temp initialization
        auto path1 = std::make_shared<Path>(tmpWorkingDirectory);
        auto path2 = std::make_shared<Path>("test2");

        return std::make_shared<FsCheckpointStorageAccess>(path1, path2, JobIDPOD(), 100, 100);
    }
};
} // namespace omnistream

#endif // OMNISTREAM_CHECKPOINT_STORAGE_H
