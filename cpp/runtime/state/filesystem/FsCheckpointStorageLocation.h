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

#ifndef FLINK_TNEL_FSCHECKPOINTSTORAGELOCATION_H
#define FLINK_TNEL_FSCHECKPOINTSTORAGELOCATION_H
#include "core/fs/Path.h"
#include "runtime/state/CheckpointStorageLocationReference.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "FsCheckpointStreamFactory.h"
#include "runtime/state/filesystem/AbstractFsCheckpointStorageAccess.h"

namespace omnistream {

class FsCheckpointStorageLocation : public FsCheckpointStreamFactory {
public:
    FsCheckpointStorageLocation(
        int fileSystem,
        std::shared_ptr<Path> checkpointDir,
        std::shared_ptr<Path> sharedStateDir,
        std::shared_ptr<Path> taskOwnedStateDir,
        std::shared_ptr<CheckpointStorageLocationReference> reference,
        int fileStateSizeThreshold,
        int writeBufferSize)
        : FsCheckpointStreamFactory(fileSystem, checkpointDir, sharedStateDir, fileStateSizeThreshold, writeBufferSize),
          fileSystem(fileSystem),
          checkpointDirectory(checkpointDir),
          sharedStateDirectory(sharedStateDir),
          taskOwnedStateDirectory(taskOwnedStateDir),
          reference(reference),
          fileStateSizeThreshold(fileStateSizeThreshold),
          writeBufferSize(writeBufferSize)
    {
        if (fileStateSizeThreshold < 0) {
            THROW_LOGIC_EXCEPTION("The threshold for file state size must be zero or larger");
        }

        if (writeBufferSize < 0) {
            THROW_LOGIC_EXCEPTION("The write buffer size must be zero or larger");
        }

        Path metadataDir = Path(*checkpointDir); // TTODO EntropyInjector.removeEntropyMarkerIfPresent
        metadataFilePath = std::make_shared<Path>(metadataDir, AbstractFsCheckpointStorageAccess::METADATA_FILE_NAME);
    }

    std::shared_ptr<Path> getCheckpointDirectory()
    {
        return checkpointDirectory;
    }

    std::shared_ptr<Path> getSharedStateDirectory()
    {
        return sharedStateDirectory;
    }

    std::shared_ptr<Path> getTaskOwnedStateDirectory()
    {
        return taskOwnedStateDirectory;
    }

    std::shared_ptr<Path> getMetadataFilePath()
    {
        return metadataFilePath;
    }

private:
    int fileSystem;
    std::shared_ptr<Path> checkpointDirectory;
    std::shared_ptr<Path> sharedStateDirectory;
    std::shared_ptr<Path> taskOwnedStateDirectory;
    std::shared_ptr<Path> metadataFilePath;
    std::shared_ptr<CheckpointStorageLocationReference> reference;
    int fileStateSizeThreshold;
    int writeBufferSize;
};

} // namespace omnistream
#endif // FLINK_TNEL_FSCHECKPOINTSTORAGELOCATION_H
