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

#include "FsCheckpointStreamFactory.h"

FsCheckpointStreamFactory::FsCheckpointStreamFactory(
    int fileSystem,
    std::shared_ptr<Path> checkpointDirectory,
    std::shared_ptr<Path> sharedStateDirectory,
    int fileStateSizeThreshold,
    int writeBufferSize)
    : writeBufferSize(writeBufferSize),
      fileStateThreshold(fileStateSizeThreshold),
      fileSystem(fileSystem)
{
    if (fileStateSizeThreshold < 0) {
        THROW_LOGIC_EXCEPTION("The threshold for file state size must be zero or larger.");
    }

    if (writeBufferSize < 0) {
        THROW_LOGIC_EXCEPTION("The write buffer size must be zero or larger.");
    }

    if (fileStateSizeThreshold > MAX_FILE_STATE_THRESHOLD) {
        THROW_LOGIC_EXCEPTION("The threshold for file state size cannot be larger than MAX_FILE_STATE_THRESHOLD");
    }

    if (checkpointDirectory == nullptr) {
        THROW_LOGIC_EXCEPTION("checkpointDirectory should not be null");
    } else {
        this->checkpointDirectory = checkpointDirectory;
    }

    if (sharedStateDirectory == nullptr) {
        THROW_LOGIC_EXCEPTION("sharedStateDirectory should not be null");
    } else {
        this->sharedStateDirectory = sharedStateDirectory;
    }
}

CheckpointStateOutputStream* FsCheckpointStreamFactory::createCheckpointStateOutputStream(CheckpointedStateScope scope)
{
    std::shared_ptr<Path> target = getTargetPath(scope);
    int bufferSize = std::max(writeBufferSize, fileStateThreshold);
    bool entropyInjecting = false; // NOT IMPLEMENTED
    bool absolutePath = entropyInjecting || (scope == CheckpointedStateScope::SHARED);

    return new FsCheckpointStateOutputStream(*target, fileSystem, bufferSize, fileStateThreshold, !absolutePath);
}

std::shared_ptr<Path> FsCheckpointStreamFactory::getTargetPath(CheckpointedStateScope scope) const
{
    return (scope == CheckpointedStateScope::EXCLUSIVE) ? checkpointDirectory : sharedStateDirectory;
}
