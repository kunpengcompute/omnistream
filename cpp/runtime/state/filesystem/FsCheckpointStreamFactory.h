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

#ifndef FLINK_TNEL_FSCHECKPOINTSTREAMFACTORY_H
#define FLINK_TNEL_FSCHECKPOINTSTREAMFACTORY_H

#include "runtime/state/CheckpointStreamFactory.h"
#include "core/fs/Path.h"
#include "core/include/common.h"
#include "FsCheckpointStateOutputStream.h"

class FsCheckpointStreamFactory : public CheckpointStreamFactory {
public:
    FsCheckpointStreamFactory(
        int fileSystem,
        std::shared_ptr<Path> checkpointDirectory,
        std::shared_ptr<Path> sharedStateDirectory,
        int fileStateSizeThreshold,
        int writeBufferSize);

    ~FsCheckpointStreamFactory() = default;

    CheckpointStateOutputStream* createCheckpointStateOutputStream(CheckpointedStateScope scope) override;

    std::shared_ptr<Path> getTargetPath(CheckpointedStateScope scope) const;

    bool canFastDuplicate(StreamStateHandle* stateHandle, CheckpointedStateScope scope) override
    {
        NOT_IMPL_EXCEPTION;
    };

    std::vector<std::shared_ptr<StreamStateHandle>> duplicate(
        std::vector<StreamStateHandle*>& stateHandles, CheckpointedStateScope scope) override
    {
        NOT_IMPL_EXCEPTION;
    };

private:
    int writeBufferSize;
    int fileStateThreshold;
    std::shared_ptr<Path> checkpointDirectory;
    std::shared_ptr<Path> sharedStateDirectory;
    int fileSystem; // NOT IMPLEMENTED
    static const int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;
};

#endif // FLINK_TNEL_FSCHECKPOINTSTREAMFACTORY_H
