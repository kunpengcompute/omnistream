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

#ifndef OMNISTREAM_TASKLOCALSTATESTORE_H
#define OMNISTREAM_TASKLOCALSTATESTORE_H

#include <map>
#include <mutex>
#include <utility>
#include <set>
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/jobgraph/JobVertexID.h"
#include "runtime/checkpoint/TaskStateSnapshot.h"
#include "runtime/checkpoint/TaskStateSnapshotDeserializer.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"
#include "LocalRecoveryConfig.h"
#include "LocalRecoveryDirectoryProviderImpl.h"

namespace fs = std::filesystem;

class TaskLocalStateStore {
public:
    TaskLocalStateStore() = default;
    TaskLocalStateStore(
        omnistream::JobIDPOD jobID,
        // AllocationID allocationID,
        omnistream::JobVertexID jobVertexId,
        int subtaskIndex,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig
        // Executor discardExecutor
    );
    CompletableFutureV2<void>* dispose();

    void storeLocalState(long checkpointId, std::shared_ptr<TaskStateSnapshot> localState);
    std::shared_ptr<TaskStateSnapshot> retrieveLocalState(long checkpointID);
    void pruneMatchingCheckpoints(std::function<bool(long)> matcher);
    void confirmCheckpoint(long confirmedCheckpointId);
    void abortCheckpoint(long abortedCheckpointId);

    static std::shared_ptr<TaskStateSnapshot> nullDummy();
    std::shared_ptr<LocalRecoveryDirectoryProvider> getLocalRecoveryDirectoryProvider()
    {
        auto provider = localRecoveryConfig_->GetLocalStateDirectoryProvider();
        if (provider == nullptr) {
            throw std::invalid_argument("Local recovery must be enabled.");
        }
        return provider;
    }

    const std::string taskStateSnapshotFilename = "_task_state_snapshot";

    std::shared_ptr<LocalRecoveryConfig> getLocalRecoveryConfig()
    {
        return localRecoveryConfig_;
    }

    void setLocalRecoveryConfig(std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig)
    {
        localRecoveryConfig_ = std::move(localRecoveryConfig);
    }

protected:
    void pruneCheckpoints(std::function<bool(long)> matcher, bool breakOnceCheckerFalse);

    std::map<long, std::shared_ptr<TaskStateSnapshot>> storedTaskStateByCheckpointID;

private:
    void persistLocalStateMetadata(long checkpointId, std::shared_ptr<TaskStateSnapshot> localState);

    void asyncDiscardLocalStateForCollection(std::set<std::pair<long, std::shared_ptr<TaskStateSnapshot>>> toDiscard);
    void syncDiscardLocalStateForCollection(std::set<std::pair<long, std::shared_ptr<TaskStateSnapshot>>> toDiscard);
    void discardLocalStateForCheckpoint(long checkpointID, std::shared_ptr<TaskStateSnapshot> o);
    std::shared_ptr<TaskStateSnapshot> loadTaskStateSnapshot(long checkpointID);
    std::shared_ptr<TaskStateSnapshot> tryLoadTaskStateSnapshotFromDisk(long checkpointID);

    std::filesystem::path getTaskStateSnapshotFile(long checkpointId)
    {
        return getCheckpointDirectory(checkpointId) / taskStateSnapshotFilename;
    }
    std::filesystem::path getCheckpointDirectory(long checkpointId)
    {
        return getLocalRecoveryDirectoryProvider()->SubtaskSpecificCheckpointDirectory(checkpointId);
    }
    void createFolderOrFail(const std::filesystem::path& checkpointDirectory)
    {
        if (!std::filesystem::exists(checkpointDirectory) &&
            !std::filesystem::create_directories(checkpointDirectory)) {
            throw std::runtime_error(
                std::string("Could not create the checkpoint directory '") + checkpointDirectory.string() + "'");
        }
    }

    omnistream::JobIDPOD jobID_;
    // AllocationID allocationID_;
    omnistream::JobVertexID jobVertexId_;
    int subtaskIndex_;
    bool disposed_;

    std::mutex mutexLock;
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig_;
};

#endif // OMNISTREAM_TASKLOCALSTATESTORE_H
