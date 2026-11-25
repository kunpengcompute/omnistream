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

#include <fstream>
#include "TaskLocalStateStore.h"

TaskLocalStateStore::TaskLocalStateStore(
    omnistream::JobIDPOD jobID,
    omnistream::JobVertexID jobVertexId,
    int subtaskIndex,
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig)
    : jobID_(jobID),
      jobVertexId_(jobVertexId),
      subtaskIndex_(subtaskIndex),
      disposed_(false),
      localRecoveryConfig_(localRecoveryConfig)
{
}

void TaskLocalStateStore::storeLocalState(long checkpointId, std::shared_ptr<TaskStateSnapshot> localState)
{
    std::shared_ptr<TaskStateSnapshot> stateToStore;
    if (localState == nullptr) {
        stateToStore = nullDummy();
    } else {
        stateToStore = localState;
    }
    LOG_TRACE("Stored local state for checkpoint " << checkpointId);
    std::pair<long, std::shared_ptr<TaskStateSnapshot>> toDiscard;
    {
        std::lock_guard<std::mutex> lockGuard(mutexLock);
        if (disposed_) {
            // we ignore late stores and simply discard the state.
            toDiscard = std::make_pair(checkpointId, stateToStore);
        } else {
            auto it = storedTaskStateByCheckpointID.find(checkpointId);
            std::shared_ptr<TaskStateSnapshot> previous;
            if (it != storedTaskStateByCheckpointID.end()) {
                previous = it->second;
            }
            storedTaskStateByCheckpointID[checkpointId] = stateToStore;
            persistLocalStateMetadata(checkpointId, stateToStore);

            if (previous != nullptr) {
                toDiscard = std::make_pair(checkpointId, previous);
            }
        }
    }

    if (toDiscard.second != nullptr) {
        std::set<std::pair<long, std::shared_ptr<TaskStateSnapshot>>> discardList = {toDiscard};
        asyncDiscardLocalStateForCollection(discardList);
    }
}

std::shared_ptr<TaskStateSnapshot> TaskLocalStateStore::retrieveLocalState(long checkpointID)
{
    if (!localRecoveryConfig_->IsLocalRecoveryEnabled()) {
        return nullptr;
    }

    std::shared_ptr<TaskStateSnapshot> snapshot;

    {
        std::lock_guard<std::mutex> lock(mutexLock);
        snapshot = loadTaskStateSnapshot(checkpointID);
    }

    return (snapshot != nullDummy()) ? snapshot : nullptr;
}

void TaskLocalStateStore::pruneMatchingCheckpoints(std::function<bool(long)> matcher)
{
    pruneCheckpoints(matcher, false);
}

void TaskLocalStateStore::confirmCheckpoint(long confirmedCheckpointId)
{
    pruneCheckpoints(
        [confirmedCheckpointId](long snapshotCheckpointId) { return snapshotCheckpointId < confirmedCheckpointId; },
        true);
}

void TaskLocalStateStore::abortCheckpoint(long abortedCheckpointId)
{
    pruneCheckpoints(
        [abortedCheckpointId](long snapshotCheckpointId) { return snapshotCheckpointId == abortedCheckpointId; },
        false);
}

std::shared_ptr<TaskStateSnapshot> TaskLocalStateStore::nullDummy()
{
    return std::make_shared<TaskStateSnapshot>(0, false);
}

void TaskLocalStateStore::pruneCheckpoints(std::function<bool(long)> matcher, bool breakOnceCheckerFalse)
{
    std::set<std::pair<long, std::shared_ptr<TaskStateSnapshot>>> toRemove;

    {
        std::lock_guard<std::mutex> lock(mutexLock);

        auto it = storedTaskStateByCheckpointID.begin();

        while (it != storedTaskStateByCheckpointID.end()) {
            long entryCheckpointId = it->first;

            if (matcher(entryCheckpointId)) {
                // We have to make a copy of this shared_ptr, otherwise it will be released when erase(it)
                std::shared_ptr<TaskStateSnapshot> keepAlive = it->second;
                toRemove.emplace(std::make_pair(entryCheckpointId, keepAlive));
                it = storedTaskStateByCheckpointID.erase(it);
            } else if (breakOnceCheckerFalse) {
                break;
            } else {
                it++;
            }
        }
    }
    asyncDiscardLocalStateForCollection(toRemove);
}

void TaskLocalStateStore::asyncDiscardLocalStateForCollection(
    std::set<std::pair<long, std::shared_ptr<TaskStateSnapshot>>> toDiscard)
{
    if (!toDiscard.empty()) {
        // Might need async operation here
        syncDiscardLocalStateForCollection(toDiscard);
    }
}

void TaskLocalStateStore::syncDiscardLocalStateForCollection(
    std::set<std::pair<long, std::shared_ptr<TaskStateSnapshot>>> toDiscard)
{
    for (auto entry : toDiscard) {
        discardLocalStateForCheckpoint(entry.first, entry.second);
    }
}

void TaskLocalStateStore::discardLocalStateForCheckpoint(
    long checkpointID, std::shared_ptr<TaskStateSnapshot> taskStateSnapshot)
{
    LOG("Discarding local task state snapshot of checkpoint " << checkpointID);
    if (taskStateSnapshot != nullptr) {
        try {
            taskStateSnapshot->DiscardState();
        } catch (const std::exception& discardEx) {
            throw std::runtime_error(
                "Exception while discarding local task state snapshot of checkpoint " + std::to_string(checkpointID));
        }
    }

    std::filesystem::path checkpointDir = getCheckpointDirectory(checkpointID);

    try {
        std::filesystem::remove_all(checkpointDir);
    } catch (const std::exception& ex) {
        LOG("Exception while deleting local state directory of checkpoint in subtask." << checkpointID);
    }
}

void TaskLocalStateStore::persistLocalStateMetadata(long checkpointId, std::shared_ptr<TaskStateSnapshot> localState)
{
    createFolderOrFail(getCheckpointDirectory(checkpointId));
    std::filesystem::path taskStateSnapshotFile = getTaskStateSnapshotFile(checkpointId);

    try {
        std::ofstream ofs(taskStateSnapshotFile, std::ios::binary);
        if (!ofs.is_open()) {
            throw std::runtime_error("Could not open file for writing: " + taskStateSnapshotFile.string());
        }

        // Serialize the TaskStateSnapshot object
        ofs << TaskStateSnapshotSerializer::Serialize(localState);

        LOG_PRINTF(
            "Successfully written local task state snapshot file %s for checkpoint %ld.",
            taskStateSnapshotFile.string().c_str(),
            checkpointId);
    } catch (const std::exception& e) {
        throw std::runtime_error("Could not write the local task state snapshot file.");
    }
}

std::shared_ptr<TaskStateSnapshot> TaskLocalStateStore::loadTaskStateSnapshot(long checkpointID)
{
    // C++ equivalent of computeIfAbsent
    auto it = storedTaskStateByCheckpointID.find(checkpointID);
    if (it != storedTaskStateByCheckpointID.end()) {
        return it->second;
    } else {
        auto newSnapshot = tryLoadTaskStateSnapshotFromDisk(checkpointID);
        storedTaskStateByCheckpointID[checkpointID] = newSnapshot;
        return newSnapshot;
    }
}

std::shared_ptr<TaskStateSnapshot> TaskLocalStateStore::tryLoadTaskStateSnapshotFromDisk(long checkpointID)
{
    const std::filesystem::path taskStateSnapshotFile = getTaskStateSnapshotFile(checkpointID);
    if (std::filesystem::exists(taskStateSnapshotFile)) {
        std::shared_ptr<TaskStateSnapshot> taskStateSnapshot = nullptr;
        try {
            std::ifstream ifs(taskStateSnapshotFile, std::ios::binary);
            if (ifs.is_open()) {
                // Assuming TaskStateSnapshot has a deserialize method or similar
                // Since Java uses ObjectInputStream, we need equivalent deserialization
                taskStateSnapshot = std::make_shared<TaskStateSnapshot>();
                // Read the object from file (implementation depends on serialization format)
                // This is a placeholder for the actual deserialization logic
                size_t dataSize;
                ifs.read(reinterpret_cast<char*>(&dataSize), sizeof(dataSize));
                std::vector<char> buffer(dataSize);
                ifs.read(buffer.data(), dataSize);

                // This part needs to be tested! What was written to the file?
                taskStateSnapshot =
                    TaskStateSnapshotDeserializer::Deserialize(std::string(buffer.data(), buffer.size()));
                ifs.close();
                LOG("Loaded task state snapshot for checkpoint {} successfully from disk." << checkpointID);
            }
        } catch (const std::exception& e) {
            LOG("Could not read task state snapshot file {} for checkpoint {}. Deleting the corresponding local state."
                << taskStateSnapshotFile.string() << checkpointID);
            discardLocalStateForCheckpoint(checkpointID, nullptr);
            // Clean up if allocation succeeded but deserialization failed
            if (taskStateSnapshot != nullptr) {
                taskStateSnapshot = nullptr;
            }
        }
        return taskStateSnapshot;
    }
    return nullptr;
}
