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

#ifndef OMNISTREAM_CHECKPOINTCONFIGPOD_H
#define OMNISTREAM_CHECKPOINTCONFIGPOD_H

#include <string>
#include <sstream>
#include <cstdint>
#include <nlohmann/json.hpp>

namespace omnistream {

class CheckpointConfigPOD {
public:
    CheckpointConfigPOD() = default;

    CheckpointConfigPOD(
        std::string checkpointsDirectory,
        std::string stateBackend,
        std::string checkpointStorage,
        int maxRetainedCheckpoint,
        bool asyncSnapshots,
        bool incrementalCheckpoints,
        bool localRecovery,
        std::string localRecoveryTaskManagerStateRootDirs,
        std::string savepointDirectory,
        std::int64_t fsSmallFileThreshold,
        int fsWriteBufferSize)
        : checkpointsDirectory(std::move(checkpointsDirectory)),
          stateBackend(std::move(stateBackend)),
          checkpointStorage(std::move(checkpointStorage)),
          maxRetainedCheckpoint(maxRetainedCheckpoint),
          asyncSnapshots(asyncSnapshots),
          incrementalCheckpoints(incrementalCheckpoints),
          localRecovery(localRecovery),
          localRecoveryTaskManagerStateRootDirs(std::move(localRecoveryTaskManagerStateRootDirs)),
          savepointDirectory(std::move(savepointDirectory)),
          fsSmallFileThreshold(fsSmallFileThreshold),
          fsWriteBufferSize(fsWriteBufferSize)
    {
    }

    std::string getCheckpointsDirectory() const
    {
        return checkpointsDirectory;
    }

    void setCheckpointsDirectory(const std::string& input)
    {
        checkpointsDirectory = input;
    }

    std::string getStateBackend() const
    {
        return stateBackend;
    }

    void setStateBackend(const std::string& input)
    {
        stateBackend = input;
    }

    std::string getCheckpointStorage() const
    {
        return checkpointStorage;
    }

    void setCheckpointStorage(const std::string& input)
    {
        checkpointStorage = input;
    }

    int getMaxRetainedCheckpoint() const
    {
        return maxRetainedCheckpoint;
    }

    void setMaxRetainedCheckpoint(int input)
    {
        maxRetainedCheckpoint = input;
    }

    bool getAsyncSnapshots() const
    {
        return asyncSnapshots;
    }

    void setAsyncSnapshots(bool input)
    {
        asyncSnapshots = input;
    }

    bool getIncrementalCheckpoints() const
    {
        return incrementalCheckpoints;
    }

    void setIncrementalCheckpoints(bool input)
    {
        incrementalCheckpoints = input;
    }

    bool getLocalRecovery() const
    {
        return localRecovery;
    }

    void setLocalRecovery(bool input)
    {
        localRecovery = input;
    }

    std::string getLocalRecoveryTaskManagerStateRootDirs() const
    {
        return localRecoveryTaskManagerStateRootDirs;
    }

    void setLocalRecoveryTaskManagerStateRootDirs(const std::string& input)
    {
        localRecoveryTaskManagerStateRootDirs = input;
    }

    std::string getSavepointDirectory() const
    {
        return savepointDirectory;
    }

    void setSavepointDirectory(const std::string& input)
    {
        savepointDirectory = input;
    }

    std::int64_t getFsSmallFileThreshold() const
    {
        return fsSmallFileThreshold;
    }

    void setFsSmallFileThreshold(std::int64_t input)
    {
        fsSmallFileThreshold = input;
    }

    int getFsWriteBufferSize() const
    {
        return fsWriteBufferSize;
    }

    void setFsWriteBufferSize(int input)
    {
        fsWriteBufferSize = input;
    }

    int getRocksdbCheckpointTransferThreadNum() const
    {
        return 4;
    }

    std::string toString() const
    {
        std::ostringstream oss;
        oss << "CheckpointConfigPOD{"
            << "checkpointsDirectory=" << checkpointsDirectory << ", stateBackend=" << stateBackend
            << ", checkpointStorage=" << checkpointStorage << ", maxRetainedCheckpoint=" << maxRetainedCheckpoint
            << ", asyncSnapshots=" << asyncSnapshots << ", incrementalCheckpoints=" << incrementalCheckpoints
            << ", localRecovery=" << localRecovery
            << ", localRecoveryTaskManagerStateRootDirs=" << localRecoveryTaskManagerStateRootDirs
            << ", savepointDirectory=" << savepointDirectory << ", fsSmallFileThreshold=" << fsSmallFileThreshold
            << ", fsWriteBufferSize=" << fsWriteBufferSize << '}';
        return oss.str();
    }

    bool operator==(const CheckpointConfigPOD& other) const
    {
        return checkpointsDirectory == other.checkpointsDirectory && stateBackend == other.stateBackend &&
               checkpointStorage == other.checkpointStorage && maxRetainedCheckpoint == other.maxRetainedCheckpoint &&
               asyncSnapshots == other.asyncSnapshots && incrementalCheckpoints == other.incrementalCheckpoints &&
               localRecovery == other.localRecovery &&
               localRecoveryTaskManagerStateRootDirs == other.localRecoveryTaskManagerStateRootDirs &&
               savepointDirectory == other.savepointDirectory && fsSmallFileThreshold == other.fsSmallFileThreshold &&
               fsWriteBufferSize == other.fsWriteBufferSize;
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        CheckpointConfigPOD,
        checkpointsDirectory,
        stateBackend,
        checkpointStorage,
        maxRetainedCheckpoint,
        asyncSnapshots,
        incrementalCheckpoints,
        localRecovery,
        localRecoveryTaskManagerStateRootDirs,
        savepointDirectory,
        fsSmallFileThreshold,
        fsWriteBufferSize)

private:
    std::string checkpointsDirectory;
    std::string stateBackend;
    std::string checkpointStorage;
    int maxRetainedCheckpoint = 0;
    bool asyncSnapshots = false;
    bool incrementalCheckpoints = false;
    bool localRecovery = false;
    std::string localRecoveryTaskManagerStateRootDirs;
    std::string savepointDirectory;
    std::int64_t fsSmallFileThreshold = 0;
    int fsWriteBufferSize = 0;
};

} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::CheckpointConfigPOD> {
    size_t operator()(const omnistream::CheckpointConfigPOD& obj) const
    {
        size_t h1 = std::hash<std::string>()(obj.getCheckpointsDirectory());
        size_t h2 = std::hash<std::string>()(obj.getStateBackend());
        size_t h3 = std::hash<std::string>()(obj.getCheckpointStorage());
        size_t h4 = std::hash<int>()(obj.getMaxRetainedCheckpoint());
        size_t h5 = std::hash<bool>()(obj.getAsyncSnapshots());
        size_t h6 = std::hash<bool>()(obj.getIncrementalCheckpoints());
        size_t h7 = std::hash<bool>()(obj.getLocalRecovery());
        size_t h8 = std::hash<std::string>()(obj.getLocalRecoveryTaskManagerStateRootDirs());
        size_t h9 = std::hash<std::string>()(obj.getSavepointDirectory());
        size_t h10 = std::hash<std::int64_t>()(obj.getFsSmallFileThreshold());
        size_t h11 = std::hash<int>()(obj.getFsWriteBufferSize());

        size_t seed = 0;
        const size_t C = 0x85ebca6b;

        seed ^= (h1 + C + (seed << 6) + (seed >> 2));
        seed ^= (h2 + C + (seed << 6) + (seed >> 2));
        seed ^= (h3 + C + (seed << 6) + (seed >> 2));
        seed ^= (h4 + C + (seed << 6) + (seed >> 2));
        seed ^= (h5 + C + (seed << 6) + (seed >> 2));
        seed ^= (h6 + C + (seed << 6) + (seed >> 2));
        seed ^= (h7 + C + (seed << 6) + (seed >> 2));
        seed ^= (h8 + C + (seed << 6) + (seed >> 2));
        seed ^= (h9 + C + (seed << 6) + (seed >> 2));
        seed ^= (h10 + C + (seed << 6) + (seed >> 2));
        seed ^= (h11 + C + (seed << 6) + (seed >> 2));

        return seed;
    }
};
} // namespace std

#endif // OMNISTREAM_CHECKPOINTCONFIGPOD_H
