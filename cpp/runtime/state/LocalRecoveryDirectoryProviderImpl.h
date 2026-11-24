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

#ifndef OMNISTREAM_LOCALRECOVERYDIRECTORYPROVIDERIMPL_H
#define OMNISTREAM_LOCALRECOVERYDIRECTORYPROVIDERIMPL_H

#include "LocalRecoveryDirectoryProvider.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/jobgraph/JobVertexID.h"
#include <vector>
#include <string>
#include <climits>
#include <filesystem>

class LocalRecoveryDirectoryProviderImpl : public LocalRecoveryDirectoryProvider {
public:
    LocalRecoveryDirectoryProviderImpl(
        const std::vector<std::filesystem::path>& allocationBaseDirs,
        const omnistream::JobIDPOD& jobID,
        const omnistream::JobVertexID& jobVertexID,
        int subtaskIndex)
        : allocationBaseDirs_(allocationBaseDirs),
          jobID_(jobID),
          jobVertexID_(jobVertexID),
          subtaskIndex_(subtaskIndex)
    {
        if (allocationBaseDirs_.empty()) {
            throw std::invalid_argument("At least one base directory required");
        }

        for (const auto& dir : allocationBaseDirs_) {
            std::filesystem::create_directories(dir);
        }
    }

    std::filesystem::path AllocationBaseDirectory(long checkpointId) override
    {
        uint32_t narrowed = static_cast<uint32_t>(static_cast<int32_t>(checkpointId));
        uint32_t masked = narrowed & static_cast<uint32_t>(INT32_MAX);
        size_t index = masked % allocationBaseDirs_.size();

        return SelectAllocationBaseDirectory(index);
    }

    std::filesystem::path SubtaskBaseDirectory(long checkpointId) override
    {
        return AllocationBaseDirectory(checkpointId) / SubtaskDirString();
    }

    std::filesystem::path SubtaskSpecificCheckpointDirectory(long checkpointId) override
    {
        return SubtaskBaseDirectory(checkpointId) / CheckpointDirString(checkpointId);
    }

    std::filesystem::path SelectAllocationBaseDirectory(int idx) override
    {
        if (idx < 0 || idx >= static_cast<int>(allocationBaseDirs_.size())) {
            throw std::out_of_range("Invalid directory index");
        }
        return allocationBaseDirs_[idx];
    }

    std::filesystem::path SelectSubtaskBaseDirectory(int idx) override
    {
        return SelectAllocationBaseDirectory(idx) / SubtaskDirString();
    }

    int AllocationBaseDirsCount() const override
    {
        return static_cast<int>(allocationBaseDirs_.size());
    }

    std::string ToString() const override
    {
        std::string result = "LocalRecoveryDirectoryProvider{AllocationBaseDirectories=[";
        for (size_t i = 0; i < allocationBaseDirs_.size(); ++i) {
            if (i > 0) result += ", ";
            result += allocationBaseDirs_[i].string();
        }
        result += "], JobID=" + jobID_.toString() + ", JobVertexID=" + jobVertexID_.toString() +
                  ", SubtaskIndex=" + std::to_string(subtaskIndex_) + "}";
        return result;
    }

    std::vector<std::filesystem::path> GetPaths() const override
    {
        return allocationBaseDirs_;
    }

    int GetSubIndex() const override
    {
        return subtaskIndex_;
    }

    void SetJobIdHexStr(std::string hexStr) override
    {
        jobIdHexStr = hexStr;
    }
    std::string GetJobIdHexStr() override
    {
        return jobIdHexStr;
    }
    void SetVertexIdHexStr(std::string hexStr) override
    {
        vertexIdHexStr = hexStr;
    }
    std::string GetVertexIdHexStr() override
    {
        return vertexIdHexStr;
    }

private:
    std::string SubtaskDirString() const
    {
        return "jid_" + jobID_.AbstractIDPOD::toString() + "/vtx_" + jobVertexID_.AbstractIDPOD::toString() + "_sti_" +
               std::to_string(subtaskIndex_);
    }

    std::string CheckpointDirString(long checkpointId) const
    {
        return "chk_" + std::to_string(checkpointId);
    }

    std::vector<std::filesystem::path> allocationBaseDirs_;
    omnistream::JobIDPOD jobID_;
    omnistream::JobVertexID jobVertexID_;
    int subtaskIndex_;
    std::string jobIdHexStr;
    std::string vertexIdHexStr;
};

#endif // OMNISTREAM_LOCALRECOVERYDIRECTORYPROVIDERIMPL_H
