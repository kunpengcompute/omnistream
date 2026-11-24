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

#ifndef OMNISTREAM_LOCALRECOVERYDIRECTORYPROVIDER_H
#define OMNISTREAM_LOCALRECOVERYDIRECTORYPROVIDER_H

#include <string>
#include <filesystem>
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/jobgraph/JobVertexID.h"

class LocalRecoveryDirectoryProvider {
public:
    virtual ~LocalRecoveryDirectoryProvider() = default;

    virtual std::filesystem::path AllocationBaseDirectory(long checkpointId) = 0;
    virtual std::filesystem::path SubtaskBaseDirectory(long checkpointId) = 0;
    virtual std::filesystem::path SubtaskSpecificCheckpointDirectory(long checkpointId) = 0;

    virtual std::filesystem::path SelectAllocationBaseDirectory(int idx) = 0;
    virtual std::filesystem::path SelectSubtaskBaseDirectory(int idx) = 0;

    virtual int AllocationBaseDirsCount() const = 0;

    virtual std::string ToString() const = 0;

    virtual std::vector<std::filesystem::path> GetPaths() const = 0;
    virtual int GetSubIndex() const = 0;
    virtual void SetJobIdHexStr(std::string hexStr) = 0;
    virtual std::string GetJobIdHexStr() = 0;
    virtual void SetVertexIdHexStr(std::string hexStr) = 0;
    virtual std::string GetVertexIdHexStr() = 0;
};

#endif // OMNISTREAM_LOCALRECOVERYDIRECTORYPROVIDER_H
