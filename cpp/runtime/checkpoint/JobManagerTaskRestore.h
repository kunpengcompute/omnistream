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

#ifndef OMNISTREAM_JOBMANAGERTASKRESTORE_H
#define OMNISTREAM_JOBMANAGERTASKRESTORE_H

#include "runtime/checkpoint/TaskStateSnapshot.h"
namespace omnistream {
class JobManagerTaskRestore {
public:
    JobManagerTaskRestore(long long restoreCheckpointId, std::shared_ptr<TaskStateSnapshot> taskStateSnapshot)
        : restoreCheckpointId(restoreCheckpointId),
          taskStateSnapshot(taskStateSnapshot)
    {
    }

    long getRestoreCheckpointId() const
    {
        return restoreCheckpointId;
    }

    const std::shared_ptr<TaskStateSnapshot> getTaskStateSnapshot() const
    {
        return taskStateSnapshot;
    }

    std::string toString() const
    {
        std::ostringstream oss;
        oss << "JobManagerTaskRestore{"
            << "restoreCheckpointId=" << restoreCheckpointId << ", taskStateSnapshot=" << taskStateSnapshot->ToString()
            << '}';
        return oss.str();
    }

private:
    static const long serialVersionUID = 1L;

    /** The id of the checkpoint from which we restore. */
    long restoreCheckpointId;

    /** The state for this task to restore. */
    const std::shared_ptr<TaskStateSnapshot> taskStateSnapshot;
};

} // namespace omnistream
#endif // OMNISTREAM_JOBMANAGERTASKRESTORE_H
