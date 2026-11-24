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

#ifndef TASK_TRACKER_H
#define TASK_TRACKER_H

#include <map>
#include <set>

/**
 * Track upstream tasks to determine if all upstream data for a given checkpoint has been received.
 * When all tasks have reported for a checkpoint, the checkpoint is ready for commit.
 */
class TaskTracker {
public:
    explicit TaskTracker(int numberOfTasks) : numberOfTasks_(numberOfTasks)
    {
    }

    /**
     * @return true if all tasks for this checkpointId have reported and this checkpoint is ready for commit
     */
    bool add(long checkpointId, int taskId)
    {
        auto& taskSet = notifiedTasks_[checkpointId];
        taskSet.insert(taskId);

        if (static_cast<int>(taskSet.size()) == numberOfTasks_) {
            auto end = notifiedTasks_.upper_bound(checkpointId);
            notifiedTasks_.erase(notifiedTasks_.begin(), end);
            return true;
        }
        return false;
    }

private:
    int numberOfTasks_;
    std::map<long, std::set<int>> notifiedTasks_;
};

#endif // TASK_TRACKER_H
