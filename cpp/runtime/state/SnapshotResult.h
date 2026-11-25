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

#ifndef FLINK_TNEL_SNAPSHOTRESULT_H
#define FLINK_TNEL_SNAPSHOTRESULT_H

#include <iostream>
#include <memory>
#include "StateObject.h"

/**
 * This class contains the combined results from the snapshot of a state backend:
 *
 * - A state object representing the state that will be reported to the Job Manager
 *   to acknowledge the checkpoint.
 * - A state object that represents the state for the TaskLocalStateStoreImpl.
 *
 * Both state objects are optional and can be nullptr, for example if there was no state
 * to snapshot in the backend. If the local state object is not nullptr, the job manager
 * state must also not be nullptr, because the Job Manager always owns the ground truth
 * about the checkpointed state.
 */
template <typename T>
class SnapshotResult : public StateObject {
    static_assert(std::is_base_of<StateObject, T>::value, "T must inherit from StateObject");

public:
    const std::shared_ptr<T>& GetJobManagerOwnedSnapshot() const
    {
        return jobManagerOwnedSnapshot;
    }

    const std::shared_ptr<T>& GetTaskLocalSnapshot() const
    {
        return taskLocalSnapshot;
    }

    void DiscardState() override
    {
        std::exception_ptr aggregatedExceptions = nullptr;

        if (jobManagerOwnedSnapshot != nullptr) {
            try {
                jobManagerOwnedSnapshot->DiscardState();
            } catch (...) {
                aggregatedExceptions = std::current_exception();
            }
        }

        if (taskLocalSnapshot != nullptr) {
            try {
                taskLocalSnapshot->DiscardState();
            } catch (...) {
                if (aggregatedExceptions) {
                    // Another approach is to keep a collection of suppressed exceptions and present later.
                    std::cerr << "Suppressed exception during DiscardState()" << std::endl;
                } else {
                    aggregatedExceptions = std::current_exception();
                }
            }
        }

        if (aggregatedExceptions) {
            std::rethrow_exception(aggregatedExceptions);
        }
    }

    long GetStateSize() const override
    {
        return jobManagerOwnedSnapshot != nullptr ? jobManagerOwnedSnapshot->GetStateSize() : 0L;
    }

    static std::shared_ptr<SnapshotResult<T>> Empty()
    {
        return EMPTY;
    }

    static std::shared_ptr<SnapshotResult<T>> Of(std::shared_ptr<T> jobManagerState)
    {
        return (jobManagerState != nullptr) ? std::make_shared<SnapshotResult<T>>(jobManagerState, nullptr) : Empty();
    }

    static std::shared_ptr<SnapshotResult<T>> WithLocalState(
        std::shared_ptr<T> jobManagerState, std::shared_ptr<T> localState)
    {
        return std::make_shared<SnapshotResult<T>>(jobManagerState, localState);
    }

    /**
     * Creates a SnapshotResult for the given jobManagerOwnedSnapshot and taskLocalSnapshot.
     * If jobManagerOwnedSnapshot is null, then taskLocalSnapshot must also be null.
     * @param jobManagerOwnedSnapshot Snapshot to report to the job manager. Can be nullptr.
     * @param taskLocalSnapshot Optional snapshot to report to the local state manager.
     *        If provided, jobManagerOwnedSnapshot must not be nullptr.
     */
    SnapshotResult(std::shared_ptr<T> jobManagerOwnedSnapshot, std::shared_ptr<T> taskLocalSnapshot)
        : jobManagerOwnedSnapshot(jobManagerOwnedSnapshot),
          taskLocalSnapshot(taskLocalSnapshot)
    {
        if (jobManagerOwnedSnapshot == nullptr && taskLocalSnapshot != nullptr) {
            throw std::logic_error("Cannot report local state snapshot without corresponding remote state!");
        }
    }

    std::string ToString() const override
    {
        return "ToString returns empty in SnapshotResult";
    }

    /** A singleton instance to represent an empty snapshot result. */
    static const std::shared_ptr<SnapshotResult<T>> EMPTY;

private:
    /** This is the state snapshot that will be reported to the Job Manager to acknowledge a checkpoint. */
    const std::shared_ptr<T> jobManagerOwnedSnapshot;

    /** This is the state snapshot that will be reported to the Job Manager to acknowledge a checkpoint. */
    const std::shared_ptr<T> taskLocalSnapshot;
};

template <typename T>
const std::shared_ptr<SnapshotResult<T>> SnapshotResult<T>::EMPTY =
    std::make_shared<SnapshotResult<T>>(nullptr, nullptr);
#endif // FLINK_TNEL_SNAPSHOTRESULT_H
