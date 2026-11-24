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

#ifndef OMNISTREAM_TASKSTATESNAPSHOT_H
#define OMNISTREAM_TASKSTATESNAPSHOT_H
#include <unordered_map>
#include <map>
#include <set>
#include <memory>
#include <functional>
#include <string>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <iterator>
#include <vector>
#include "OperatorSubtaskState.h"
#include "InflightDataRescalingDescriptor.h"
#include "runtime/jobgraph/OperatorID.h"
class TaskStateSnapshot : public CompositeStateHandle {
public:
    static const std::shared_ptr<TaskStateSnapshot> finishedOnRestore;
    static constexpr int INITIAL_SIZE = 10;
    TaskStateSnapshot() : TaskStateSnapshot(INITIAL_SIZE, false)
    {
    }
    explicit TaskStateSnapshot(
        const std::unordered_map<OperatorID, std::shared_ptr<OperatorSubtaskState>>& subtaskStatesByOperatorID,
        bool isTaskDeployedAsFinished,
        bool isTaskFinished)
        : subtaskStatesByOperatorID(subtaskStatesByOperatorID),
          isTaskDeployedAsFinished(isTaskDeployedAsFinished),
          isTaskFinished(isTaskFinished)
    {
        // Note: Preconditions.checkNotNull equivalent handled in constructor initialization
    }
    explicit TaskStateSnapshot(const TaskStateSnapshot* other)
    {
    }

    TaskStateSnapshot(int size, bool isTaskFinished)
        : TaskStateSnapshot(
              std::unordered_map<OperatorID, std::shared_ptr<OperatorSubtaskState>>(), false, isTaskFinished)
    {
        subtaskStatesByOperatorID.reserve(size);
    }

    explicit TaskStateSnapshot(
        const std::unordered_map<OperatorID, std::shared_ptr<OperatorSubtaskState>>& subtaskStatesByOperatorID)
        : TaskStateSnapshot(subtaskStatesByOperatorID, false, false)
    {
    }

    /** Returns whether all the operators of the task are already finished on restoring. */
    bool GetIsTaskDeployedAsFinished() const
    {
        return isTaskDeployedAsFinished;
    }

    /** Returns whether all the operators of the task have called finished methods. */
    bool GetIsTaskFinished() const
    {
        return isTaskFinished;
    }

    /** Returns the subtask state for the given operator id (or null if not contained). */
    std::shared_ptr<OperatorSubtaskState> GetSubtaskStateByOperatorID(const OperatorID& operatorID) const;

    /**
     * Maps the given operator id to the given subtask state. Returns the subtask state of a
     * previous mapping, if such a mapping existed or null otherwise.
     */
    std::shared_ptr<OperatorSubtaskState> PutSubtaskStateByOperatorID(
        const OperatorID& operatorID, std::shared_ptr<OperatorSubtaskState> state);

    /** Returns the set of all mappings from operator id to the corresponding subtask state. */
    std::set<std::pair<OperatorID, std::shared_ptr<OperatorSubtaskState>>> GetSubtaskStateMappings() const;

    /**
     * Returns true if at least one {@link OperatorSubtaskState} in subtaskStatesByOperatorID has
     * state.
     */
    bool HasState() const;

    /**
     * Returns the input channel mapping for rescaling with in-flight data or {@link
     * InflightDataRescalingDescriptor#noRescale}.
     */
    std::shared_ptr<InflightDataRescalingDescriptor> GetInputRescalingDescriptor() const;

    /**
     * Returns the output channel mapping for rescaling with in-flight data or {@link
     * InflightDataRescalingDescriptor#noRescale}.
     */
    std::shared_ptr<InflightDataRescalingDescriptor> GetOutputRescalingDescriptor() const;

    void DiscardState() override;

    long GetStateSize() const override;

    void RegisterSharedStates(SharedStateRegistry& stateRegistry, long checkpointID) override;

    long GetCheckpointedSize() override;

    void SetIsTaskFinished(bool hasTaskFinished);

    void SetIsTaskDeployedAsFinished(bool hasTaskDeployedAsFinished);

    std::unordered_map<OperatorID, std::shared_ptr<OperatorSubtaskState>> GetSubtaskStatesByOperatorID()
    {
        return subtaskStatesByOperatorID;
    }

    bool operator==(const TaskStateSnapshot& other) const
    {
        if (this == &other) {
            return true;
        }

        return subtaskStatesByOperatorID == other.subtaskStatesByOperatorID &&
               isTaskDeployedAsFinished == other.isTaskDeployedAsFinished && isTaskFinished == other.isTaskFinished;
    }

    bool operator!=(const TaskStateSnapshot& other) const
    {
        return !(*this == other);
    }

    size_t HashCode() const;
    std::string ToString() const;

private:
    static const long serialVersionUID = 1L;
    /** Mapping from an operator id to the state of one subtask of this operator. */
    std::unordered_map<OperatorID, std::shared_ptr<OperatorSubtaskState>> subtaskStatesByOperatorID;

    bool isTaskDeployedAsFinished = false;

    bool isTaskFinished = false;

    /** Returns the only valid mapping as ensured by {@link StateAssignmentOperation}. */
    std::shared_ptr<InflightDataRescalingDescriptor> GetMapping(
        std::function<std::shared_ptr<InflightDataRescalingDescriptor>(const std::shared_ptr<OperatorSubtaskState>&)>
            mappingExtractor) const;
};

#endif // OMNISTREAM_TASKSTATESNAPSHOT_H
