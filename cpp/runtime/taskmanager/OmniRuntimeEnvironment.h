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

#ifndef OMNIRUNTIMEENVIRONMENT_V2_H
#define OMNIRUNTIMEENVIRONMENT_V2_H

#include <memory>
#include <executiongraph/JobInformationPOD.h>
#include <executiongraph/TaskInformationPOD.h>
#include <executiongraph/common/TaskPlainInfoPOD.h>
#include <executiongraph/descriptor/TaskDeploymentDescriptorPOD.h>
#include <partition/ResultPartitionWriter.h>
#include <sstream>
#include <state/bridge/TaskStateManagerBridge.h>

#include "runtime/execution/OmniEnvironment.h"
#include "runtime/shuffle/ShuffleEnvironment.h"
#include "metrics/groups/TaskMetricGroup.h"
#include "runtime/jobgraph/tasks/TaskOperatorEventGateway.h"
#include "state/bridge/TaskOperatorEventGatewayBridge.h"
#include "state/bridge/OmniTaskBridge.h"

namespace omnistream {
class OmniTask;

class RuntimeEnvironmentV2 : public EnvironmentV2 {
public:
    ~RuntimeEnvironmentV2();
    RuntimeEnvironmentV2() = default;
    RuntimeEnvironmentV2(
        const std::shared_ptr<ShuffleEnvironment>& omniShuffleEnvironment,
        const TaskInformationPOD& taskConfiguration,
        const JobInformationPOD& jobConfiguration,
        const TaskPlainInfoPOD& taskPlainInfoPod,
        const ExecutionAttemptIDPOD& attemptIdpod,
        const std::vector<std::shared_ptr<ResultPartitionWriter>>& writers,
        const std::vector<std::shared_ptr<IndexedInputGate>>& inputGates,
        OmniTask* omniTask,
        std::shared_ptr<TaskMetricGroup>& taskMetricGroup,
        std::shared_ptr<TaskStateManagerBridge> taskStateManagerBridge,
        std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge,
        const TaskDeploymentDescriptorPOD& tdd)
        : omniShuffleEnvironment_(omniShuffleEnvironment),
          taskConfiguration_(taskConfiguration),
          jobConfiguration_(jobConfiguration),
          taskPlainInfoPod_(taskPlainInfoPod),
          writers_(writers),
          inputGates(inputGates),
          omniTask_(omniTask),
          taskMetricGroup_(taskMetricGroup),
          taskstatemanagerbridge_(taskStateManagerBridge),
          taskOperatorEventGatewayBridge_(taskOperatorEventGatewayBridge),
          omniTaskBridge_(omniTaskBridge)
    {
        JobIDPOD jobId = jobConfiguration.getJobId();
        auto stateStore = new TaskLocalStateStore(
            jobId, attemptIdpod.getJobVertexId(), attemptIdpod.getSubtaskIndex(), localRecoveryConfig);
        auto responder = new NoOpCheckpoingResponder();
        operatorEventGateway = std::make_shared<TaskOperatorEventGateway>(taskOperatorEventGatewayBridge_);
        std::string taskStateSnapshotString = tdd.getTaskStateSnapshot();
        std::shared_ptr<TaskStateSnapshot> taskStateSnapshot;
        long restoreCheckpointId = tdd.getRestoreCheckpointId();
        std::shared_ptr<JobManagerTaskRestore> jobManagerTaskRestore;

        if (!taskStateSnapshotString.empty()) {
            taskStateSnapshot = TaskStateSnapshotDeserializer::Deserialize(taskStateSnapshotString);
        }
        if (taskStateSnapshot) {
            jobManagerTaskRestore = std::make_shared<JobManagerTaskRestore>(restoreCheckpointId, taskStateSnapshot);
        }
        taskStateManager_ = std::make_shared<TaskStateManager>(
            jobId,
            attemptIdpod,
            stateStore,
            responder,
            taskstatemanagerbridge_,
            omniTaskBridge_,
            jobManagerTaskRestore);
    }

    [[nodiscard]] std::shared_ptr<TaskStateManager> getTaskStateManager() override
    {
        return taskStateManager_;
    }

    [[nodiscard]] std::shared_ptr<ShuffleEnvironment> omni_shuffle_environment() const
    {
        return omniShuffleEnvironment_;
    }

    std::shared_ptr<RocksDBSharedResources> getRocksDBSharedResources() const override
    {
        return rocksDBSharedResources_;
    }

    void setRocksDBSharedResources(std::shared_ptr<RocksDBSharedResources> resources) override
    {
        rocksDBSharedResources_ = std::move(resources);
    }

    const TaskInformationPOD& taskConfiguration() const override
    {
        return taskConfiguration_;
    }

    const JobInformationPOD& jobConfiguration() const override
    {
        return jobConfiguration_;
    }

    void setTaskConfiguration(TaskInformationPOD taskConfiguration)
    {
        taskConfiguration_ = taskConfiguration;
    }

    void SetTaskStateManager(std::shared_ptr<TaskStateManager> taskStateManager)
    {
        taskStateManager_ = std::move(taskStateManager);
    }

    [[nodiscard]] std::shared_ptr<TaskMetricGroup> taskMetricGroup() const override
    {
        return taskMetricGroup_;
    }
    void SetTaskMetricGroup(std::shared_ptr<TaskMetricGroup> group)
    {
        taskMetricGroup_ = std::move(group);
    }
    [[nodiscard]] TaskPlainInfoPOD taskPlainInfo() const
    {
        return taskPlainInfoPod_;
    }

    void SetInputGates(const std::vector<std::shared_ptr<IndexedInputGate>>& inputGates)
    {
        this->inputGates = inputGates;
    }

    [[nodiscard]] std::vector<std::shared_ptr<ResultPartitionWriter>> writers() const
    {
        return writers_;
    }

    [[nodiscard]] std::vector<std::shared_ptr<IndexedInputGate>> GetAllInputGates() const
    {
        return inputGates;
    }

    [[nodiscard]] OmniTask* omniTask() const
    {
        return omniTask_;
    }
    std::shared_ptr<TaskOperatorEventGateway> getOperatorCoordinatorEventGateway()
    {
        return operatorEventGateway;
    }

    // Visibility helper
    std::string toString() const
    {
        std::stringstream ss;
        ss << "RuntimeEnvironment {" << std::endl;

        ss << "  omniShuffleEnvironment_: ";
        if (omniShuffleEnvironment_) {
            ss << "shared_ptr (non-null)"
               << std::endl; // You might want to add more details about the ShuffleEnvironment if possible.
        } else {
            ss << "nullptr" << std::endl;
        }
        ss << "  taskConfiguration_: " << taskConfiguration_.toString()
           << std::endl; // Assuming TaskInformationPOD has a toString()
        ss << "  jobConfiguration_: " << jobConfiguration_.toString()
           << std::endl; // Assuming JobInformationPOD has a toString()
        ss << "  taskPlainInfoPod_: " << taskPlainInfoPod_.toString()
           << std::endl; // Assuming TaskPlainInfoPOD has a toString()

        ss << "  writers_: [";
        ss << "]" << std::endl;

        ss << "}";
        return ss.str();
    }
    void DeclineCheckpoint(long checkpointId)
    {
        checkpointResponder_->DeclineCheckpoint(jobConfiguration_.getJobId(), attemptIdpod_, checkpointId);
    }
    void setLocalRecoveryConfig(std::shared_ptr<LocalRecoveryConfig> config)
    {
        localRecoveryConfig = std::move(config);
        taskStateManager_->setLocalRecoveryConfig(localRecoveryConfig);
    }

    std::vector<std::shared_ptr<ResultPartitionWriter>> getAllWriters()
    {
        return writers_;
    }

    std::vector<std::shared_ptr<IndexedInputGate>> getInputGates()
    {
        return inputGates;
    }

private:
    std::shared_ptr<ShuffleEnvironment> omniShuffleEnvironment_;
    std::shared_ptr<RocksDBSharedResources> rocksDBSharedResources_;
    TaskInformationPOD taskConfiguration_;
    JobInformationPOD jobConfiguration_;
    TaskPlainInfoPOD taskPlainInfoPod_;
    ExecutionAttemptIDPOD attemptIdpod_;
    /**
     *  private final TaskInfo taskInfo;
     */
    std::vector<std::shared_ptr<ResultPartitionWriter>> writers_;
    std::vector<std::shared_ptr<IndexedInputGate>> inputGates;
    OmniTask* omniTask_ = nullptr;
    std::shared_ptr<TaskMetricGroup> taskMetricGroup_;
    std::shared_ptr<TaskOperatorEventGateway> operatorEventGateway;
    std::shared_ptr<TaskStateManager> taskStateManager_;
    std::shared_ptr<CheckpointResponder> checkpointResponder_;
    std::shared_ptr<TaskStateManagerBridge> taskstatemanagerbridge_;
    std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge_;
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
    // IndexedInputGate[] inputGates;
};
} // namespace omnistream
#endif
