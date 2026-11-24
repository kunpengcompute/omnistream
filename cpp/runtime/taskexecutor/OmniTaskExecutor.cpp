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

#include <iostream>
#include <memory>

#include "OmniTaskExecutor.h"

#include "state/bridge/TaskOperatorEventGatewayBridge.h"

namespace omnistream {
OmniTaskExecutor::OmniTaskExecutor(std::shared_ptr<TaskManagerServices> taskManagerServices)
    : taskManagerServices_(taskManagerServices),
      taskManagerMetricGroup_(std::make_shared<TaskManagerMetricGroup>())
{
    // constructor
}

OmniTask* OmniTaskExecutor::submitTask(
    JobInformationPOD& jobInfo,
    TaskInformationPOD& taskInfo,
    TaskDeploymentDescriptorPOD& tdd,
    std::shared_ptr<TaskStateManagerBridge> stateBridge,
    std::shared_ptr<OmniTaskBridge> omni_task_bridge,
    std::shared_ptr<TaskOperatorEventGatewayBridge> TaskOperatorEventGatewayBridge,
    std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge,
    std::shared_ptr<RocksDBSharedResources> rocksDBSharedResources)
{
    std::string jobInformation;
    std::string taskInformation;
    auto shuffleEnv = this->taskManagerServices_->getShuffleEnvironment();
    auto* task = new OmniTask(
        jobInfo,
        taskInfo,
        tdd,
        shuffleEnv,
        stateBridge,
        omni_task_bridge,
        TaskOperatorEventGatewayBridge,
        remoteDataFetcherBridge);
    task->getRuntimeEnv()->setRocksDBSharedResources(std::move(rocksDBSharedResources));
    return task;
}

OmniTask* OmniTaskExecutor::submitTaskWithCK(
    JobInformationPOD& jobInfo,
    TaskInformationPOD& taskInfo,
    TaskDeploymentDescriptorPOD& tdd,
    std::shared_ptr<OmniTaskBridge> omni_task_bridge,
    std::shared_ptr<TaskOperatorEventGatewayBridge> TaskOperatorEventGatewayBridge,
    std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
{
    std::string jobInformation;
    std::string taskInformation;
    auto shuffleEnv = this->taskManagerServices_->getShuffleEnvironment();
    return new OmniTask(
        jobInfo, taskInfo, tdd, shuffleEnv, omni_task_bridge, TaskOperatorEventGatewayBridge, remoteDataFetcherBridge);
}

std::shared_ptr<TaskManagerMetricGroup> OmniTaskExecutor::GetTaskManagerMetricGroup() const
{
    return taskManagerMetricGroup_;
}
std::shared_ptr<TaskManagerServices> OmniTaskExecutor::GetTaskManagerService() const
{
    return taskManagerServices_;
}
} // namespace omnistream
