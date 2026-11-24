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

#ifndef OMNITASKEXECUTOR_H
#define OMNITASKEXECUTOR_H

#include <memory>
#include <executiongraph/JobInformationPOD.h>
#include <executiongraph/TaskInformationPOD.h>
#include <state/bridge/TaskStateManagerBridge.h>
#include <taskexecutor/TaskManagerServices.h>
#include <taskmanager/OmniTask.h>

#include "runtime/partition/ResultPartitionManager.h"
#include "state/bridge/TaskOperatorEventGatewayBridge.h"
#include "runtime/partition/consumer/RemoteDataFetcherBridge.h"
#include "runtime/metrics/groups/TaskManagerMetricGroup.h"

namespace omnistream {
class OmniTaskExecutor {
public:
    explicit OmniTaskExecutor(std::shared_ptr<TaskManagerServices> taskManagerServices);

    OmniTask* submitTask(
        JobInformationPOD& jobInfo,
        TaskInformationPOD& taskInfo,
        TaskDeploymentDescriptorPOD& tdd,
        std::shared_ptr<TaskStateManagerBridge> stateBridge,
        std::shared_ptr<OmniTaskBridge> omni_task_bridge,
        std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
        std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge,
        std::shared_ptr<RocksDBSharedResources> rocksDBSharedResources = nullptr);
    OmniTask* submitTaskWithCK(
        JobInformationPOD& jobInfo,
        TaskInformationPOD& taskInfo,
        TaskDeploymentDescriptorPOD& tdd,
        std::shared_ptr<OmniTaskBridge> omni_task_bridge,
        std::shared_ptr<TaskOperatorEventGatewayBridge> TaskOperatorEventGatewayBridge,
        std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge);

    std::shared_ptr<TaskManagerMetricGroup> GetTaskManagerMetricGroup() const;
    std::shared_ptr<TaskManagerServices> GetTaskManagerService() const;

private:
    std::shared_ptr<TaskManagerServices> taskManagerServices_;
    std::shared_ptr<TaskManagerMetricGroup> taskManagerMetricGroup_;
};
} // namespace omnistream

#endif // OMNITASKEXECUTOR_H
