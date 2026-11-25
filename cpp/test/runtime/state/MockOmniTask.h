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

#ifndef MOCKOMNITASK_H
#define MOCKOMNITASK_H

#include "taskmanager/OmniTask.h"

namespace omnistream {
class MockOmniTask : public OmniTask {
public:
    std::shared_ptr<TaskStateManagerBridge> stateBridge;
    std::shared_ptr<OmniTaskBridge> omni_task_bridge;
    std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge;

    MockOmniTask(
        std::shared_ptr<TaskStateManagerBridge> stateBridge,
        std::shared_ptr<OmniTaskBridge> omni_task_bridge,
        std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge)
        : OmniTask(stateBridge, omni_task_bridge, taskOperatorEventGatewayBridge, nullptr), //
          stateBridge(stateBridge),
          omni_task_bridge(omni_task_bridge),
          taskOperatorEventGatewayBridge(taskOperatorEventGatewayBridge)
    {
    }

    void triggerCheckpointBarrierV1(long checkpointid, long checkpointtimestamp, std::string checkpoint_options_str)
    {
        std::cout << "we are here at triggerCheckpointBarrierV1" << std::endl;
        std::string checkpointMetaDataJson = to_string(checkpointid);
        std::string checkpointMetricsJson = to_string(checkpointtimestamp);
        std::string acknowledgedStateJson = checkpoint_options_str;
        std::string localStateJson = "localStateJson";
        this->stateBridge->ReportTaskStateSnapshots(
            checkpointMetaDataJson, checkpointMetricsJson, acknowledgedStateJson, localStateJson);
    }
};
} // namespace omnistream

#endif // MOCKOMNITASK_H
