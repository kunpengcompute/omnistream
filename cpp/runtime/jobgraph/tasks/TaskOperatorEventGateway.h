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

#ifndef OMNISTREAM_TASKOPERATOREVENTGATEWAY_H
#define OMNISTREAM_TASKOPERATOREVENTGATEWAY_H
#include "../OperatorID.h"
#include "operators/coordination/AcknowledgeCheckpointEvent.h"
#include "runtime/operators/coordination/OperatorEvent.h"

#include "runtime/state/bridge/TaskOperatorEventGatewayBridge.h"
class TaskOperatorEventGateway {
public:
    explicit TaskOperatorEventGateway(const std::shared_ptr<TaskOperatorEventGatewayBridge>& bridge) : bridge_(bridge)
    {
    }

    std::shared_ptr<TaskOperatorEventGatewayBridge> bridge_;
    /**
     * Sends an event from the operator (identified by the given operator ID) to the operator
     * coordinator (identified by the same ID).
     */
    virtual void SendOperatorEventToCoordinator(OperatorID operatorId, std::shared_ptr<OperatorEvent> event)
    {
        std::string operatorIdJson = operatorId.toString();
        std::string eventJson = event->toString();
        if (bridge_ != nullptr) {
            bridge_->sendOperatorEventToCoordinator(operatorIdJson, eventJson);
        }
    }
    virtual ~TaskOperatorEventGateway() = default;
    /**
     * Sends a request from current operator to a specified operator coordinator which is identified
     * by the given operator ID and return the response.
     */
    // todo
    // virtual CompletableFuture<CoordinationResponse> sendRequestToCoordinator(OperatorID operator,
    //    CoordinationRequest& request) = 0;
};
#endif // OMNISTREAM_TASKOPERATOREVENTGATEWAY_H
