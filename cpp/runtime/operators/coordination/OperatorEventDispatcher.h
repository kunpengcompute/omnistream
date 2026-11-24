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

#ifndef OMNISTREAM_OPERATOREVENTDISPATCHER_H
#define OMNISTREAM_OPERATOREVENTDISPATCHER_H

#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <stdexcept>
#include <string>
#include <cassert>

// Flink includes - these would correspond to the Java imports
#include "runtime/jobgraph/tasks/TaskOperatorEventGateway.h"
#include "OperatorEvent.h"
#include "OperatorEventGateway.h"
#include "OperatorEventHandler.h"

class OperatorEventDispatcher {
public:
    /**
     * Register a listener that is notified every time an OperatorEvent is sent from the
     * OperatorCoordinator (of the operator with the given OperatorID) to this subtask.
     */
    virtual void RegisterEventHandler(const OperatorID& operatorId, std::shared_ptr<OperatorEventHandler> handler) = 0;

    /**
     * Gets the gateway through which events can be passed to the OperatorCoordinator for the
     * operator identified by the given OperatorID.
     */
    virtual std::shared_ptr<OperatorEventGateway> GetOperatorEventGateway(const OperatorID& operatorId) = 0;
};

/**
 * An implementation of the {@link OperatorEventDispatcher}.
 *
 * <p>This class is intended for single threaded use from the stream task mailbox.
 */
class OperatorEventDispatcherImpl : public OperatorEventDispatcher {
public:
    explicit OperatorEventDispatcherImpl(std::shared_ptr<TaskOperatorEventGateway> toCoordinator)
        : toCoordinator(std::move(toCoordinator)),
          handlers()
    {
    }

    void DispatchEventToHandlers(const OperatorID& operatorID, std::shared_ptr<OperatorEvent> serializedEvent);

    void RegisterEventHandler(const OperatorID& operatorId, std::shared_ptr<OperatorEventHandler> handler) override;

    std::unordered_set<OperatorID> GetRegisteredOperators();

    std::shared_ptr<OperatorEventGateway> GetOperatorEventGateway(const OperatorID& operatorId) override
    {
        return std::make_shared<OperatorEventGatewayImpl>(toCoordinator, operatorId);
    }

private:
    // ------------------------------------------------------------------------
    class OperatorEventGatewayImpl : public OperatorEventGateway {
    public:
        OperatorEventGatewayImpl(std::shared_ptr<TaskOperatorEventGateway> toCoordinator, const OperatorID& operatorId)
            : toCoordinator(toCoordinator),
              operatorId(operatorId)
        {
        }

        void SendEventToCoordinator(std::shared_ptr<OperatorEvent> event) override;

    private:
        std::shared_ptr<TaskOperatorEventGateway> toCoordinator;
        OperatorID operatorId;
    };
    std::shared_ptr<TaskOperatorEventGateway> toCoordinator;
    std::unordered_map<OperatorID, std::shared_ptr<OperatorEventHandler>> handlers;
};

#endif // OMNISTREAM_OPERATOREVENTDISPATCHER_H
