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

#include "OperatorEventDispatcher.h"

void OperatorEventDispatcherImpl::OperatorEventGatewayImpl::SendEventToCoordinator(std::shared_ptr<OperatorEvent> event)
{
    /*
    std::shared_ptr<SerializedValue < OperatorEvent>>
            serializedEvent;
    try {
        serializedEvent = std::make_shared<OperatorEvent>(event);
    } catch (const std::exception &e) {
        // this is not a recoverable situation, so we wrap this in an
        // unchecked exception and let it bubble up
        throw std::runtime_error("Cannot serialize operator event");
    }
    */
    toCoordinator->SendOperatorEventToCoordinator(operatorId, event);
}

std::unordered_set<OperatorID> OperatorEventDispatcherImpl::GetRegisteredOperators()
{
    return std::unordered_set<OperatorID>();
}

void OperatorEventDispatcherImpl::RegisterEventHandler(
    const OperatorID& operatorId, std::shared_ptr<OperatorEventHandler> handler)
{
    auto result = handlers.insert(std::make_pair(operatorId, handler));
    if (!result.second) {
        throw std::invalid_argument("already a handler registered for this operatorId");
    }
}

void OperatorEventDispatcherImpl::DispatchEventToHandlers(
    const OperatorID& operatorID, std::shared_ptr<OperatorEvent> serializedEvent)
{
    // This logic needs recheck, in Flink, handleOperatorEvent taks a event as input. Ours takes string as input
    auto it = handlers.find(operatorID);
    if (it != handlers.end()) {
        it->second->handleOperatorEvent("TODO:Fix This String!");
    } else {
        throw std::runtime_error("Operator not registered for operator events");
    }
}
