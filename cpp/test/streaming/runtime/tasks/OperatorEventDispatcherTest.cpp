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

// #include <gtest/gtest.h>
// #include <memory>
// #include <stdexcept>
// #include "streaming/runtime/tasks/OperatorEventDispatcher.h"
// // Mock classes for testing
// class MockTaskOperatorEventGateway : public TaskOperatorEventGateway {
// public:
//     void SendOperatorEventToCoordinator(OperatorID operatorId, std::shared_ptr<OperatorEvent> event) {};
// };
//
// class MockOperatorEventHandler : public OperatorEventHandler {
// public:
//     virtual ~MockOperatorEventHandler() = default;
//     void handleOperatorEvent(const std::string& eventString)
//     {
//         std::cout << eventString << std::endl;
//     }
// };
//
// // Test GetRegisteredOperators functionality
// TEST(OperatorEventDispatcherTest, GetRegisteredOperators)
// {
//
//     std::shared_ptr<TaskOperatorEventGateway> gateway = std::make_shared<MockTaskOperatorEventGateway>();
//     std::unique_ptr<OperatorEventDispatcherImpl> dispatcher = std::make_unique<OperatorEventDispatcherImpl>(gateway);
//     std::shared_ptr<MockOperatorEventHandler> mockHandler = std::make_shared<MockOperatorEventHandler>();
//     std::shared_ptr<OperatorEvent> event = std::make_shared<OperatorEvent>();
//
//     auto registeredOperators = dispatcher->GetRegisteredOperators();
//     EXPECT_TRUE(registeredOperators.empty());
//
//     OperatorID operatorId1 = OperatorID();
//     OperatorID operatorId2 = OperatorID();
//     OperatorID operatorId3 = OperatorID();
//
//     auto handler1 = std::make_shared<MockOperatorEventHandler>();
//     auto handler2 = std::make_shared<MockOperatorEventHandler>();
//     auto handler3 = std::make_shared<MockOperatorEventHandler>();
//
//     dispatcher->RegisterEventHandler(operatorId1, handler1);
//     dispatcher->RegisterEventHandler(operatorId2, handler2);
//     dispatcher->RegisterEventHandler(operatorId3, handler3);
//
//     auto registeredOperators2 = dispatcher->GetRegisteredOperators();
//
//     EXPECT_EQ(registeredOperators2.size(), 3);
//     EXPECT_TRUE(registeredOperators2.count(operatorId1) > 0);
//     EXPECT_TRUE(registeredOperators2.count(operatorId2) > 0);
//     EXPECT_TRUE(registeredOperators2.count(operatorId3) > 0);
// }
