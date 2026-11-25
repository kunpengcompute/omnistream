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

#include <gtest/gtest.h>
#include "streaming/api/operators/StreamGroupedReduceOperator.h"
#include "test_utils/Mocks.h"
#include "basictypes/Tuple2.h"
#include "basictypes/Long.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/TaskStateManager.h"
#include <memory>
#include <string>

using namespace std;

// 测试 StreamGroupedReduceOperator 的构造函数
TEST(StreamGroupedReduceOperatorTest, Constructor_ValidPaths)
{
    MockOutput output;
    string soPath = "/tmp/libMockReduceFunction.so";
    nlohmann::json config;
    config["udf_so"] = "/tmp/libMockReduceFunction.so";
    config["key_so"][0] = "libMockKeyedBy.so";
    config["hash_path"] = "/tmp/";
    config["udf_obj"] = "{}";

    omnistream::datastream::StreamGroupedReduceOperator<Object> reduceOp(&output);
    EXPECT_NO_THROW(reduceOp.loadUdf(config));
}

TEST(StreamGroupedReduceOperatorTest, Constructor_ReduceFunctionNull)
{
    MockOutput output;
    string soPath = "invalid_reduce.so";
    string keyBySoName = "libMockKeyedBy.so";
    nlohmann::json config;
    config["udf_so"] = "invalid_reduce.so";
    config["key_so"][0] = "libMockKeyedBy.so";
    config["hash_path"] = "/tmp/";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamGroupedReduceOperator<Object> reduceOp(&output);
    EXPECT_THROW(reduceOp.loadUdf(config), std::out_of_range);
}

// 测试 processElement 方法
TEST(StreamGroupedReduceOperatorTest, ProcessElement_NewKey)
{
    MockOutput output;
    string soPath = "/tmp/libMockReduceFunction.so";
    string keyBySoName = "/tmp/libMockKeyedBy.so";
    nlohmann::json config;
    config["udf_so"] = soPath;
    config["key_so"][0] = "libMockKeyedBy.so";
    config["hash_path"] = "/tmp/";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamGroupedReduceOperator<Object> reduceOp(&output, config);

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    reduceOp.setup();
    reduceOp.initializeState(initializer, new StringSerializer());
    reduceOp.open();
    // 创建一个 StreamRecord
    Object* key = new String("key1");
    Object* value = new Long(1);
    Tuple2* tuple = new Tuple2(key, value);
    StreamRecord* record = new StreamRecord(tuple);
    reduceOp.setKeyContextElement(record);
    // 处理元素
    reduceOp.processElement(record);

    // 检查输出是否正确
    vector<StreamRecord*> collectedRecords = output.getCollectedRecords();
    EXPECT_EQ(collectedRecords.size(), 1);
    EXPECT_EQ(dynamic_cast<Long*>(reinterpret_cast<Tuple2*>(collectedRecords[0]->getValue())->f1)->getValue(), 1);

    // 清理
    for (auto record : collectedRecords) {
        delete record;
    }
}

TEST(StreamGroupedReduceOperatorTest, ProcessElement_ExistingKey)
{
    MockOutput output;
    string soPath = "/tmp/libMockReduceFunction.so";
    string keyBySoName = "/tmp/libMockKeyedBy.so";
    nlohmann::json config;
    config["udf_so"] = soPath;
    config["key_so"][0] = "libMockKeyedBy.so";
    config["hash_path"] = "/tmp/";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamGroupedReduceOperator<Object> reduceOp(&output, config);
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    reduceOp.setup();
    reduceOp.initializeState(initializer, new StringSerializer());
    reduceOp.open();
    // 创建两个 StreamRecord
    Object* key1 = new String("key1");
    Object* value1 = new Long(1);
    Tuple2* tuple1 = new Tuple2(key1, value1);
    StreamRecord* record1 = new StreamRecord(tuple1);

    Object* key2 = new String("key1");
    Object* value2 = new Long(2);
    Tuple2* tuple2 = new Tuple2(key2, value2);
    StreamRecord* record2 = new StreamRecord(tuple2);

    reduceOp.setKeyContextElement(record1);
    // 处理第一个元素
    reduceOp.processElement(record1);

    reduceOp.setKeyContextElement(record2);
    // 再次处理相同的键
    reduceOp.processElement(record2);

    // 检查输出是否正确
    vector<StreamRecord*> collectedRecords = output.getCollectedRecords();
    EXPECT_EQ(collectedRecords.size(), 2);
    EXPECT_EQ(dynamic_cast<Long*>(reinterpret_cast<Tuple2*>(collectedRecords[1]->getValue())->f1)->getValue(), 3);

    // 清理
    for (auto record : collectedRecords) {
        delete record;
    }
}

// 测试 getName 方法
TEST(StreamGroupedReduceOperatorTest, GetName)
{
    MockOutput output;
    string soPath = "/tmp/libMockReduceFunction.so";
    string keyBySoName = "libMockKeyedBy.so";
    nlohmann::json config;
    config["udf_so"] = soPath;
    config["key_so"][0] = "libMockKeyedBy.so";
    config["hash_path"] = "/tmp/";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamGroupedReduceOperator<Object> reduceOp(&output, config);

    EXPECT_STREQ(reduceOp.getName(), "StreamGroupedReduceOperator");
}

// 测试 open 方法
TEST(StreamGroupedReduceOperatorTest, Open_NotImplemented)
{
    MockOutput output;
    string soPath = "/tmp/libMockReduceFunction.so";
    string keyBySoName = "libMockKeyedBy.so";
    nlohmann::json config;
    config["udf_so"] = soPath;
    config["key_so"][0] = "libMockKeyedBy.so";
    config["hash_path"] = "/tmp/";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamGroupedReduceOperator<Object> reduceOp(&output, config);
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    reduceOp.setup();
    reduceOp.initializeState(initializer, new LongSerializer());
    reduceOp.open();
}
