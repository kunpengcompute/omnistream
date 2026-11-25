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
#include "streaming/runtime/tasks/StreamTask.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include <memory>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include <fstream>

using namespace std;
using namespace testing;
using namespace omnistream;
using json = nlohmann::json;

static omnistream::datastream::StreamTask* createTaskTest()
{
    auto env = make_shared<omnistream::RuntimeEnvironmentV2>();
    // Create JSON for testing
    std::string ntdd =
        "{\"partition\":\n"
        "                {\"partitionName\":\"forward\",\"channelNumber\":1},\n"
        "            \"operators\":[\n"
        "                {\"output\":{\"kind\":\"basic\",\"type\":\"Long\"},\n"
        "                \"inputs\":[{\"kind\":\"basic\",\"type\":\"Long\"}],\n"
        "                \"name\":\"Map\",\n"
        "                "
        "\"description\":{\"inputTypes\":[{\"filed\":\"Long\",\"typeName\":\"org.apache.flink.api.common.typeutils."
        "base.LongSerializer\"}],\"key_so\":\"\",\"udf_so\":\"/tmp/libMockMapFunction.so\",\n"
        "                    \"index\":2,\n"
        "                    "
        "\"outputTypes\":{\"filed\":\"Long\",\"typeName\":\"org.apache.flink.api.common.typeutils.base."
        "LongSerializer\"},\n"
        "                    \"originDescription\":\"Map\"},\n"
        "                \"id\":\"org.apache.flink.streaming.api.operators.StreamMap\"}],\n"
        "            \"type\":\"DataStream\"}";

    omnistream::StreamConfigPOD streamConfigPod;
    omnistream::OperatorPOD operatorPod;
    std::string id = "org.apache.flink.streaming.api.operators.StreamMap";
    std::string description =
        R"({"udf_so":"/tmp/libMockMapFunction.so","udf_obj":"{}","stateKeyTypes":{"serializerName":"org.apache.flink.api.common.typeutils.base.LongSerializer"},"jobType":2})";
    operatorPod.setDescription(description);
    operatorPod.setId(id);
    operatorPod.setJobType(Type_o::STREAM);
    operatorPod.setTaskType(Type_o::STREAM);
    operatorPod.setVOperatorType(Type_o::STREAM);
    streamConfigPod.setOperatorDescription(operatorPod);
    omnistream::TaskInformationPOD taskInformationPOD;
    taskInformationPOD.setStreamConfigPOD(streamConfigPod);
    std::vector<omnistream::StreamConfigPOD> chained_config;
    chained_config.push_back(streamConfigPod);
    taskInformationPOD.setChainedConfig(chained_config);

    env->setTaskConfiguration(taskInformationPOD);
    OutputBufferStatus outputBufferStatus;
    outputBufferStatus.outputBuffer_ = reinterpret_cast<uintptr_t>(reinterpret_cast<uint8_t*>(malloc(1024)));
    outputBufferStatus.capacity_ = 1024;

    omnistream::datastream::StreamTask* streamTask =
        new omnistream::datastream::StreamTask(json::parse(ntdd), &outputBufferStatus, env);
    return streamTask;
}

TEST(StreamTaskTest, DISABLED_ExtractTaskPartitionerConfigValid)
{
    omnistream::datastream::StreamTask* streamTask = createTaskTest();
    json ntdd = R"(
    {
        "partition": {
            "partitionName": "forward",
            "channelNumber": 1
        }
    })"_json;

    omnistream::datastream::TaskPartitionerConfig config = streamTask->extractTaskPartitionerConfig(ntdd);
    EXPECT_EQ(config.getPartitionName(), "forward");
    EXPECT_EQ(config.getNumberOfChannel(), 1);
    delete streamTask;
}

TEST(StreamTaskTest, DISABLED_ExtractTaskPartitionerConfigInvalid)
{
    omnistream::datastream::StreamTask* streamTask = createTaskTest();
    json ntdd = R"({})"_json;

    omnistream::datastream::TaskPartitionerConfig config = streamTask->extractTaskPartitionerConfig(ntdd);
    EXPECT_EQ(config.getPartitionName(), "forward");
    EXPECT_EQ(config.getNumberOfChannel(), 1);
    delete streamTask;
}

TEST(StreamTaskTest, DISABLED_CreatePartitionerForward)
{
    omnistream::datastream::StreamTask* streamTask = createTaskTest();
    streamTask->setTaskPartitionerConfig(omnistream::datastream::TaskPartitionerConfig("forward", 1, nullptr));
    omnistream::datastream::StreamPartitioner<IOReadableWritable>* partitioner = streamTask->createPartitioner();
    EXPECT_NE(partitioner, nullptr);
    delete partitioner;
    delete streamTask;
}

TEST(StreamTaskTest, DISABLED_CreatePartitionerResacale)
{
    omnistream::datastream::StreamTask* streamTask = createTaskTest();
    streamTask->setTaskPartitionerConfig(omnistream::datastream::TaskPartitionerConfig("rescale", 1, nullptr));
    omnistream::datastream::StreamPartitioner<IOReadableWritable>* partitioner = streamTask->createPartitioner();
    EXPECT_NE(partitioner, nullptr);
    delete partitioner;
    delete streamTask;
}

TEST(StreamTaskTest, DISABLED_CreatePartitionerRebalance)
{
    omnistream::datastream::StreamTask* streamTask = createTaskTest();
    streamTask->setTaskPartitionerConfig(omnistream::datastream::TaskPartitionerConfig("rebalance", 1, nullptr));
    omnistream::datastream::StreamPartitioner<IOReadableWritable>* partitioner = streamTask->createPartitioner();
    EXPECT_NE(partitioner, nullptr);
    delete partitioner;
    delete streamTask;
}

TEST(StreamTaskTest, DISABLED_CreatePartitionerHash)
{
    omnistream::datastream::StreamTask* streamTask = createTaskTest();
    streamTask->setTaskPartitionerConfig(omnistream::datastream::TaskPartitionerConfig("hash", 1, nullptr));
    omnistream::datastream::StreamPartitioner<IOReadableWritable>* partitioner = streamTask->createPartitioner();
    EXPECT_EQ(partitioner, nullptr);
    delete partitioner;
    delete streamTask;
}
