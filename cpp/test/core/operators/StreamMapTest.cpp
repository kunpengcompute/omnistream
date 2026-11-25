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
#include <iostream>
#include "streaming/api/operators/StreamMap.h"
#include "test_utils/Mocks.h"

using namespace std;

// 测试 StreamMap 的构造函数
TEST(StreamMapTest, Constructor_ValidPath)
{
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "/tmp/libMockMapFunction.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, config);

    // 检查 output 是否正确设置
    EXPECT_EQ(&output, streamMap.GetOutput());
}

TEST(StreamMapTest, Constructor_InvalidPath)
{
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "invalid_path.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output);
    EXPECT_THROW(streamMap.loadUdf(config), std::out_of_range);
}

// 测试 processElement 方法
/*TEST(StreamMapTest, ProcessElement_Valid) {
    MockOutput *output = new MockOutput();
    StreamMap<MapFunction, int> *streamMap = new StreamMap<MapFunction, int>(output, "/tmp/libMockMapFunction.so");

    MockObject *input = new MockObject(10);
    StreamRecord *record = new StreamRecord(input);
    streamMap->processElement(record);

// 检查输出是否正确
    vector<StreamRecord*> collectedRecords = output->getCollectedRecords();
    EXPECT_EQ(collectedRecords.size(), 1);
    EXPECT_EQ(dynamic_cast<MockObject *>(collectedRecords[0]->getValue())->getValue(), 11);

    delete collectedRecords[0]->getValue();
    delete collectedRecords[0];
    delete input;
    delete streamMap;
    delete output;
}*/

// 测试 getName 方法
TEST(StreamMapTest, GetName)
{
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "/tmp/libMockMapFunction.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, config);

    EXPECT_STREQ(streamMap.getName(), "StreamMap");
}

// 测试 open 方法
TEST(StreamMapTest, Open_NotImplemented)
{
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "/tmp/libMockMapFunction.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, config);

    streamMap.open();

    // 不需要额外的检查，因为该方法未实现
}

// 测试 close 方法
TEST(StreamMapTest, Close_NotImplemented)
{
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "/tmp/libMockMapFunction.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, config);

    streamMap.close();
}
