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

#include <nlohmann/json.hpp>

#include "runtime/checkpoint/OperatorSavepointAdaptor.h"

using omnistream::OperatorSavepointAdaptor;

// ===== parseStringArray 测试 =====

// 正常解析字符串数组
TEST(OperatorSavepointAdaptorTest, ParseStringArrayReturnsCorrectValues)
{
    nlohmann::json json;
    json["inputTypes"] = {"INT", "VARCHAR", "BOOLEAN"};
    auto result = OperatorSavepointAdaptor::parseStringArray(json, "inputTypes");
    ASSERT_EQ(result.size(), 3u);
    EXPECT_EQ(result[0], "INT");
    EXPECT_EQ(result[1], "VARCHAR");
    EXPECT_EQ(result[2], "BOOLEAN");
}

// 字段不存在时返回空 vector
TEST(OperatorSavepointAdaptorTest, ParseStringArrayReturnsEmptyForMissingField)
{
    nlohmann::json json;
    json["otherField"] = {"INT"};
    auto result = OperatorSavepointAdaptor::parseStringArray(json, "inputTypes");
    EXPECT_TRUE(result.empty());
}

// 字段不是数组时返回空 vector
TEST(OperatorSavepointAdaptorTest, ParseStringArrayReturnsEmptyForNonArrayField)
{
    nlohmann::json json;
    json["inputTypes"] = "not_an_array";
    auto result = OperatorSavepointAdaptor::parseStringArray(json, "inputTypes");
    EXPECT_TRUE(result.empty());
}

// 空数组返回空 vector
TEST(OperatorSavepointAdaptorTest, ParseStringArrayReturnsEmptyForEmptyArray)
{
    nlohmann::json json;
    json["inputTypes"] = nlohmann::json::array();
    auto result = OperatorSavepointAdaptor::parseStringArray(json, "inputTypes");
    EXPECT_TRUE(result.empty());
}

// 数组中包含非字符串元素时跳过
TEST(OperatorSavepointAdaptorTest, ParseStringArraySkipsNonStringElements)
{
    nlohmann::json json;
    json["inputTypes"] = {"INT", 123, "VARCHAR", nullptr, "BOOLEAN"};
    auto result = OperatorSavepointAdaptor::parseStringArray(json, "inputTypes");
    ASSERT_EQ(result.size(), 3u);
    EXPECT_EQ(result[0], "INT");
    EXPECT_EQ(result[1], "VARCHAR");
    EXPECT_EQ(result[2], "BOOLEAN");
}

// 空 JSON 对象返回空 vector
TEST(OperatorSavepointAdaptorTest, ParseStringArrayReturnsEmptyForEmptyJson)
{
    nlohmann::json json;
    auto result = OperatorSavepointAdaptor::parseStringArray(json, "inputTypes");
    EXPECT_TRUE(result.empty());
}

// 单元素数组
TEST(OperatorSavepointAdaptorTest, ParseStringArrayHandlesSingleElement)
{
    nlohmann::json json;
    json["inputTypes"] = {"BIGINT"};
    auto result = OperatorSavepointAdaptor::parseStringArray(json, "inputTypes");
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], "BIGINT");
}

// ===== convertToDataTypes 测试 =====

// 正常转换基本类型
TEST(OperatorSavepointAdaptorTest, ConvertToDataTypesHandlesBasicTypes)
{
    std::vector<std::string> typeNames = {"INT", "BIGINT", "VARCHAR", "BOOLEAN"};
    auto result = OperatorSavepointAdaptor::convertToDataTypes(typeNames);
    ASSERT_EQ(result.size(), 4u);
    EXPECT_EQ(result[0], omniruntime::type::OMNI_INT);
    EXPECT_EQ(result[1], omniruntime::type::OMNI_LONG);
    EXPECT_EQ(result[2], omniruntime::type::OMNI_VARCHAR);
    EXPECT_EQ(result[3], omniruntime::type::OMNI_BOOLEAN);
}

// 空输入返回空结果
TEST(OperatorSavepointAdaptorTest, ConvertToDataTypesHandlesEmptyInput)
{
    std::vector<std::string> typeNames;
    auto result = OperatorSavepointAdaptor::convertToDataTypes(typeNames);
    EXPECT_TRUE(result.empty());
}

// 单元素输入
TEST(OperatorSavepointAdaptorTest, ConvertToDataTypesHandlesSingleType)
{
    std::vector<std::string> typeNames = {"DOUBLE"};
    auto result = OperatorSavepointAdaptor::convertToDataTypes(typeNames);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], omniruntime::type::OMNI_DOUBLE);
}

// 转换 TIMESTAMP 类型
TEST(OperatorSavepointAdaptorTest, ConvertToDataTypesHandlesTimestampType)
{
    std::vector<std::string> typeNames = {"TIMESTAMP", "TIMESTAMP_WITH_LOCAL_TIME_ZONE"};
    auto result = OperatorSavepointAdaptor::convertToDataTypes(typeNames);
    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], omniruntime::type::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE);
    EXPECT_EQ(result[1], omniruntime::type::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE);
}

// 转换 DECIMAL 类型
TEST(OperatorSavepointAdaptorTest, ConvertToDataTypesHandlesDecimalType)
{
    std::vector<std::string> typeNames = {"DECIMAL"};
    auto result = OperatorSavepointAdaptor::convertToDataTypes(typeNames);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], omniruntime::type::OMNI_DECIMAL64);
}

// 转换 FLOAT 和 DOUBLE 类型
TEST(OperatorSavepointAdaptorTest, ConvertToDataTypesHandlesFloatingPointTypes)
{
    std::vector<std::string> typeNames = {"FLOAT", "DOUBLE"};
    auto result = OperatorSavepointAdaptor::convertToDataTypes(typeNames);
    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], omniruntime::type::OMNI_INT);
    EXPECT_EQ(result[1], omniruntime::type::OMNI_DOUBLE);
}

// 转换 SMALLINT 和 TINYINT 类型
TEST(OperatorSavepointAdaptorTest, ConvertToDataTypesHandlesSmallIntTypes)
{
    std::vector<std::string> typeNames = {"SMALLINT", "TINYINT"};
    auto result = OperatorSavepointAdaptor::convertToDataTypes(typeNames);
    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], omniruntime::type::OMNI_SHORT);
    EXPECT_EQ(result[1], omniruntime::type::OMNI_SHORT);
}

// 转换 DATE 类型
TEST(OperatorSavepointAdaptorTest, ConvertToDataTypesHandlesDateType)
{
    std::vector<std::string> typeNames = {"DATE"};
    auto result = OperatorSavepointAdaptor::convertToDataTypes(typeNames);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], omniruntime::type::OMNI_DATE32);
}
