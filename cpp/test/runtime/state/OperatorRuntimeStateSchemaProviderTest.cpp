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

#include "runtime/state/OperatorRuntimeStateSchemaProvider.h"

using namespace omnistream;

namespace {
nlohmann::json streamingJoinDescription(const std::string& joinType)
{
    return {{"joinType", joinType}, {"leftInputSpec", "NoUniqueKey"}, {"rightInputSpec", "NoUniqueKey"}};
}
} // namespace

TEST(OperatorRuntimeStateSchemaProviderTest, ResolvesInnerJoinMapStateSchema)
{
    auto provider = OperatorRuntimeStateSchemaProviderFactory::create(streamingJoinDescription("InnerJoin"));
    ASSERT_NE(provider, nullptr);

    auto left = provider->resolveMapStateSchema("left-records", BackendDataType::ROW_BK, BackendDataType::INT_BK);
    auto right = provider->resolveMapStateSchema("right-records", BackendDataType::ROW_BK, BackendDataType::INT_BK);
    ASSERT_TRUE(left.has_value());
    ASSERT_TRUE(right.has_value());
    EXPECT_EQ(left->keyBackendType, BackendDataType::SHARED_ROW_BK);
    EXPECT_EQ(left->valueBackendType, BackendDataType::INT_BK);
    EXPECT_EQ(right->keyBackendType, BackendDataType::SHARED_ROW_BK);
    EXPECT_EQ(right->valueBackendType, BackendDataType::INT_BK);
    EXPECT_FALSE(
        provider->resolveMapStateSchema("left-recordsvb", BackendDataType::OBJECT_BK, BackendDataType::OBJECT_BK)
            .has_value());
}

TEST(OperatorRuntimeStateSchemaProviderTest, ResolvesLeftOuterJoinMapStateSchema)
{
    auto provider = OperatorRuntimeStateSchemaProviderFactory::create(streamingJoinDescription("LeftOuterJoin"));
    ASSERT_NE(provider, nullptr);

    auto left =
        provider->resolveMapStateSchema("left-records", BackendDataType::ROW_BK, BackendDataType::TUPLE_INT32_INT32);
    auto right = provider->resolveMapStateSchema("right-records", BackendDataType::ROW_BK, BackendDataType::INT_BK);
    ASSERT_TRUE(left.has_value());
    ASSERT_TRUE(right.has_value());
    EXPECT_EQ(left->valueBackendType, BackendDataType::TUPLE_INT32_INT32);
    EXPECT_EQ(right->valueBackendType, BackendDataType::INT_BK);
}

TEST(OperatorRuntimeStateSchemaProviderTest, RejectsMismatchedCheckpointSchema)
{
    auto provider = OperatorRuntimeStateSchemaProviderFactory::create(streamingJoinDescription("InnerJoin"));
    ASSERT_NE(provider, nullptr);

    EXPECT_THROW(
        provider->resolveMapStateSchema("left-records", BackendDataType::OBJECT_BK, BackendDataType::INT_BK),
        std::runtime_error);
    EXPECT_THROW(
        provider->resolveMapStateSchema("left-records", BackendDataType::ROW_BK, BackendDataType::TUPLE_INT32_INT32),
        std::runtime_error);
}

TEST(OperatorRuntimeStateSchemaProviderTest, IgnoresNonStreamingJoinDescription)
{
    nlohmann::json windowJoinDescription{{"joinType", "InnerJoin"}, {"leftWindowEndIndex", 1}};
    EXPECT_EQ(OperatorRuntimeStateSchemaProviderFactory::create(windowJoinDescription), nullptr);
    EXPECT_EQ(OperatorRuntimeStateSchemaProviderFactory::create(nlohmann::json::object()), nullptr);
}
