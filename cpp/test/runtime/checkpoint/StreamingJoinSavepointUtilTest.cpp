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

#include <cstdint>
#include <stdexcept>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"

using omnistream::StreamingJoinSavepointUtil;

namespace {

nlohmann::json streamingJoinDescription()
{
    return {
        {"joinType", "InnerJoin"},
        {"leftInputSpec", "NoUniqueKey"},
        {"rightInputSpec", "NoUniqueKey"},
        {"leftUniqueKeys", nlohmann::json::array()},
        {"rightUniqueKeys", nlohmann::json::array()},
        {"leftInputTypes", {"BIGINT", "VARCHAR"}},
        {"rightInputTypes", {"BIGINT"}},
        {"leftJoinKey", {0}},
        {"rightJoinKey", {0}},
        {"filterNulls", {true}},
    };
}

ByteView byteView(const std::vector<int8_t>& bytes)
{
    return ByteView::fromBuffer(bytes.data(), bytes.size());
}

std::vector<int8_t> serializeFlinkMapValue(int32_t count, int32_t numAssociations, bool outerJoinState)
{
    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    output.writeBoolean(false);
    output.writeInt(count);
    if (outerJoinState) {
        output.writeInt(numAssociations);
    }
    return std::vector<int8_t>(
        reinterpret_cast<int8_t*>(output.getData()),
        reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
}

void expectJoinValueEquals(
    const StreamingJoinSavepointUtil::ParsedJoinValue& actual,
    const StreamingJoinSavepointUtil::ParsedJoinValue& expected)
{
    EXPECT_EQ(actual.count, expected.count);
    EXPECT_EQ(actual.numAssociations, expected.numAssociations);
    EXPECT_EQ(actual.outerJoinState, expected.outerJoinState);
}

} // namespace

TEST(StreamingJoinSavepointUtilTest, FlinkJoinValueRoundTripsInnerAndLeftOuterLayouts)
{
    StreamingJoinSavepointUtil::ParsedJoinValue inner;
    inner.count = 17;
    auto innerBytes = serializeFlinkMapValue(inner.count, inner.numAssociations, false);
    expectJoinValueEquals(StreamingJoinSavepointUtil::parseFlinkJoinValue(byteView(innerBytes), false), inner);

    StreamingJoinSavepointUtil::ParsedJoinValue outer;
    outer.count = 19;
    outer.numAssociations = 5;
    outer.outerJoinState = true;
    auto outerBytes = serializeFlinkMapValue(outer.count, outer.numAssociations, true);
    expectJoinValueEquals(StreamingJoinSavepointUtil::parseFlinkJoinValue(byteView(outerBytes), true), outer);
}

TEST(StreamingJoinSavepointUtilTest, RejectsNullTruncatedAndTrailingFlinkJoinPayloads)
{
    std::vector<int8_t> truncated = {0, 0, 0};
    EXPECT_THROW(StreamingJoinSavepointUtil::parseFlinkJoinValue(byteView(truncated), false), std::runtime_error);

    auto nullValue = serializeFlinkMapValue(1, 0, false);
    nullValue[0] = 1;
    EXPECT_THROW(StreamingJoinSavepointUtil::parseFlinkJoinValue(byteView(nullValue), false), std::runtime_error);

    auto flinkBytes = serializeFlinkMapValue(1, 0, false);
    flinkBytes.push_back(0);
    EXPECT_THROW(StreamingJoinSavepointUtil::parseFlinkJoinValue(byteView(flinkBytes), false), std::runtime_error);
}

TEST(StreamingJoinSavepointUtilTest, RejectsMalformedOrUnsupportedInputTypesAndJoinKeys)
{
    auto description = streamingJoinDescription();
    EXPECT_EQ(
        StreamingJoinSavepointUtil::getAdaptorType(description),
        FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);

    for (const auto& unsupportedType :
         std::vector<nlohmann::json>{nlohmann::json(), nlohmann::json(""), nlohmann::json("CHAR(8)")}) {
        description = streamingJoinDescription();
        description[StreamingJoinSavepointUtil::LEFT_INPUT_TYPES_FIELD][1] = unsupportedType;
        EXPECT_EQ(StreamingJoinSavepointUtil::getAdaptorType(description), FlinkSavepointAdaptorType::None);
        EXPECT_EQ(
            StreamingJoinSavepointUtil::buildUnsupportedReason(description),
            "StreamingJoin compatible savepoint only supports BIGINT, VARCHAR/STRING and TIMESTAMP input fields "
            "with precision <= 3");
    }

    for (const auto& invalidJoinKey : {nlohmann::json(-1), nlohmann::json(2), nlohmann::json("0")}) {
        description = streamingJoinDescription();
        description[StreamingJoinSavepointUtil::LEFT_JOIN_KEY_FIELD] = {invalidJoinKey};
        EXPECT_EQ(StreamingJoinSavepointUtil::getAdaptorType(description), FlinkSavepointAdaptorType::None);
        EXPECT_EQ(
            StreamingJoinSavepointUtil::buildUnsupportedReason(description),
            "StreamingJoin compatible savepoint requires BIGINT join keys");
    }
}
