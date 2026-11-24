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
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "core/graph/OperatorConfig.h"
#include "streaming/runtime/tasks/WatermarkGaugeExposingOutput.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "test_utils/Mocks.h"
#include "nlohmann/json.hpp"
#include "runtime/executiongraph/StreamConfigPOD.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include <unordered_map>

class MockStreamOperatorFactory : public omnistream::StreamOperatorFactory {
public:
    static StreamOperator* createOperatorAndCollector(
        omnistream::OperatorConfig& opConfig, WatermarkGaugeExposingOutput* chainOutput)
    {
        return StreamOperatorFactory::createOperatorAndCollector(opConfig, chainOutput);
    }

    static StreamOperator* createOperatorAndCollector(
        const std::string& id, const std::string& description, WatermarkGaugeExposingOutput* chainOutput)
    {
        omnistream::OperatorPOD operatorPod;
        operatorPod.setDescription(description);
        operatorPod.setId(id);
        operatorPod.setVOperatorType(omnistream::Type_o::STREAM);
        return StreamOperatorFactory::createOperatorAndCollector(operatorPod, chainOutput, nullptr);
    }
};

TEST(StreamOperatorFactoryTest, CreateOperatorAndCollector_Map)
{
    std::string id = "org.apache.flink.streaming.api.operators.StreamMap";
    std::string description = "{\"udf_so\":\"/tmp/libMockMapFunction.so\",\"udf_obj\":\"{}\"}";

    MockOutput output;
    StreamOperator* operatorAndCollector =
        MockStreamOperatorFactory::createOperatorAndCollector(id, description, &output);
    EXPECT_NE(operatorAndCollector, nullptr);
    delete operatorAndCollector;
}

TEST(StreamOperatorFactoryTest, CreateOperatorAndCollector_Map_InvalidSoPath)
{
    std::string id = "org.apache.flink.streaming.api.operators.StreamMap";
    std::string description = "{\"udf_so\":\"invalidMap.so\",\"udf_obj\":\"{}\"}";
    MockOutput output;
    EXPECT_THROW(MockStreamOperatorFactory::createOperatorAndCollector(id, description, &output), std::out_of_range);
}

TEST(StreamOperatorFactoryTest, CreateOperatorAndCollector_Reduce)
{
    std::string id = "org.apache.flink.streaming.api.operators.StreamMap";
    std::string description =
        "{\"udf_so\":\"/tmp/libMockReduceFunction.so\",\"key_so\":\"libMockKeyedBy.so\",\"udf_obj\":\"{}\"}";
    MockOutput output;
    StreamOperator* operatorAndCollector =
        MockStreamOperatorFactory::createOperatorAndCollector(id, description, &output);
    EXPECT_NE(operatorAndCollector, nullptr);
    delete operatorAndCollector;
}

TEST(StreamOperatorFactoryTest, CreateOperatorAndCollector_UnknownOperator)
{
    std::string id = "UnknownOperator";
    std::string description = "{}";

    MockOutput output;
    EXPECT_EQ(MockStreamOperatorFactory::createOperatorAndCollector(id, description, &output), nullptr);
}

TEST(StreamOperatorFactoryTest, GroupAggConfiguresCompatibleSavepointInBothFactoryPaths)
{
    const std::string id = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    for (bool hasRaw : {false, true}) {
        auto accTypes = nlohmann::json::array({"BIGINT"});
        if (hasRaw) {
            accTypes.push_back("RAW('example.DataView', '')");
        }
        const nlohmann::json description = {
            {"processFunction", "GroupAggFunction"},
            {"inputTypes", {"BIGINT"}},
            {"outputTypes", {"BIGINT", "BIGINT"}},
            {"grouping", {0}},
            {"distinctInfos", nlohmann::json::array()},
            {"aggInfoList", {{"indexOfCountStar", -1}, {"accTypes", accTypes}, {"aggValueTypes", {"BIGINT"}}}},
        };
        const auto expected =
            hasRaw ? FlinkSavepointAdaptorType::GroupAggAdaptor : FlinkSavepointAdaptorType::OmniIsCompatible;
        MockOutput output;
        omnistream::OperatorConfig config(id, "GroupAggregate", {}, {}, description);
        std::unique_ptr<StreamOperator> configOp(
            MockStreamOperatorFactory::createOperatorAndCollector(config, &output));
        std::unique_ptr<StreamOperator> podOp(
            MockStreamOperatorFactory::createOperatorAndCollector(id, description.dump(), &output));
        for (auto* op : {configOp.get(), podOp.get()}) {
            ASSERT_NE(op, nullptr);
            auto* aggOp = dynamic_cast<AbstractStreamOperator<RowData*>*>(op);
            ASSERT_NE(aggOp, nullptr);
            EXPECT_EQ(aggOp->getSavepointAdaptorInfo().type, expected);
            EXPECT_TRUE(aggOp->getSavepointAdaptorInfo().reason.empty());
            EXPECT_EQ(aggOp->getOperatorDescription(), description);
        }
    }
}
