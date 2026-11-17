#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>

#include "streaming/api/operators/StreamOperatorFactory.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "table/data/RowData.h"
#include "table/runtime/operators/misc/DropUpdateBeforeOperator.h"
#include "core/graph/OperatorConfig.h"
#include "streaming/runtime/tasks/WatermarkGaugeExposingOutput.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "test_utils/Mocks.h"
#include "nlohmann/json.hpp"
#include "runtime/executiongraph/StreamConfigPOD.h"
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

TEST(StreamOperatorFactoryTest, CreateOperatorAndCollector_DropUpdateBeforeSqlFilter)
{
    omnistream::OperatorPOD operatorPod;
    operatorPod.setId("org.apache.flink.streaming.api.operators.StreamFilter");
    operatorPod.setDescription(R"({"operator":"DropUpdateBefore"})");
    operatorPod.setVOperatorType(omnistream::Type_o::SQL);

    MockOutput output;
    std::unique_ptr<StreamOperator> operatorAndCollector(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(operatorPod, &output, nullptr));

    ASSERT_NE(operatorAndCollector, nullptr);
    auto* dropUpdateBefore =
        dynamic_cast<omnistream::DropUpdateBeforeOperator*>(operatorAndCollector.get());
    ASSERT_NE(dropUpdateBefore, nullptr);
    EXPECT_STREQ(dropUpdateBefore->getName(), "DropUpdateBefore");
    EXPECT_NE(dropUpdateBefore->getTypeName().find("DropUpdateBefore"), std::string::npos);
}

TEST(StreamOperatorFactoryTest, CreateOperatorAndCollector_SqlFilterWithoutDropUpdateBeforeMarkerFailsFast)
{
    omnistream::OperatorPOD operatorPod;
    operatorPod.setId("org.apache.flink.streaming.api.operators.StreamFilter");
    operatorPod.setDescription("{}");
    operatorPod.setVOperatorType(omnistream::Type_o::SQL);

    MockOutput output;
    EXPECT_THROW(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(operatorPod, &output, nullptr),
        std::logic_error);
}

TEST(StreamOperatorFactoryTest, CreateWindowJoinConfiguresCompatibleSavepointAdaptor)
{
    const std::string id = "org.apache.flink.table.runtime.operators.join.window.WindowJoinOperator.InnerJoinOperator";
    const std::string description = R"({
        "leftInputTypes": ["BIGINT", "BIGINT"],
        "rightInputTypes": ["BIGINT", "BIGINT"],
        "leftJoinKey": [0],
        "rightJoinKey": [0],
        "leftWindowEndIndex": 1,
        "rightWindowEndIndex": 1,
        "nonEquiCondition": null
    })";
    MockOutput output;

    auto* createdWindowJoin = MockStreamOperatorFactory::createOperatorAndCollector(id, description, &output);
    ASSERT_NE(createdWindowJoin, nullptr);
    // The factory test has no state initializer. open() initializes WindowJoin's collector before
    // failing on the deliberately absent timer service, which also makes normal destruction safe.
    EXPECT_THROW(createdWindowJoin->open(), std::logic_error);
    std::unique_ptr<StreamOperator> windowJoin(createdWindowJoin);

    auto* abstractOperator = dynamic_cast<AbstractStreamOperator<std::shared_ptr<RowData>>*>(windowJoin.get());
    ASSERT_NE(abstractOperator, nullptr);
    EXPECT_EQ(abstractOperator->getSavepointAdaptorInfo().type, FlinkSavepointAdaptorType::WindowJoinAdaptor);
    EXPECT_TRUE(abstractOperator->getSavepointAdaptorInfo().reason.empty());
    EXPECT_EQ(abstractOperator->getOperatorDescription(), nlohmann::json::parse(description));
}
