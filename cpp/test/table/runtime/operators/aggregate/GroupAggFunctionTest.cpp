#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include <nlohmann/json.hpp>
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "functions/Collector.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "core/typeutils/LongSerializer.h"
#include <gtest/gtest.h>
#include <chrono>
#include <unordered_map>
#include "table/data/binary/BinaryRowData.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "core/operators/OutputTest.h"

// using json = nlohmann::json;
using namespace omnistream;
using namespace omnistream::datastream;

template <typename T>
class DummyStreamPartitioner : public StreamPartitioner<T> {
public:
    int selectChannel(T* record) override
    {
        return 0;
    }

    std::unique_ptr<StreamPartitioner<T>> copy() override
    {
        return std::make_unique<DummyStreamPartitioner<T>>(*this);
    }

    bool isPointWise() const override
    {
        return true;
    }

    [[nodiscard]] std::string toString() const override
    {
        return "FORWARD";
    }
};

class DummyRecordWriterOutput : public WatermarkGaugeExposingOutput {
public:
    DummyRecordWriterOutput(TypeSerializer* outSerializer, RecordWriter* recordWriter)
        : outSerializer(outSerializer),
          recordWriter(recordWriter)
    {
    }

    ~DummyRecordWriterOutput()
    {
        delete outSerializer;
        delete recordWriter;
    }

    void collect(void* record) override
    {
    }
    void close() override
    {
    }
    void emitWatermark(Watermark* watermark) override
    {
    }

    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
    }

private:
    TypeSerializer* outSerializer;
    omnistream::datastream::RecordWriter* recordWriter;
};

BinaryRowData* createBinaryRowData(const std::vector<int64_t>& rows)
{
    size_t numCols = rows.size();
    BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(numCols);
    for (size_t i = 0; i < numCols; ++i) {
        rowData->setLong(i, rows[i]);
    }
    return rowData;
}

TEST(GroupAggFunctionTest, OneKeyOneMaxAggBigIntTest)
{
    // Create BinaryRowData for input rows
    // TODO: proper keySelector hasn't been implemented yet!
    int64_t keyVal = 30;
    BinaryRowData* input1 = BinaryRowData::createBinaryRowDataWithMem(2);
    input1->setLong(0, 3);
    input1->setLong(1, keyVal);

    BinaryRowData* input2 = BinaryRowData::createBinaryRowDataWithMem(2);
    input2->setLong(0, 2);
    input2->setLong(1, keyVal);

    BinaryRowData* input3 = BinaryRowData::createBinaryRowDataWithMem(2);
    input3->setLong(0, 4);
    input3->setLong(1, keyVal);

    // Warp BinaryRowData in StreamRecord
    StreamRecord* record1 = new StreamRecord(input1);
    StreamRecord* record2 = new StreamRecord(input2);
    StreamRecord* record3 = new StreamRecord(input3);

    // Operator description
    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    // Group by column 1, aggregate on column 0
    std::string description = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"accTypes": ["BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"LongMaxAggFunction","argIndexes":[0],"consumeRetraction":"false","name":"MAX($0)", "filterArg":-1}],"indexOfCountStar":-1},
                                        "grouping":[1],
                                        "distinctInfos":[],
                                        "inputTypes":["BIGINT","BIGINT"],
                                        "originDescription":"[3]:GroupAggregate(groupBy=[age], select=[age, MAX(id) AS EXPR$1])","outputTypes":["BIGINT","BIGINT"]},
                                        "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    json parsedJson = json::parse(description);

    omnistream::OperatorConfig opConfig(
        uniqueName,        // uniqueName:
        "Group_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    // Output writer description
    // RowDataSerializer * RowDataSerializer = new RowDataSerializer();
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    const int32_t BUFFER_CAPACITY = 256;
    uint8_t* address = new uint8_t[BUFFER_CAPACITY];
    auto* partitioner = new DummyStreamPartitioner<IOReadableWritable>();
    auto* recordWriter = new RecordWriter(address, BUFFER_CAPACITY, partitioner);
    auto* recordWriteOutput = new DummyRecordWriterOutput(binaryRowDataSerializer, recordWriter);

    // create streamOperatorFactory
    StreamOperatorFactory streamOperatorFactory;
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
            streamOperatorFactory.createOperatorAndCollector(opConfig, recordWriteOutput));

    // initiate keyedState
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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedOp->initializeState(initializer, ser);

    keyedOp->open();
    JoinedRowData* out = keyedOp->getResultRow();

    keyedOp->setKeyContextElement(record1);
    keyedOp->processElement(record1);

    ASSERT_EQ(*(out->getInt(1)), *(input1->getLong(0)));

    keyedOp->setKeyContextElement(record2);
    keyedOp->processElement(record2);

    ASSERT_EQ(*(out->getInt(1)), *(input1->getLong(0)));

    keyedOp->setKeyContextElement(record3);
    keyedOp->processElement(record3);

    ASSERT_EQ(*(out->getInt(1)), *(input3->getLong(0)));

    // clean up
    delete input1;
    delete input2;
    delete input3;
    delete record1;
    delete record2;
    delete record3;
}

// +-------+------------+---------+
// | A.id  | A.category | B.price |
// +-------+------------+---------+
// | 1     | 12         | 300     |
// | 1     | 12         | 500     | Max for {1,12} = 500
// | 4     | 40         | 350     | Max for {4,40} = 350
// | 2     | 12         | 400     |
// | 3     | 40         | 300     | Max for {3,40} = 300
// | 1     | 12         | 450     |
// | 2     | 12         | 450     | Max for {2,12} = 450
// | 3     | 40         | 200     |
// | 4     | 40         | 250     |
// +-------+------------+---------+

// HASHTABLE:
// Key (1, 12) -> 500
// Key (2, 12) -> 450
// Key (3, 40) -> 300
// Key (4, 40) -> 350

// Expected response:
// +----+----+----------+---------+
// | op | id | category | maximum |
// +----+----+----------+---------+
// | +I |  1 |       12 |     300 |
// | +U |  1 |       12 |     500 |
// | +I |  4 |       40 |     350 |
// | +I |  2 |       12 |     400 |
// | +I |  3 |       40 |     300 |
// | +U |  2 |       12 |     450 |
// +----+----+----------+---------+

TEST(GroupAggFunctionTest, TwoKeysOneMaxAggBigInt)
{
    std::vector<std::vector<int64_t>> rows = {
        {1, 12, 300},
        {1, 12, 500},
        {4, 40, 350},
        {2, 12, 400},
        {3, 40, 300},
        {1, 12, 450},
        {2, 12, 450},
        {3, 40, 200},
        {4, 40, 250}};

    // std::random_shuffle(rows.begin(), rows.end());

    // Prepare inputs and records
    std::vector<BinaryRowData*> inputs;
    std::vector<StreamRecord*> records;
    for (const auto& row : rows) {
        BinaryRowData* input = createBinaryRowData(row);
        inputs.push_back(input);
        records.push_back(new StreamRecord(input));
    }

    // Operator description
    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    // Group by column 1, aggregate on column 0
    std::string description = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"accTypes":["BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"LongMaxAggFunction","argIndexes":[2],"consumeRetraction":"false","name":"MAX($0)","filterArg":-1}],"indexOfCountStar":-1},
                                        "grouping":[0,1],
                                        "distinctInfos":[],
                                        "inputTypes":["BIGINT","BIGINT","BIGINT"],
                                        "originDescription":"[3]:GroupAggregate(groupBy=[age], select=[age, MAX(id) AS EXPR$1])","outputTypes":["BIGINT","BIGINT","BIGINT"]},
                                        "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    json parsedJson = json::parse(description);

    omnistream::OperatorConfig opConfig(
        uniqueName,        // uniqueName:
        "Group_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    // Output writer description
    // RowDataSerializer * RowDataSerializer = new RowDataSerializer();
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    const int32_t BUFFER_CAPACITY = 1000;
    uint8_t* address = new uint8_t[BUFFER_CAPACITY];
    auto* partitioner = new DummyStreamPartitioner<IOReadableWritable>();
    auto* recordWriter = new RecordWriter(address, BUFFER_CAPACITY, partitioner);
    auto* recordWriteOutput = new DummyRecordWriterOutput(binaryRowDataSerializer, recordWriter);

    // create streamOperatorFactory
    StreamOperatorFactory streamOperatorFactory;
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
            streamOperatorFactory.createOperatorAndCollector(opConfig, recordWriteOutput));

    // initiate keyedState
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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedOp->initializeState(initializer, ser);

    keyedOp->open();
    JoinedRowData* out = keyedOp->getResultRow();

    std::vector<std::tuple<RowKind, int, int, int>> expectedResponse = {
        {RowKind::INSERT, 1, 12, 300},
        {RowKind::UPDATE_AFTER, 1, 12, 500},
        {RowKind::INSERT, 4, 40, 350},
        {RowKind::INSERT, 2, 12, 400},
        {RowKind::INSERT, 3, 40, 300},
        {RowKind::INSERT, 3, 40, 300}, // not emitted
        {RowKind::UPDATE_AFTER, 2, 12, 450},
        {RowKind::UPDATE_AFTER, 2, 12, 450}, // not emitted
        {RowKind::UPDATE_AFTER, 2, 12, 450}  // not emitted
    };

    for (size_t i = 0; i < records.size(); ++i) {
        auto [expectedKind, expectedCol1, expectedCol2, expectedAgg] = expectedResponse[i];

        keyedOp->setKeyContextElement(records[i]);
        keyedOp->processElement(records[i]);
        if (i != 5 && i != 7 && i != 8) {
            ASSERT_EQ(out->getRowKind(), expectedKind);
            ASSERT_EQ(*(out->getInt(0)), expectedCol1);
            ASSERT_EQ(*(out->getInt(1)), expectedCol2);
            ASSERT_EQ(*(out->getInt(2)), expectedAgg);
        }
    }

    for (auto* input : inputs) delete input;
    for (auto* record : records) delete record;
}

// +----+----+----------+---------+--------------+---------------+
// | op | id | category | maximum | average_time | average_price |
// +----+----+----------+---------+--------------+---------------+
// | +I |  1 |       12 |     300 |          400 |           300 |
// | +U |  1 |       12 |     500 |          500 |           400 |
// | +I |  4 |       40 |     350 |          100 |           350 |
// | +I |  2 |       12 |     400 |          250 |           400 |
// | +I |  3 |       40 |     300 |          900 |           300 |
// | +U |  1 |       12 |     500 |          400 |           416 |
// | +U |  2 |       12 |     450 |          350 |           425 |
// | +I |  5 |       10 |     100 |          100 |           100 |
// | +U |  3 |       40 |     300 |          900 |           250 |
// | +U |  5 |       10 |     100 |          125 |           100 |
// | +U |  5 |       10 |     100 |          100 |           100 |
// | +U |  5 |       10 |     200 |          150 |           112 |
// | +U |  4 |       40 |     350 |          125 |           300 |
// +----+----+----------+---------+--------------+---------------+

// HASHTABLE:   [Max_p]  [Avg_t ]  [Avg_p ]
// Key (1, 12) -> {500,  1200, 3,  1250, 3}
// Key (2, 12) -> {450,   700, 2,   850, 2}
// Key (3, 40) -> {300,  1800, 2,   500, 2}
// Key (4, 40) -> {350,   250, 2,   600, 2}
// Key (5, 10) -> {200,  1000, 2,   600, 2}

TEST(GroupAggFunctionTest, TwoKeyTwoAggMaxAndAvg)
{
    std::vector<std::vector<int64_t>> rows = {
        {1, 12, 300, 400},
        {1, 12, 500, 600},
        {4, 40, 350, 100},
        {2, 12, 400, 250},
        {3, 40, 300, 900},
        {1, 12, 450, 200},
        {2, 12, 450, 450},
        {5, 10, 100, 100},
        {5, 10, 100, 100},
        {3, 40, 200, 900},
        {5, 10, 100, 100},
        {5, 10, 100, 200},
        {5, 10, 200, 500},
        {4, 40, 250, 150}};

    std::vector<BinaryRowData*> inputs;
    std::vector<StreamRecord*> records;
    for (const auto& row : rows) {
        BinaryRowData* input = createBinaryRowData(row);
        inputs.push_back(input);
        records.push_back(new StreamRecord(input));
    }

    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    std::string description = R"DELIM({
                                    "operators": [{ "description": {
                                                    "aggInfoList": {
                                                        "accTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"],
                                                        "aggValueTypes": ["BIGINT", "BIGINT", "BIGINT"],
                                                        "aggregateCalls": [
                                                            {"aggregationFunction": "LongMaxAggFunction", "argIndexes": [2], "consumeRetraction": "false", "name": "MAX($2)","filterArg":-1},
                                                            {"aggregationFunction": "LongAvgAggFunction", "argIndexes": [3], "consumeRetraction": "false", "name": "AVG($3)","filterArg":-1},
                                                            {"aggregationFunction": "LongAvgAggFunction", "argIndexes": [2], "consumeRetraction": "false", "name": "AVG($2)","filterArg":-1}
                                                        ],
                                                        "indexOfCountStar": -1
                                                    },
                                                    "grouping": [0,1],
                                                    "distinctInfos":[],
                                                    "inputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
                                                    "originDescription": "[3]:GroupAggregate(groupBy=[id, category], select=[id, category, MAX(price) AS maximum, AVG(time_seconds) AS average_time, AVG(price) AS average_price])",
                                                    "outputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"]
                                                }}] })DELIM";

    json parsedJson = json::parse(description);

    omnistream::OperatorConfig opConfig(
        uniqueName,
        "Group_By_Simple",
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    const int32_t BUFFER_CAPACITY = 1000;
    uint8_t* address = new uint8_t[BUFFER_CAPACITY];
    auto* partitioner = new DummyStreamPartitioner<IOReadableWritable>();
    RecordWriter* recordWriter = new RecordWriter(address, BUFFER_CAPACITY, partitioner);
    auto* recordWriteOutput = new DummyRecordWriterOutput(binaryRowDataSerializer, recordWriter);
    StreamOperatorFactory streamOperatorFactory;
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
            streamOperatorFactory.createOperatorAndCollector(opConfig, recordWriteOutput));
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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedOp->initializeState(initializer, ser);
    keyedOp->open();
    JoinedRowData* out = keyedOp->getResultRow();

    std::vector<std::tuple<RowKind, int, int, int, int, int>> expectedResponse = {
        {RowKind::INSERT, 1, 12, 300, 400, 300},
        {RowKind::UPDATE_AFTER, 1, 12, 500, 500, 400},
        {RowKind::INSERT, 4, 40, 350, 100, 350},
        {RowKind::INSERT, 2, 12, 400, 250, 400},
        {RowKind::INSERT, 3, 40, 300, 900, 300},
        {RowKind::UPDATE_AFTER, 1, 12, 500, 400, 416},
        {RowKind::UPDATE_AFTER, 2, 12, 450, 350, 425},
        {RowKind::INSERT, 5, 10, 100, 100, 100},
        {RowKind::INSERT, 5, 10, 100, 100, 100}, // skipped, not emitted
        {RowKind::UPDATE_AFTER, 3, 40, 300, 900, 250},
        {RowKind::UPDATE_AFTER, 3, 40, 300, 900, 250}, // skipped, not emitted
        {RowKind::UPDATE_AFTER, 5, 10, 100, 125, 100},
        {RowKind::UPDATE_AFTER, 5, 10, 200, 200, 120},
        {RowKind::UPDATE_AFTER, 4, 40, 350, 125, 300}};

    for (size_t i = 0; i < expectedResponse.size(); ++i) {
        auto [expectedKind, expectedCol1, expectedCol2, expectedMax, expectedAgg1, expectedAgg2] = expectedResponse[i];

        keyedOp->setKeyContextElement(records[i]);
        keyedOp->processElement(records[i]);

        if (i != 8 && i != 10) {
            ASSERT_EQ(out->getRowKind(), expectedKind);
            ASSERT_EQ(*(out->getInt(0)), expectedCol1);
            ASSERT_EQ(*(out->getInt(1)), expectedCol2);
            ASSERT_EQ(*(out->getInt(2)), expectedMax);
            ASSERT_EQ(*(out->getInt(3)), expectedAgg1);
            ASSERT_EQ(*(out->getInt(4)), expectedAgg2);
        }
    }

    for (auto* input : inputs) delete input;
    for (auto* record : records) delete record;
}

TEST(GroupAggFunctionTest, OneKeyOneMaxAggCompactTimestampTest)
{
    // Create BinaryRowData for input rows
    // TODO: proper keySelector hasn't been implemented yet!
    int64_t keyVal = 30;
    int precision = 3;

    BinaryRowData* input1 = BinaryRowData::createBinaryRowDataWithMem(3);
    input1->setLong(0, keyVal);
    TimestampData timestamp1 = TimestampData::fromEpochMillis(2L, 5);
    input1->setTimestamp(1, timestamp1, precision);

    BinaryRowData* input2 = BinaryRowData::createBinaryRowDataWithMem(3);
    input2->setLong(0, keyVal);
    TimestampData timestamp2 = TimestampData::fromEpochMillis(2L, 4);
    input2->setTimestamp(1, timestamp2, precision);

    BinaryRowData* input3 = BinaryRowData::createBinaryRowDataWithMem(3);
    input3->setLong(0, keyVal);
    TimestampData timestamp3 = TimestampData::fromEpochMillis(3L, 3);
    input3->setTimestamp(1, timestamp3, precision);

    // Warp BinaryRowData in StreamRecord
    StreamRecord* record1 = new StreamRecord(input1);
    StreamRecord* record2 = new StreamRecord(input2);
    StreamRecord* record3 = new StreamRecord(input3);

    // Operator description
    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    // Group by column 1, aggregate on column 0
    std::string description = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"accTypes":["BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"TimestampMaxAggFunction","argIndexes":[1],"consumeRetraction":"false","name":"MAX($1)","filterArg":-1}],"indexOfCountStar":-1},
                                        "grouping":[0],
                                        "distinctInfos":[],
                                        "inputTypes":["BIGINT","TIMESTAMP(3)"],
                                        "originDescription":"[3]:GroupAggregate(groupBy=[id], select=[id, MAX(age) AS EXPR$1])","outputTypes":["BIGINT","TIMESTAMP(3)"]},
                                        "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"kind":"logical","isNull":true,"precision":3,"type":"TIMESTAMP","timestampKind":0}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"kind":"logical","isNull":true,"precision":3,"type":"TIMESTAMP","timestampKind":0}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    json parsedJson = json::parse(description);

    omnistream::OperatorConfig opConfig(
        uniqueName,        // uniqueName:
        "Group_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    // Output writer description
    // RowDataSerializer * RowDataSerializer = new RowDataSerializer();
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(3);
    const int32_t BUFFER_CAPACITY = 256;
    uint8_t* address = new uint8_t[BUFFER_CAPACITY];
    auto* partitioner = new DummyStreamPartitioner<IOReadableWritable>();
    RecordWriter* recordWriter = new RecordWriter(address, BUFFER_CAPACITY, partitioner);
    auto* recordWriteOutput = new DummyRecordWriterOutput(binaryRowDataSerializer, recordWriter);

    // create streamOperatorFactory
    StreamOperatorFactory streamOperatorFactory;
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
            streamOperatorFactory.createOperatorAndCollector(opConfig, recordWriteOutput));

    // initiate keyedState
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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedOp->initializeState(initializer, ser);
    keyedOp->open();
    JoinedRowData* out = keyedOp->getResultRow();
    keyedOp->setKeyContextElement(record1);
    keyedOp->processElement(record1);

    EXPECT_EQ(out->getTimestamp(1).getMillisecond(), 2L);
    out->printRow();

    keyedOp->setKeyContextElement(record2);
    keyedOp->processElement(record2);

    EXPECT_EQ(out->getTimestamp(1).getMillisecond(), 2L);
    out->printRow();

    keyedOp->setKeyContextElement(record3);
    keyedOp->processElement(record3);

    EXPECT_EQ(out->getTimestamp(1).getMillisecond(), 3L);
    out->printRow();

    // clean up
    delete input1;
    delete input2;
    delete input3;
    delete record1;
    delete record2;
    delete record3;
}

TEST(GroupAggFunctionTest, OneKeyOneAggCountTest)
{
    // Create BinaryRowData for input rows
    // TODO: proper keySelector hasn't been implemented yet!
    int64_t keyVal = 30;
    BinaryRowData* input1 = BinaryRowData::createBinaryRowDataWithMem(2);
    input1->setLong(1, 3);
    input1->setLong(0, keyVal);

    BinaryRowData* input2 = BinaryRowData::createBinaryRowDataWithMem(2);
    input2->setLong(1, 2);
    input2->setLong(0, keyVal);

    BinaryRowData* input3 = BinaryRowData::createBinaryRowDataWithMem(2);
    input3->setLong(1, 4);
    input3->setLong(0, keyVal);

    // Warp BinaryRowData in StreamRecord
    StreamRecord* record1 = new StreamRecord(input1);
    StreamRecord* record2 = new StreamRecord(input2);
    StreamRecord* record3 = new StreamRecord(input3);

    // Operator description
    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    // Group by column 1, aggregate on column 0
    std::string description = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"accTypes":["BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"CountAggFunction","argIndexes":[1],"consumeRetraction":"false","name":"COUNT($1)","filterArg":-1}],"indexOfCountStar":-1},
                                        "grouping":[0],
                                        "distinctInfos":[],
                                        "inputTypes":["BIGINT", "BIGINT"],
                                        "originDescription":"[3]:GroupAggregate(groupBy=[id], select=[id, COUNT(auction) as total_auctions])",
                                        "outputTypes":["BIGINT"]},
                                        "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    json parsedJson = json::parse(description);

    omnistream::OperatorConfig opConfig(
        uniqueName,        // uniqueName:
        "Group_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    // Output writer description
    // RowDataSerializer * RowDataSerializer = new RowDataSerializer();
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    const int32_t BUFFER_CAPACITY = 256;
    uint8_t* address = new uint8_t[BUFFER_CAPACITY];
    auto* dummyPartitioner = new DummyStreamPartitioner<IOReadableWritable>();
    RecordWriter* recordWriter = new RecordWriter(address, BUFFER_CAPACITY, dummyPartitioner);
    auto* recordWriteOutput = new DummyRecordWriterOutput(binaryRowDataSerializer, recordWriter);

    // create streamOperatorFactory
    StreamOperatorFactory streamOperatorFactory;
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
            streamOperatorFactory.createOperatorAndCollector(opConfig, recordWriteOutput));

    // initiate keyedState
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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedOp->initializeState(initializer, ser);

    keyedOp->open();
    JoinedRowData* out = keyedOp->getResultRow();

    keyedOp->setKeyContextElement(record1);
    keyedOp->processElement(record1);

    EXPECT_EQ(*(out->getInt(1)), 1);

    keyedOp->setKeyContextElement(record2);
    keyedOp->processElement(record2);

    EXPECT_EQ(*(out->getInt(1)), 2);

    keyedOp->setKeyContextElement(record3);
    keyedOp->processElement(record3);

    EXPECT_EQ(*(out->getInt(1)), 3);

    // clean up
    delete input1;
    delete input2;
    delete input3;
    delete record1;
    delete record2;
    delete record3;
}

TEST(GroupAggFunctionTest, OneKeyOneAggCountAndAvgTest)
{
    // Create BinaryRowData for input rows
    // TODO: proper keySelector hasn't been implemented yet!
    int64_t keyVal = 30;
    BinaryRowData* input1 = BinaryRowData::createBinaryRowDataWithMem(3);
    input1->setLong(2, 5);
    input1->setLong(1, 3);
    input1->setLong(0, keyVal);

    BinaryRowData* input2 = BinaryRowData::createBinaryRowDataWithMem(3);
    input2->setLong(2, 1);
    input2->setLong(1, 2);
    input2->setLong(0, keyVal);

    BinaryRowData* input3 = BinaryRowData::createBinaryRowDataWithMem(3);
    input3->setLong(2, 3);
    input3->setLong(1, 4);
    input3->setLong(0, keyVal);

    // Warp BinaryRowData in StreamRecord
    StreamRecord* record1 = new StreamRecord(input1);
    StreamRecord* record2 = new StreamRecord(input2);
    StreamRecord* record3 = new StreamRecord(input3);

    // Operator description
    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    // Group by column 1, aggregate on column 0
    std::string description = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"accTypes":["BIGINT", "BIGINT", "BIGINT"],"aggValueTypes":["BIGINT", "BIGINT"],
                                            "aggregateCalls":[{"aggregationFunction":"CountAggFunction","argIndexes":[1],"consumeRetraction":"false","name":"COUNT($1)","filterArg":-1},
                                                                {"aggregationFunction":"LongAvgAggFunction","argIndexes":[2],"consumeRetraction":"false","name":"AVG($2)","filterArg":-1}],"indexOfCountStar":-1},
                                        "grouping":[0],
                                        "distinctInfos":[],
                                        "inputTypes":["BIGINT", "BIGINT", "BIGINT"],
                                        "outputTypes":["BIGINT", "BIGINT"]},
                                        "originDescription": "[3]:GroupAggregate(groupBy=[id], select=[id, COUNT(auctions) as total_auctions, AVG(price) AS average_price])",
                                        "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    json parsedJson = json::parse(description);

    omnistream::OperatorConfig opConfig(
        uniqueName,        // uniqueName:
        "Group_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    // Output writer description
    // RowDataSerializer * RowDataSerializer = new RowDataSerializer();
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(3);
    const int32_t BUFFER_CAPACITY = 256;
    uint8_t* address = new uint8_t[BUFFER_CAPACITY];
    auto* dummyPartitioner = new DummyStreamPartitioner<IOReadableWritable>();
    RecordWriter* recordWriter = new RecordWriter(address, BUFFER_CAPACITY, dummyPartitioner);
    auto* recordWriteOutput = new DummyRecordWriterOutput(binaryRowDataSerializer, recordWriter);

    // create streamOperatorFactory
    StreamOperatorFactory streamOperatorFactory;
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
            streamOperatorFactory.createOperatorAndCollector(opConfig, recordWriteOutput));

    // initiate keyedState
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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT),
         omnistream::RowField("col3", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedOp->initializeState(initializer, ser);

    keyedOp->open();
    JoinedRowData* out = keyedOp->getResultRow();

    keyedOp->setKeyContextElement(record1);
    keyedOp->processElement(record1);

    EXPECT_EQ(*(out->getInt(1)), 1);
    EXPECT_EQ(*(out->getInt(2)), 5);

    keyedOp->setKeyContextElement(record2);
    keyedOp->processElement(record2);

    EXPECT_EQ(*(out->getInt(1)), 2);
    EXPECT_EQ(*(out->getInt(2)), 3);

    keyedOp->setKeyContextElement(record3);
    keyedOp->processElement(record3);

    EXPECT_EQ(*(out->getInt(1)), 3);
    EXPECT_EQ(*(out->getInt(2)), 3);

    // clean up
    delete input1;
    delete input2;
    delete input3;
    delete record1;
    delete record2;
    delete record3;
}

// TODO: Need functionality to work with non-compact time
TEST(GroupAggFunctionTest, DISABLED_OneKeyOneMaxAggNonCompactTimestampTest)
{
    // Create BinaryRowData for input rows
    // TODO: proper keySelector hasn't been implemented yet!
    int64_t keyVal = 30;
    int precision = 4;
    BinaryRowData* input1 = BinaryRowData::createBinaryRowDataWithMem(3);
    input1->setLong(0, keyVal);
    TimestampData timestamp1 = TimestampData::fromEpochMillis(2L, 5);
    input1->setTimestamp(1, timestamp1, precision);

    BinaryRowData* input2 = BinaryRowData::createBinaryRowDataWithMem(3);
    input2->setLong(0, keyVal);
    TimestampData timestamp2 = TimestampData::fromEpochMillis(2L, 4);
    input2->setTimestamp(1, timestamp2, precision);

    BinaryRowData* input3 = BinaryRowData::createBinaryRowDataWithMem(3);
    input3->setLong(0, keyVal);
    TimestampData timestamp3 = TimestampData::fromEpochMillis(3L, 3);
    input3->setTimestamp(1, timestamp3, precision);

    // Warp BinaryRowData in StreamRecord
    StreamRecord* record1 = new StreamRecord(input1);
    StreamRecord* record2 = new StreamRecord(input2);
    StreamRecord* record3 = new StreamRecord(input3);

    // Operator description
    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    // Group by column 1, aggregate on column 0
    std::string description = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"accTypes":["BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"TimestampMaxAggFunction","argIndexes":[1, 2],"consumeRetraction":"false","name":"MAX($1)","filterArg":-1}],"indexOfCountStar":-1},
                                        "grouping":[0,1,2],
                                        "distinctInfos":[],
                                        "inputTypes":["BIGINT","TIMESTAMP(3)", "TIMESTAMP(3)"],
                                        "originDescription":"[3]:GroupAggregate(groupBy=[id], select=[id, MAX(millisec) AS EXPR$1, MAX(nannosec) AS EXPR$2])","outputTypes":["BIGINT","TIMESTAMP(3)", "TIMESTAMP(3)"]},
                                        "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"kind":"logical","isNull":true,"precision":3,"type":"TIMESTAMP","timestampKind":0}, {"kind":"logical","isNull":true,"precision":3,"type":"TIMESTAMP","timestampKind":0}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"kind":"logical","isNull":true,"precision":3,"type":"TIMESTAMP","timestampKind":0}, {"kind":"logical","isNull":true,"precision":3,"type":"TIMESTAMP","timestampKind":0}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    json parsedJson = json::parse(description);

    omnistream::OperatorConfig opConfig(
        uniqueName,        // uniqueName:
        "Group_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    // Output writer description
    // RowDataSerializer * RowDataSerializer = new RowDataSerializer();
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(3);
    const int32_t BUFFER_CAPACITY = 256;
    uint8_t* address = new uint8_t[BUFFER_CAPACITY];
    auto* dummyPartitioner = new DummyStreamPartitioner<IOReadableWritable>();
    RecordWriter* recordWriter = new RecordWriter(address, BUFFER_CAPACITY, dummyPartitioner);
    auto* recordWriteOutput = new DummyRecordWriterOutput(binaryRowDataSerializer, recordWriter);

    // create streamOperatorFactory
    StreamOperatorFactory streamOperatorFactory;
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
            streamOperatorFactory.createOperatorAndCollector(opConfig, recordWriteOutput));

    // initiate keyedState
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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedOp->initializeState(initializer, ser);

    keyedOp->open();
    JoinedRowData* out = keyedOp->getResultRow();
    keyedOp->setKeyContextElement(record1);
    out->printRow();
    EXPECT_EQ(out->getTimestampPrecise(1).getMillisecond(), 2L);
    EXPECT_EQ(out->getTimestampPrecise(1).getNanoOfMillisecond(), 5);

    out->printRow();
    keyedOp->setKeyContextElement(record2);
    keyedOp->processElement(record2);

    EXPECT_EQ(out->getTimestampPrecise(1).getMillisecond(), 2L);
    EXPECT_EQ(out->getTimestampPrecise(1).getNanoOfMillisecond(), 5);

    out->printRow();
    keyedOp->setKeyContextElement(record3);
    keyedOp->processElement(record3);

    EXPECT_EQ(out->getTimestampPrecise(1).getMillisecond(), 3L);
    EXPECT_EQ(out->getTimestampPrecise(1).getNanoOfMillisecond(), 3);
    EXPECT_EQ(out->getRow1()->getArity(), out->getRow2()->getArity());
    out->printRow();
    // clean up
    delete input1;
    delete input2;
    delete input3;
    delete record1;
    delete record2;
    delete record3;
}

omnistream::VectorBatch* newVectorBatchForQ17()
{
    omnistream::VectorBatch* vbatch = new omnistream::VectorBatch(10);
    auto col1 = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(10);
    for (int i = 0; i < 10; i++) {
        std::string_view dateStr = "a";
        col1->SetValue(i, dateStr);
    }
    auto col0 = new omniruntime::vec::Vector<int64_t>(10);
    auto col2 = new omniruntime::vec::Vector<bool>(10);
    auto col3 = new omniruntime::vec::Vector<bool>(10);
    auto col4 = new omniruntime::vec::Vector<bool>(10);
    auto col5 = new omniruntime::vec::Vector<int64_t>(10);
    std::array<int64_t, 10> bigint0 = {11, 22, 33, 11, 22, 33, 44, 55, 66, 11};
    std::array<bool, 10> bool2 = {false, false, true, false, false, true, false, false, true, false};
    std::array<bool, 10> bool3 = {true, false, false, false, true, false, false, false, false, true};
    std::array<bool, 10> bool4 = {false, true, false, true, false, false, true, true, false, false};
    std::array<int64_t, 10> bigint5 = {4, 5, 6, 7, 8, 9, 10, 2, 13, 11};
    for (int i = 0; i < 10; i++) {
        col0->SetValue(i, bigint0[i]);
        col2->SetValue(i, bool2[i]);
        col3->SetValue(i, bool3[i]);
        col4->SetValue(i, bool4[i]);
        col5->SetValue(i, bigint5[i]);
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    vbatch->Append(col0);
    vbatch->Append(col1);
    vbatch->Append(col2);
    vbatch->Append(col3);
    vbatch->Append(col4);
    vbatch->Append(col5);

    return vbatch;
}

std::string Q17ConfigStr_agg = R"delimiter({
        "name" : "GroupAggregate(groupBy=[auction, day], select=[auction, day, COUNT(*) AS total_bids, COUNT(*) FILTER $f2 AS rank1_bids,COUNT(*) FILTER $f3 AS rank2_bids, COUNT(*) FILTER $f4 AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price,AVG(price) AS avg_price, SUM(price) AS sum_price])",
        "description":{
        "inputTypes":["BIGINT","VARCHAR(2147483647)","BOOLEAN","BOOLEAN","BOOLEAN","BIGINT"],
        "aggInfoList":{
        "aggregateCalls":[
        {"name":"COUNT()","filterArg":-1,"argIndexes":[],"aggregationFunction":"Count1AggFunction","consumeRetraction":"false"},
        {"name":"COUNT() FILTER $2","filterArg":2,"argIndexes":[],"aggregationFunction":"Count1AggFunction","consumeRetraction":"false"},
        {"name":"COUNT() FILTER $3","filterArg":3,"argIndexes":[],"aggregationFunction":"Count1AggFunction","consumeRetraction":"false"},
        {"name":"COUNT() FILTER $4","filterArg":4,"argIndexes":[],"aggregationFunction":"Count1AggFunction","consumeRetraction":"false"},
        {"name":"MIN($5)","filterArg":-1,"argIndexes":[5],"aggregationFunction":"LongMinAggFunction","consumeRetraction":"false"},
        {"name":"MAX($5)","filterArg":-1,"argIndexes":[5],"aggregationFunction":"LongMaxAggFunction","consumeRetraction":"false"},
        {"name":"AVG($5)","filterArg":-1,"argIndexes":[5],"aggregationFunction":"LongAvgAggFunction","consumeRetraction":"false"},
        {"name":"SUM($5)","filterArg":-1,"argIndexes":[5],"aggregationFunction":"LongSumAggFunction","consumeRetraction":"false"}
        ],
        "indexOfCountStar":-1,
        "accTypes":["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"],
        "aggValueTypes":["BIGINT","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"]
        },
        "generateUpdateBefore":true,
        "outputTypes":["BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT","BIGINT"],
        "distinctInfos":[],
        "grouping":[0,1],
        "originDescription":null
        },
        "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator"
        })delimiter";

TEST(GroupAggFunctionTest, VectorBatch_Q17Agg)
{
    nlohmann::json parsedJson = nlohmann::json::parse(Q17ConfigStr_agg);
    OperatorConfig opConfig(
        parsedJson["id"],   // uniqueName:
        parsedJson["name"], // Name
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    GroupAggFunction* func = new GroupAggFunction(0l, opConfig.getDescription());
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        new KeyedProcessOperator(func, output, opConfig.getDescription());
    keyedOp->setup();

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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    omnistream::VectorBatch* vbatch = newVectorBatchForQ17();
    // Batch
    /*
    (long) , (varchar)  (bool)      (bool)     (bool)    (long)
    keyCol1, keyCol2 , valueCol1, valueCol2, valueCol3, valueCol4, RowType
    11      a           0           1           0           4       +I
    22      a           0           0           1           5       +I
    33      a           1           0           0           6       +I
    11      a           0           0           1           7       +I
    22      a           0           1           0           8       +I
    33      a           1           0           0           9       +I
    44      a           0           0           1           10      +I
    55      a           0           0           1           2       +I
    66      a           1           0           0           13      +I
    11      a           0           1           0           11      +I
    */
    keyedOp->processBatch(new StreamRecord(vbatch));
    omnistream::VectorBatch* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    int rowCount = resultBatch->GetRowCount();
    int colCount = resultBatch->GetVectorCount();
    EXPECT_EQ(rowCount, 6);
    EXPECT_EQ(colCount, 10);
    std::vector types = {
        "BIGINT", "VARCHAR", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"};
    std::cout << "========================print result round 1============================" << std::endl;
    for (int i = 0; i < rowCount; i++) {
        for (int j = 0; j < colCount; j++) {
            if (types[j] == "VARCHAR") {
                auto result = reinterpret_cast<
                                  omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                                  resultBatch->Get(j))
                                  ->GetValue(i);
                std::string resStr(result);
                std::cout << resStr;
            } else if (types[j] == "INTEGER") {
                int result = resultBatch->GetValueAt<int32_t>(j, i);
                std::cout << result;
            } else if (types[j] == "BIGINT") {
                long result = resultBatch->GetValueAt<int64_t>(j, i);
                std::cout << result;
            } else {
                std::string result = "NNNNO";
                std::cout << result;
            }
            std::cout << " ";
        }
        std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
    }
    // outputBatch
    /*
    (long) , (varchar)  (bool)      (bool)     (bool)    (long)
    keyCol1, keyCol2 , valueCol1, valueCol2, valueCol3, valueCol4, valueCol5,valueCol6,valueCol7, valueCol8, RowType
    55          a        1           0           0       1           2        2         2           2           +I
    33          a        2           2           0       0           6        9         7           15          +I
    66          a        1           1           0       0           13       13        13          13          +I
    22          a        2           0           1       1           5        8         6           13          +I
    44          a        1           0           0       1           10       10        10          10          +I
    11          a        3           0           2       1           4        11        7           22          +I
    */

    omnistream::VectorBatch* vbatch2 = newVectorBatchForQ17();
    keyedOp->processBatch(new StreamRecord(vbatch2));
    omnistream::VectorBatch* resultBatch2 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    int rowCount2 = resultBatch2->GetRowCount();
    int colCount2 = resultBatch2->GetVectorCount();
    EXPECT_EQ(rowCount2, 12);
    EXPECT_EQ(colCount2, 10);
    std::cout << "========================print result round 2============================" << std::endl;
    for (int i = 0; i < rowCount2; i++) {
        for (int j = 0; j < colCount2; j++) {
            if (types[j] == "VARCHAR") {
                auto result = reinterpret_cast<
                                  omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                                  resultBatch2->Get(j))
                                  ->GetValue(i);
                std::string resStr(result);
                std::cout << resStr;
            } else if (types[j] == "INTEGER") {
                int result = resultBatch2->GetValueAt<int32_t>(j, i);
                std::cout << result;
            } else if (types[j] == "BIGINT") {
                long result = resultBatch2->GetValueAt<int64_t>(j, i);
                std::cout << result;
            } else {
                std::string result = "NNNNO";
                std::cout << result;
            }
            std::cout << " ";
        }
        std::cout << to_string(resultBatch2->getRowKind(i)) << std::endl;
    }
    // outputBatch
    /*
    (long) , (varchar)  (bool)      (bool)     (bool)    (long)
    keyCol1, keyCol2 , valueCol1, valueCol2, valueCol3, valueCol4, valueCol5,valueCol6,valueCol7, valueCol8, RowType
      55        a          0          0         0           1           2       2          2        2           -U
      55        a          1          0         0           2           2       2          2        4           +U
      33        a          1          2         0           0           6       9          7        15          -U
      33        a          3          4         0           0           6       9          7        30          +U
      66        a          0          1         0           0           13      13         13       13          -U
      66        a          1          2         0           0           13      13         13       26          +U
      22        a          1          0         1           1           5       8          6        13          -U
      22        a          3          0         2           2           5       8          6        26          +U
      44        a          0          0         0           1           10      10         10       10          -U
      44        a          1          0         0           2           10      10         10       20          +U
      11        a          2          0         2           1           4       11         7        22          -U
      11        a          5          0         4           2           4       11         7        44          +U
    */
}

omnistream::VectorBatch* newVectorBatchOneKeyOneValue_1()
{
    omnistream::VectorBatch* vbatch = new omnistream::VectorBatch(5);
    auto vKey = new omniruntime::vec::Vector<int64_t>(5);
    auto vValue = new omniruntime::vec::Vector<int64_t>(5);
    std::array<long, 5> key = {1, 2, 3, 2, 3};
    std::array<long, 5> value = {5, 3, 8, 6, 4};
    for (int i = 0; i < 5; i++) {
        vKey->SetValue(i, key[i]);
        vValue->SetValue(i, value[i]);
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    vbatch->Append(vKey);
    vbatch->Append(vValue);
    return vbatch;
}

omnistream::VectorBatch* newVectorBatchOneKeyOneValue_2()
{
    omnistream::VectorBatch* vbatch = new omnistream::VectorBatch(5);
    auto vKey = new omniruntime::vec::Vector<int64_t>(5);
    auto vValue = new omniruntime::vec::Vector<int64_t>(5);
    std::array<long, 5> key = {1, 4, 2, 3, 2};
    std::array<long, 5> value = {7, 5, 12, 10, 5};
    for (int i = 0; i < 5; i++) {
        vKey->SetValue(i, key[i]);
        vValue->SetValue(i, value[i]);
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    vbatch->Append(vKey);
    vbatch->Append(vValue);
    return vbatch;
}

TEST(GroupAggFunctionTest, VectorBatch_OneKeyMaxAggTest)
{
    // Operator description
    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    // Group by column 0, aggregate on column 1
    std::string description = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"accTypes": ["BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"LongMaxAggFunction","argIndexes":[1],"consumeRetraction":"false","filterArg":-1,"name":"MAX($0)"}],"indexOfCountStar":-1},
                                        "grouping":[0],
                                        "inputTypes":["BIGINT","BIGINT"],
                                        "originDescription":"[3]:GroupAggregate(groupBy=[age], select=[age, MAX(id) AS EXPR$1])",
                                        "outputTypes":["BIGINT","BIGINT"],
                                        "distinctInfos":[]},
                                        "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    json parsedJson = json::parse(description);
    omnistream::OperatorConfig opConfig(
        uniqueName,        // uniqueName:
        "Group_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    BatchOutputTest* output = new BatchOutputTest();

    // create streamOperatorFactory
    StreamOperatorFactory streamOperatorFactory;
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
            streamOperatorFactory.createOperatorAndCollector(opConfig, output));
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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    /* Input Batch
    (long) , (long)   , (RowType)
    keyCol, valueCol   , RowType
    1        , 5         , +I
    2        , 3         , +I
    3        , 8         , +I
    2        , 6         , +I
    3        , 4         , +I
    */

    omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneValue_1();
    auto record = new StreamRecord(vbatch);
    keyedOp->processBatch(record);

    omnistream::VectorBatch* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    // print VectorBatch
    int rowCount = resultBatch->GetRowCount();
    int colCount = resultBatch->GetVectorCount();
    EXPECT_EQ(rowCount, 3);
    EXPECT_EQ(colCount, 2);
    std::cout << "=========== print result1 ==========" << std::endl;
    for (int i = 0; i < rowCount; i++) {
        for (int j = 0; j < colCount; j++) {
            long result = resultBatch->GetValueAt<int64_t>(j, i);
            std::cout << result;
            std::cout << " ";
        }
        std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
    }
    delete output->getStreamRecord();

    /* Expect output
    (long) , (long)   , (RowType)
    keyCol, valueCol   , RowType
    3        , 8         , +I
    2        , 6         , +I
    1        , 5         , +I
    */

    /* Input Batch2
    (long) , (long)   , (RowType)
    keyCol, valueCol   , RowType
    1        , 7         , +I
    4        , 5         , +I
    2        , 12        , +I
    3        , 10        , +I
    2        , 5         , +I
    */
    omnistream::VectorBatch* vbatch2 = newVectorBatchOneKeyOneValue_2();
    auto record2 = new StreamRecord(vbatch2);
    keyedOp->processBatch(record2);

    omnistream::VectorBatch* resultBatch2 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    // print VectorBatch
    int rowCount2 = resultBatch2->GetRowCount();
    int colCount2 = resultBatch2->GetVectorCount();
    EXPECT_EQ(rowCount2, 4);
    EXPECT_EQ(colCount2, 2);
    std::cout << "=========== print result2 ==========" << std::endl;
    for (int i = 0; i < rowCount2; i++) {
        for (int j = 0; j < colCount2; j++) {
            long result = resultBatch2->GetValueAt<int64_t>(j, i);
            std::cout << result;
            std::cout << " ";
        }
        std::cout << to_string(resultBatch2->getRowKind(i)) << std::endl;
    }
    /* Expect output2
    (long) , (long)   , (RowType)
    keyCol, valueCol   , RowType
    3        , 10         , +U
    2        , 12         , +U
    4        , 5          , +I
    1        , 7          , +U
    */
}

TEST(GroupAggFunctionTest, VectorBatch_OneKeyAvgAggTest)
{
    // Operator description
    std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    // Group by column 0, aggregate on column 1
    std::string description = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"accTypes": ["BIGINT","BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"LongAvgAggFunction","argIndexes":[1],"consumeRetraction":"false","filterArg":-1,"name":"AVG($0)"}],"indexOfCountStar":-1},
                                        "grouping":[0],
                                        "inputTypes":["BIGINT","BIGINT"],
                                        "originDescription":"[3]:GroupAggregate(groupBy=[age], select=[age, AVG(id) AS EXPR$1])",
                                        "outputTypes":["BIGINT","BIGINT"],
                                        "distinctInfos":[]},
                                        "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    json parsedJson = json::parse(description);
    omnistream::OperatorConfig opConfig(
        uniqueName,        // uniqueName:
        "Group_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    BatchOutputTest* output = new BatchOutputTest();

    // create streamOperatorFactory
    StreamOperatorFactory streamOperatorFactory;
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
            streamOperatorFactory.createOperatorAndCollector(opConfig, output));
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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    /* Input Batch
    (long) , (long)   , (RowType)
    keyCol, valueCol   , RowType
    1        , 5         , +I
    2        , 3         , +I
    3        , 8         , +I
    2        , 6         , +I
    3        , 4         , +I
    */

    omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneValue_1();
    auto record = new StreamRecord(vbatch);
    keyedOp->processBatch(record);

    omnistream::VectorBatch* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    // print VectorBatch
    int rowCount = resultBatch->GetRowCount();
    int colCount = resultBatch->GetVectorCount();
    EXPECT_EQ(rowCount, 3);
    EXPECT_EQ(colCount, 2);
    std::cout << "=========== print result1 ==========" << std::endl;
    for (int i = 0; i < rowCount; i++) {
        for (int j = 0; j < colCount; j++) {
            long result = resultBatch->GetValueAt<int64_t>(j, i);
            std::cout << result;
            std::cout << " ";
        }
        std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
    }

    /* Expect output
    (long) , (long)   , (RowType)
    keyCol, valueCol   , RowType
    3        , 6         , +I
    2        , 4         , +I
    1        , 5         , +I
    */

    omnistream::VectorBatch* vbatch2 = newVectorBatchOneKeyOneValue_2();
    auto record2 = new StreamRecord(vbatch2);
    keyedOp->processBatch(record2);
    /* Input Batch2
    (long) , (long)   , (RowType)
    keyCol, valueCol   , RowType
    1        , 7         , +I
    4        , 5         , +I
    2        , 12        , +I
    3        , 10        , +I
    2        , 5         , +I
    */

    omnistream::VectorBatch* resultBatch2 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    // print VectorBatch
    int rowCount2 = resultBatch2->GetRowCount();
    int colCount2 = resultBatch2->GetVectorCount();
    EXPECT_EQ(rowCount2, 4);
    EXPECT_EQ(colCount2, 2);
    std::cout << "=========== print result2 ==========" << std::endl;
    for (int i = 0; i < rowCount2; i++) {
        for (int j = 0; j < colCount2; j++) {
            long result = resultBatch2->GetValueAt<int64_t>(j, i);
            std::cout << result;
            std::cout << " ";
        }
        std::cout << to_string(resultBatch2->getRowKind(i)) << std::endl;
    }
    delete output->getStreamRecord();

    /* Expect output2
    (long) , (long)   , (RowType)
    keyCol, valueCol   , RowType
    3        , 7         , +u
    2        , 6         , +U
    4        , 5         , +I
    1        , 6         , +U
    */
}

omnistream::VectorBatch* newVectorBatchMultiKeyVarcharAvg()
{
    // Columns: name (VARCHAR), id (VARCHAR), num (INTEGER).
    omnistream::VectorBatch* vbatch = new omnistream::VectorBatch(11);
    auto colName = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(11);
    auto colId = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(11);
    auto colNum = new omniruntime::vec::Vector<int32_t>(11);

    std::array<std::string_view, 11> names = {
        "cat_a", "cat_a", "cat_a", "cat_a", "cat_a", "cat_a", "cat_a", "cat_b", "cat_b", "cat_b", "cat_c"};
    std::array<std::string_view, 11> ids = {"p1", "p1", "p2", "p2", "p1", "p2", "p3", "q1", "q1", "q2", "r1"};
    std::array<int32_t, 11> nums = {5, 1, 3, 3, 1, 3, 1, 4, 2, 2, 1};
    for (int i = 0; i < 11; i++) {
        colName->SetValue(i, names[i]);
        colId->SetValue(i, ids[i]);
        colNum->SetValue(i, nums[i]);
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    vbatch->Append(colName);
    vbatch->Append(colId);
    vbatch->Append(colNum);
    return vbatch;
}

std::string MultiKeyVarcharAvgConfigStr = R"delimiter({
        "name" : "GroupAggregate(groupBy=[name, id], select=[name, id, AVG(num) AS $2])",
        "description":{
        "inputTypes":["VARCHAR(2147483647)","VARCHAR(2147483647)","INTEGER"],
        "aggInfoList":{
        "aggregateCalls":[
        {"name":"AVG($2)","filterArg":-1,"argIndexes":[2],"aggregationFunction":"IntAvgAggFunction","consumeRetraction":"false"}
        ],
        "indexOfCountStar":-1,
        "accTypes":["BIGINT","BIGINT"],
        "aggValueTypes":["INT"]
        },
        "generateUpdateBefore":true,
        "outputTypes":["VARCHAR(2147483647)","VARCHAR(2147483647)","INTEGER"],
        "distinctInfos":[],
        "grouping":[0,1],
        "originDescription":null
        },
        "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator"
        })delimiter";

TEST(GroupAggFunctionTest, VectorBatch_MultiKeyVarcharAvgAggTest)
{
    nlohmann::json parsedJson = nlohmann::json::parse(MultiKeyVarcharAvgConfigStr);
    OperatorConfig opConfig(
        parsedJson["id"],
        parsedJson["name"],
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    GroupAggFunction* func = new GroupAggFunction(0l, opConfig.getDescription());
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        new KeyedProcessOperator(func, output, opConfig.getDescription());
    keyedOp->setup();

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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    omnistream::VectorBatch* vbatch = newVectorBatchMultiKeyVarcharAvg();
    keyedOp->processBatch(new StreamRecord(vbatch));

    omnistream::VectorBatch* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    int rowCount = resultBatch->GetRowCount();
    int colCount = resultBatch->GetVectorCount();
    EXPECT_EQ(rowCount, 6);
    EXPECT_EQ(colCount, 3);

    // Regression for setInt: the AVG output column (declared INTEGER) must be a real
    // int32 column (OMNI_INT), not an int64 (OMNI_LONG) one. Storing INT values in a
    // Vector<int64_t> makes Calc's CAST read the column at a 4-byte stride and corrupt
    // every other value.
    EXPECT_EQ(resultBatch->Get(2)->GetTypeId(), omniruntime::type::DataTypeId::OMNI_INT);

    std::unordered_map<std::string, int32_t> actual;
    for (int i = 0; i < rowCount; i++) {
        auto nameVec = reinterpret_cast<omniruntime::vec::Vector<
            omniruntime::vec::LargeStringContainer<std::string_view>>*>(resultBatch->Get(0));
        auto idVec = reinterpret_cast<omniruntime::vec::Vector<
            omniruntime::vec::LargeStringContainer<std::string_view>>*>(resultBatch->Get(1));
        std::string name(nameVec->GetValue(i));
        std::string id(idVec->GetValue(i));
        actual[name + "," + id] = resultBatch->GetValueAt<int32_t>(2, i);
    }

    EXPECT_EQ(actual.size(), 6u);
    EXPECT_EQ(actual["cat_a,p1"], 2); // (5+1+1)/3 = 2
    EXPECT_EQ(actual["cat_a,p2"], 3); // (3+3+3)/3 = 3
    EXPECT_EQ(actual["cat_a,p3"], 1);
    EXPECT_EQ(actual["cat_b,q1"], 3); // (4+2)/2 = 3
    EXPECT_EQ(actual["cat_b,q2"], 2);
    EXPECT_EQ(actual["cat_c,r1"], 1);
}

omnistream::VectorBatch* newVectorBatchForQ17WithSize(int batchSize = 10000)
{
    omnistream::VectorBatch* vbatch = new omnistream::VectorBatch(batchSize);

    auto col1 = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(batchSize);
    auto col0 = new omniruntime::vec::Vector<int64_t>(batchSize);
    auto col2 = new omniruntime::vec::Vector<bool>(batchSize);
    auto col3 = new omniruntime::vec::Vector<bool>(batchSize);
    auto col4 = new omniruntime::vec::Vector<bool>(batchSize);
    auto col5 = new omniruntime::vec::Vector<int64_t>(batchSize);

    std::array<int64_t, 10> bigint0 = {11, 22, 33, 11, 22, 33, 44, 55, 66, 11};
    std::array<bool, 10> bool2 = {false, false, true, false, false, true, false, false, true, false};
    std::array<bool, 10> bool3 = {true, false, false, false, true, false, false, false, false, true};
    std::array<bool, 10> bool4 = {false, true, false, true, false, false, true, true, false, false};
    std::array<int64_t, 10> bigint5 = {4, 5, 6, 7, 8, 9, 10, 2, 13, 11};

    for (int i = 0; i < batchSize; i++) {
        std::string_view dateStr = "a";
        col1->SetValue(i, dateStr);
        col0->SetValue(i, bigint0[i % 10]);
        col2->SetValue(i, bool2[i % 10]);
        col3->SetValue(i, bool3[i % 10]);
        col4->SetValue(i, bool4[i % 10]);
        col5->SetValue(i, bigint5[i % 10]);
        vbatch->setRowKind(i, RowKind::INSERT);
    }

    vbatch->Append(col0);
    vbatch->Append(col1);
    vbatch->Append(col2);
    vbatch->Append(col3);
    vbatch->Append(col4);
    vbatch->Append(col5);

    return vbatch;
}

TEST(GroupAggFunctionTest, VectorBatch_Q17Agg_Benchmark)
{
    nlohmann::json parsedJson = nlohmann::json::parse(Q17ConfigStr_agg);
    OperatorConfig opConfig(
        parsedJson["id"],
        parsedJson["name"],
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    GroupAggFunction* func = new GroupAggFunction(0l, opConfig.getDescription());
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp =
        new KeyedProcessOperator(func, output, opConfig.getDescription());
    keyedOp->setup();

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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();

    omnistream::VectorBatch* vbatch = newVectorBatchForQ17WithSize(10000);

    // ⏱️ Start timing first processBatch
    auto start1 = std::chrono::high_resolution_clock::now();
    auto record = new StreamRecord(vbatch);
    keyedOp->processBatch(record);
    auto end1 = std::chrono::high_resolution_clock::now();
    auto duration1 = std::chrono::duration_cast<std::chrono::microseconds>(end1 - start1).count();
    std::cout << "Time taken for processBatch round 1: " << duration1 << " microseconds" << std::endl;
}
