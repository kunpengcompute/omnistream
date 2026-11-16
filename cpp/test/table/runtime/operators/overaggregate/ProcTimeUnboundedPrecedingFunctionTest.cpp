/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Unit tests for the native OverAggregate v1 operator:
// ProcTimeUnboundedPrecedingFunction (processing-time unbounded OVER window,
// prefix aggregation). The harness mirrors GroupAggFunctionTest: build the
// operator through StreamOperatorFactory (so the factory branch is exercised),
// initialise keyed state, feed rows one by one, capture the emitted batches.

#include "table/runtime/operators/overaggregate/ProcTimeUnboundedPrecedingFunction.h"
#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "functions/Collector.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "core/typeutils/LongSerializer.h"
#include "table/data/binary/BinaryRowData.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/EmbeddedRocksDBStateBackend.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/api/operators/KeyedProcessOperator.h"

using namespace omnistream;
using namespace omnistream::datastream;

namespace {

template <typename T>
class DummyStreamPartitioner : public StreamPartitioner<T> {
public:
    int selectChannel(T* record) override
    {
        (void)record;
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

// Mock output that records every collected VectorBatch so the tests can
// assert on the emitted rows (the operator flushes one batch per fed row).
class CapturingRecordWriterOutput : public WatermarkGaugeExposingOutput {
public:
    std::vector<omnistream::VectorBatch*> capturedBatches;

    void collect(void* record) override
    {
        // TimestampedCollector::collect wraps each emitted value in a new
        // StreamRecord before handing it downstream; unwrap it here (and
        // take ownership of the wrapper record).
        auto* streamRecord = static_cast<StreamRecord*>(record);
        capturedBatches.push_back(reinterpret_cast<omnistream::VectorBatch*>(streamRecord->getValue()));
        delete streamRecord;
    }
    void close() override
    {
    }
    void emitWatermark(Watermark* watermark) override
    {
        (void)watermark;
    }
    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        (void)watermarkStatus;
    }
};

// Wraps the GroupAggFunctionTest bootstrap into one reusable harness.
class OverTestHarness {
public:
    OverTestHarness(const std::string& description, const std::string& stateBackend, int inputArity)
    {
        json parsedJson = json::parse(description);
        omnistream::OperatorConfig opConfig(
            "org.apache.flink.streaming.api.operators.KeyedProcessOperator",
            "OverAggregate",
            parsedJson["operators"][0]["inputTypes"],
            parsedJson["operators"][0]["outputTypes"],
            parsedJson["operators"][0]["description"]);

        recordWriteOutput = new CapturingRecordWriterOutput();

        StreamOperatorFactory streamOperatorFactory;
        // Note: assigns the member (no local declaration) so feed()/batch()
        // see the same operator instance.
        keyedOp = dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
            streamOperatorFactory.createOperatorAndCollector(opConfig, recordWriteOutput));
        // ASSERT_* macros expand to `return;` which is invalid in a
        // constructor; fail loudly instead.
        if (keyedOp == nullptr) {
            throw std::runtime_error("OverTestHarness: factory did not create a KeyedProcessOperator");
        }

        auto env2 = new omnistream::RuntimeEnvironmentV2();
        auto taskInfo = new TaskInformationPOD();
        taskInfo->setStateBackend(stateBackend);
        if (stateBackend == "EmbeddedRocksDBStateBackend") {
            // RocksDB needs an explicit store path (unique per harness instance,
            // cleaned on construction) exactly like RowTimeDeduplicateFunctionTest.
            static int rocksPathCounter = 0;
            const auto rocksPath = std::filesystem::temp_directory_path() /
                ("over-unbounded-proctime-rocksdb-" + std::to_string(rocksPathCounter++));
            std::filesystem::remove_all(rocksPath);
            taskInfo->setRocksdbStorePaths({rocksPath.string()});
        }
        {
            auto configPOD = taskInfo->getStreamConfigPOD();
            auto operatorDesc = configPOD.getOperatorDescription();
            operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
            configPOD.setOperatorDescription(operatorDesc);
            taskInfo->setStreamConfigPOD(configPOD);
        }
        TypeSerializer* ser = nullptr;
        StreamTaskStateInitializerImpl* initializer = nullptr;
        if (stateBackend == "EmbeddedRocksDBStateBackend") {
            // The single-argument StreamTaskStateInitializerImpl ctor only supports
            // the heap backend; RocksDB needs the backend object passed in
            // explicitly, together with a fully populated TaskStateManager.
            auto jobId = omnistream::JobIDPOD(1, 2);
            auto jobVertexId = omnistream::JobVertexID(3, 4);
            auto attemptId = omnistream::ExecutionAttemptIDPOD::randomId();
            auto* stateStore = new TaskLocalStateStore(jobId, jobVertexId, 0, nullptr);
            auto* responder = new omnistream::NoOpCheckpoingResponder();
            env2->SetTaskStateManager(
                std::make_shared<omnistream::TaskStateManager>(jobId, attemptId, stateStore, responder));
            env2->setTaskConfiguration(*taskInfo);
            auto* rocksBackend = new EmbeddedRocksDBStateBackend(*taskInfo);
            initializer = new StreamTaskStateInitializerImpl(rocksBackend, env2);
        } else {
            env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
            env2->setTaskConfiguration(*taskInfo);
            initializer = new StreamTaskStateInitializerImpl(env2);
        }
        auto* typeInfo = new std::vector<omnistream::RowField>();
        for (int i = 0; i < inputArity; ++i) {
            typeInfo->push_back(omnistream::RowField("col" + std::to_string(i), BasicLogicalType::BIGINT));
        }
        ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

        keyedOp->initializeState(initializer, ser);
        keyedOp->open();
    }

    ~OverTestHarness()
    {
        for (auto* batch : recordWriteOutput->capturedBatches) {
            delete batch;
        }
        delete recordWriteOutput;
    }

    void feed(const std::vector<int64_t>& cols, RowKind kind = RowKind::INSERT)
    {
        BinaryRowData* input = BinaryRowData::createBinaryRowDataWithMem(static_cast<int>(cols.size()));
        for (size_t i = 0; i < cols.size(); ++i) {
            input->setLong(static_cast<int>(i), cols[i]);
        }
        input->setRowKind(kind);
        StreamRecord* record = new StreamRecord(input);
        keyedOp->setKeyContextElement(record);
        keyedOp->processElement(record);
        delete record;
        delete input;
    }

    int batchCount() const
    {
        return static_cast<int>(recordWriteOutput->capturedBatches.size());
    }

    omnistream::VectorBatch* batch(int index)
    {
        return recordWriteOutput->capturedBatches[index];
    }

    int64_t longAt(int batchIndex, int col, int row)
    {
        auto* vector = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch(batchIndex)->Get(col));
        return vector->GetValue(row);
    }

    RowKind rowKindAt(int batchIndex, int row)
    {
        return batch(batchIndex)->getRowKind(row);
    }

    KeyedProcessOperator<RowData*, RowData*, RowData*>* op() const
    {
        return keyedOp;
    }

private:
    CapturingRecordWriterOutput* recordWriteOutput = nullptr;
    KeyedProcessOperator<RowData*, RowData*, RowData*>* keyedOp = nullptr;
};

std::string sumOverDescription(const std::vector<int>& grouping)
{
    std::string groupingStr = grouping.empty() ? "[]" : "[" + std::to_string(grouping[0]) + "]";
    return R"DELIM({"input_channels":[0],
        "operators":[{"description":{
                    "processFunction":"ProcTimeUnboundedPrecedingFunction",
                    "aggInfoList":{"accTypes":["BIGINT"],"aggValueTypes":["BIGINT"],
                        "aggregateCalls":[{"argIndexes":[1],"name":"SUM($1)","filterArg":-1}],"indexOfCountStar":-1},
                    "grouping":)DELIM" + groupingStr + R"DELIM(,
                    "distinctInfos":[],
                    "inputTypes":["BIGINT","BIGINT"],
                    "originDescription":"[3]:OverAggregate(partitionBy=[bidder], select=[bidder, price, SUM(price) OVER (PARTITION BY bidder ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)])",
                    "outputTypes":["BIGINT","BIGINT","BIGINT"]},
                    "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                    "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                    "name":"OverAggregate[3]",
                    "output":{"kind":"Row","type":[{"kind":"logical","type":"BIGINT"},{"kind":"logical","type":"BIGINT"},{"kind":"logical","type":"BIGINT"}]}}],
        "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";
}

} // namespace

// bidder=7, price = 10/20/30/40:
//   SUM OVER (PARTITION BY bidder) emits the prefix sums 10/30/60/100,
//   one appended input row per fed row, RowKind always INSERT.
TEST(ProcTimeUnboundedPrecedingFunctionTest, OneKeySumPrefixAgg)
{
    OverTestHarness h(sumOverDescription({0}), "HashMapStateBackend", 2);
    h.feed({7, 10});
    h.feed({7, 20});
    h.feed({7, 30});
    h.feed({7, 40});

    ASSERT_EQ(h.batchCount(), 4);
    const int64_t expected[] = {10, 30, 60, 100};
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(h.longAt(i, 0, 0), 7);
        EXPECT_EQ(h.longAt(i, 1, 0), (i + 1) * 10);
        EXPECT_EQ(h.longAt(i, 2, 0), expected[i]);
        EXPECT_EQ(h.rowKindAt(i, 0), RowKind::INSERT);
    }
}

// Two partition keys interleaved: each key keeps its own running sum.
TEST(ProcTimeUnboundedPrecedingFunctionTest, TwoKeysSumIsolation)
{
    OverTestHarness h(sumOverDescription({0}), "HashMapStateBackend", 2);
    h.feed({1, 10}); // key 1 -> 10
    h.feed({2, 5});  // key 2 -> 5
    h.feed({1, 20}); // key 1 -> 30
    h.feed({2, 7});  // key 2 -> 12
    h.feed({1, 30}); // key 1 -> 60

    ASSERT_EQ(h.batchCount(), 5);
    EXPECT_EQ(h.longAt(0, 2, 0), 10);
    EXPECT_EQ(h.longAt(1, 2, 0), 5);
    EXPECT_EQ(h.longAt(2, 2, 0), 30);
    EXPECT_EQ(h.longAt(3, 2, 0), 12);
    EXPECT_EQ(h.longAt(4, 2, 0), 60);
}

// COUNT(*) OVER (): empty grouping = single global partition, no argument.
TEST(ProcTimeUnboundedPrecedingFunctionTest, CountStarGlobalPartition)
{
    const std::string description = R"DELIM({"input_channels":[0],
        "operators":[{"description":{
                    "processFunction":"ProcTimeUnboundedPrecedingFunction",
                    "aggInfoList":{"accTypes":["BIGINT"],"aggValueTypes":["BIGINT"],
                        "aggregateCalls":[{"argIndexes":[],"name":"COUNT(*)","filterArg":-1}],"indexOfCountStar":-1},
                    "grouping":[],
                    "distinctInfos":[],
                    "inputTypes":["BIGINT","BIGINT"],
                    "originDescription":"[3]:OverAggregate(select=[bidder, price, COUNT(*) OVER ()])",
                    "outputTypes":["BIGINT","BIGINT","BIGINT"]},
                    "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                    "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                    "name":"OverAggregate[3]",
                    "output":{"kind":"Row","type":[{"kind":"logical","type":"BIGINT"},{"kind":"logical","type":"BIGINT"},{"kind":"logical","type":"BIGINT"}]}}],
        "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    OverTestHarness h(description, "HashMapStateBackend", 2);
    h.feed({42, 1});
    h.feed({42, 2});
    h.feed({99, 3});

    ASSERT_EQ(h.batchCount(), 3);
    EXPECT_EQ(h.longAt(0, 2, 0), 1);
    EXPECT_EQ(h.longAt(1, 2, 0), 2);
    EXPECT_EQ(h.longAt(2, 2, 0), 3);
}

// MAX OVER keeps the current maximum, never decreasing.
TEST(ProcTimeUnboundedPrecedingFunctionTest, MaxKeepsCurrentMaximum)
{
    const std::string description = R"DELIM({"input_channels":[0],
        "operators":[{"description":{
                    "processFunction":"ProcTimeUnboundedPrecedingFunction",
                    "aggInfoList":{"accTypes":["BIGINT"],"aggValueTypes":["BIGINT"],
                        "aggregateCalls":[{"argIndexes":[1],"name":"MAX($1)","filterArg":-1}],"indexOfCountStar":-1},
                    "grouping":[0],
                    "distinctInfos":[],
                    "inputTypes":["BIGINT","BIGINT"],
                    "originDescription":"[3]:OverAggregate(partitionBy=[bidder], select=[bidder, price, MAX(price) OVER (PARTITION BY bidder ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)])",
                    "outputTypes":["BIGINT","BIGINT","BIGINT"]},
                    "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                    "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                    "name":"OverAggregate[3]",
                    "output":{"kind":"Row","type":[{"kind":"logical","type":"BIGINT"},{"kind":"logical","type":"BIGINT"},{"kind":"logical","type":"BIGINT"}]}}],
        "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    OverTestHarness h(description, "HashMapStateBackend", 2);
    h.feed({1, 30});
    h.feed({1, 10});
    h.feed({1, 50});
    h.feed({1, 20});

    ASSERT_EQ(h.batchCount(), 4);
    EXPECT_EQ(h.longAt(0, 2, 0), 30);
    EXPECT_EQ(h.longAt(1, 2, 0), 30);
    EXPECT_EQ(h.longAt(2, 2, 0), 50);
    EXPECT_EQ(h.longAt(3, 2, 0), 50);
}

// Two aggregates in one frame: SUM and COUNT share the accumulator row but
// occupy separate slots; the output carries one value column per aggregate.
TEST(ProcTimeUnboundedPrecedingFunctionTest, MultipleAggregates)
{
    const std::string description = R"DELIM({"input_channels":[0],
        "operators":[{"description":{
                    "processFunction":"ProcTimeUnboundedPrecedingFunction",
                    "aggInfoList":{"accTypes":["BIGINT","BIGINT"],"aggValueTypes":["BIGINT","BIGINT"],
                        "aggregateCalls":[
                            {"argIndexes":[1],"name":"SUM($1)","filterArg":-1},
                            {"argIndexes":[1],"name":"COUNT($1)","filterArg":-1}],"indexOfCountStar":-1},
                    "grouping":[0],
                    "distinctInfos":[],
                    "inputTypes":["BIGINT","BIGINT"],
                    "originDescription":"[3]:OverAggregate(partitionBy=[bidder], select=[bidder, price, SUM(price), COUNT(price)])",
                    "outputTypes":["BIGINT","BIGINT","BIGINT","BIGINT"]},
                    "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
                    "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                    "name":"OverAggregate[3]",
                    "output":{"kind":"Row","type":[{"kind":"logical","type":"BIGINT"},{"kind":"logical","type":"BIGINT"},{"kind":"logical","type":"BIGINT"},{"kind":"logical","type":"BIGINT"}]}}],
        "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    OverTestHarness h(description, "HashMapStateBackend", 2);
    h.feed({1, 10});
    h.feed({1, 20});
    h.feed({1, 30});

    ASSERT_EQ(h.batchCount(), 3);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(h.longAt(i, 2, 0), (i + 1) * (i + 2) * 5); // prefix sum
        EXPECT_EQ(h.longAt(i, 3, 0), i + 1); // running count
    }
}

// Java-alignment contract: the Java ProcTimeUnboundedPrecedingFunction
// accumulates every row unconditionally (no RowKind branch) and always
// emits INSERT. The native operator must behave byte-identically.
TEST(ProcTimeUnboundedPrecedingFunctionTest, RetractInputStillAccumulates)
{
    OverTestHarness h(sumOverDescription({0}), "HashMapStateBackend", 2);
    h.feed({1, 10}, RowKind::INSERT);
    h.feed({1, 20}, RowKind::UPDATE_BEFORE); // treated as a plain row, like Java
    h.feed({1, 30}, RowKind::DELETE);

    ASSERT_EQ(h.batchCount(), 3);
    EXPECT_EQ(h.longAt(0, 2, 0), 10);
    EXPECT_EQ(h.longAt(1, 2, 0), 30);
    EXPECT_EQ(h.longAt(2, 2, 0), 60);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(h.rowKindAt(i, 0), RowKind::INSERT);
    }
}

// Same prefix aggregation on the RocksDB backend must give the same results.
TEST(ProcTimeUnboundedPrecedingFunctionTest, DualBackendConsistencyRocksDB)
{
    OverTestHarness h(sumOverDescription({0}), "EmbeddedRocksDBStateBackend", 2);
    h.feed({7, 10});
    h.feed({7, 20});
    h.feed({7, 30});
    h.feed({7, 40});

    ASSERT_EQ(h.batchCount(), 4);
    const int64_t expected[] = {10, 30, 60, 100};
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(h.longAt(i, 2, 0), expected[i]);
    }
}

// Last line of defence: a non-whitelisted aggregate (AVG) must be rejected at
// parse time. Production never reaches this because the OmniAdaptor Validate
// strategy keeps such plans on the Java path.
TEST(ProcTimeUnboundedPrecedingFunctionTest, UnsupportedAggregateThrows)
{
    const std::string description = R"DELIM({
        "processFunction":"ProcTimeUnboundedPrecedingFunction",
        "grouping":[0],
        "inputTypes":["BIGINT","BIGINT"],
        "outputTypes":["BIGINT","BIGINT","DOUBLE"],
        "aggInfoList":{
            "accTypes":["BIGINT","BIGINT"],
            "aggValueTypes":["DOUBLE"],
            "aggregateCalls":[{"argIndexes":[1],"name":"AVG($1)","filterArg":-1}]
        }})DELIM";
    const nlohmann::json config = nlohmann::json::parse(description);
    EXPECT_THROW(
        ProcTimeUnboundedPrecedingFunction func(0L, config), std::runtime_error);
}

// Defense in depth: a description whose processFunction does not match this
// class is rejected even if the factory somehow routed it here.
TEST(ProcTimeUnboundedPrecedingFunctionTest, ProcessFunctionMismatchThrows)
{
    const std::string description = R"DELIM({
        "processFunction":"ProcTimeRowsBoundedPrecedingFunction",
        "grouping":[0],
        "inputTypes":["BIGINT","BIGINT"],
        "outputTypes":["BIGINT","BIGINT","BIGINT"],
        "aggInfoList":{
            "accTypes":["BIGINT"],
            "aggValueTypes":["BIGINT"],
            "aggregateCalls":[{"argIndexes":[1],"name":"SUM($1)","filterArg":-1}]
        }})DELIM";
    const nlohmann::json config = nlohmann::json::parse(description);
    EXPECT_THROW(
        ProcTimeUnboundedPrecedingFunction func(0L, config), std::runtime_error);
}
