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
#include <memory>
#include <nlohmann/json.hpp>
#include <taskmanager/OmniRuntimeEnvironment.h>

#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "streaming/api/operators/StreamOperatorFactory.h"

using json = nlohmann::json;
using namespace omnistream;

std::string description3 = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"globalAccTypes": ["BIGINT"],"globalAggValueTypes":["BIGINT"],"globalAggregateCalls":[{"aggregationFunction":"LongMaxAggFunction","argIndexes":[0],"consumeRetraction":"false","name":"MAX($0)"}],"indexOfCountStar":-1},
                                        "grouping":[],
                                        "sliceEndIndex": 2,
                                        "timeAttributeIndex": 2,
                                        "window": "TUMBLE(size=[10 s], offset=[0 s])",
                                        "inputTypes":["BIGINT","BIGINT","BIGINT"],
                                        "originDescription":"[3]:GroupAggregate(groupBy=[age], select=[age, SUM(id) AS EXPR$1])",
                                        "outputTypes":["BIGINT","BIGINT","BIGINT"],
                                        "distinctInfos":[]},
                                        "id":"org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

std::string hopdescription = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"globalAccTypes": ["BIGINT"],"globalAggValueTypes":["BIGINT"],"globalAggregateCalls":[{"aggregationFunction":"LongSumAggFunction","argIndexes":[0],"consumeRetraction":"false","name":"SUM($0)"}],"indexOfCountStar":-1},
                                        "grouping":[1],
                                        "timeAttributeIndex": 2,
                                        "window": "HOP(size=[10 s], slide=[2 s])",
                                        "inputTypes":["BIGINT","BIGINT","BIGINT"],
                                        "originDescription":"[3]:GroupAggregate(groupBy=[age], select=[age, SUM(id) AS EXPR$1])",
                                        "outputTypes":["BIGINT","BIGINT","BIGINT"],
                                        "distinctInfos":[]},
                                        "id":"org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

omnistream::VectorBatch* newVectorBatchOneKeyOneValue2()
{
    auto* vbatch = new omnistream::VectorBatch(5);
    std::vector<int64_t> maxColumn = {1, 2, 1, 1, 3};
    std::vector<int64_t> column2 = {3, 4, 5, 6, 7};
    std::vector<int64_t> time = {1731944410000, 1731944410000, 1731944410000, 1731944420000, 1731944420000};

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, maxColumn.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, column2.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, time.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
    return vbatch;
}

TEST(GlobalWindowAggTest, DISABLED_OnTimerTest)
{
    omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneValue2();
    json parsedJson = json::parse(description3);
    std::cout << "================step 0===================" << std::endl;
    nlohmann::json windowing = parsedJson["operators"][0]["description"];
    std::cout << "================step 01===================" << std::endl;
    auto* output = new BatchOutputTest();
    std::cout << "================step 02===================" << std::endl;
    auto processor = std::make_unique<AbstractWindowAggProcessor>(windowing, output);
    std::cout << "================step 03===================" << std::endl;
    SlicingWindowOperator<std::shared_ptr<RowData>, int64_t>* operators =
        new SlicingWindowOperator<std::shared_ptr<RowData>, int64_t>(std::move(processor), windowing);
    std::cout << "================step 04===================" << std::endl;
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
    operators->setup();
    operators->initializeState(initializer, new LongSerializer());

    std::cout << "================step 1===================" << std::endl;
    operators->open();
    std::cout << "================step 100===================" << std::endl;
    operators->processBatch(vbatch);
    auto watermark = Watermark(-1);
    operators->ProcessWatermark(&watermark);
    BinaryRowData* binaryRowData = new BinaryRowData(0); // group为空时, 需要设置为new BinaryRowData(0)
    auto* timer = new TimerHeapInternalTimer<std::shared_ptr<RowData>, int64_t>(
        -1, std::shared_ptr<RowData>(binaryRowData), 1731944420000);
    operators->onTimer(timer);
    auto* batchOutput = dynamic_cast<BatchOutputTest*>(operators->getOutput());
    auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(batchOutput->getVectorBatch());
    // print VectorBatch
    int rowCount = resultBatch->GetRowCount();
    int colCount = resultBatch->GetVectorCount();
    ASSERT_EQ(rowCount, 1);
    ASSERT_EQ(colCount, 3);
    ASSERT_EQ(resultBatch->GetValueAt<int64_t>(0, 0), 3);
    ASSERT_EQ(resultBatch->GetValueAt<int64_t>(1, 0), 1731944410000);
    ASSERT_EQ(resultBatch->GetValueAt<int64_t>(2, 0), 1731944420000);
    for (int i = 0; i < rowCount; i++) {
        for (int j = 0; j < colCount; j++) {
            long result = resultBatch->GetValueAt<int64_t>(j, i);
            std::cout << result;
            std::cout << " ";
        }
        std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
    }

    std::cout << "Global WindowAggTest OnTimerTest" << std::endl;
}

TEST(GlobalWindowAggTest, DISABLED_TUMBLETest)
{
    omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneValue2();
    json parsedJson = json::parse(description3);
    std::cout << "================step 0===================" << std::endl;
    nlohmann::json windowing = parsedJson["operators"][0]["description"];
    std::cout << "================step 01===================" << std::endl;
    auto* output = new BatchOutputTest();
    std::cout << "================step 02===================" << std::endl;
    auto processor = std::make_unique<AbstractWindowAggProcessor>(windowing, output);
    std::cout << "================step 03===================" << std::endl;
    SlicingWindowOperator<std::shared_ptr<RowData>, int64_t>* operators =
        new SlicingWindowOperator<std::shared_ptr<RowData>, int64_t>(std::move(processor), windowing);
    std::cout << "================step 04===================" << std::endl;
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
    operators->setup();
    operators->initializeState(initializer, new LongSerializer());

    std::cout << "================step 1===================" << std::endl;
    operators->open();
    std::cout << "================step 100===================" << std::endl;
    operators->processBatch(vbatch);
    auto watermark = Watermark(-1);
    operators->ProcessWatermark(&watermark);
    auto* batchOutput = dynamic_cast<BatchOutputTest*>(operators->getOutput());
    auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(batchOutput->getVectorBatch());
    // print VectorBatch
    int rowCount = resultBatch->GetRowCount();
    int colCount = resultBatch->GetVectorCount();
    for (int i = 0; i < rowCount; i++) {
        for (int j = 0; j < colCount; j++) {
            long result = resultBatch->GetValueAt<int64_t>(j, i);
            std::cout << result;
            std::cout << " ";
        }
        std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
    }

    std::cout << "Global WindowAggTest MaxAggTest" << std::endl;
}

TEST(GlobalWindowAggTest, DISABLED_HOPTest1)
{
    omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneValue2();

    // Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
    json parsedJson = json::parse(hopdescription);
    omnistream::OperatorConfig opConfig(
        uniqueName,                  // uniqueName:
        "GlobalWindowAgg_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);
    auto* output = new BatchOutputTest();
    auto* slicingWindowOperator = dynamic_cast<SlicingWindowOperator<std::shared_ptr<RowData>, int64_t>*>(
        StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
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
    slicingWindowOperator->setup();
    slicingWindowOperator->initializeState(initializer, new LongSerializer());

    std::cout << "================step 1===================" << std::endl;
    slicingWindowOperator->open();
    std::cout << "================step 4 in global window agg===================" << std::endl;
    slicingWindowOperator->processBatch(vbatch);
    auto watermark = Watermark(-1);
    slicingWindowOperator->ProcessWatermark(&watermark);
    //    BatchOutputTest* batchOutput = dynamic_cast<BatchOutputTest*>(slicingWindowOperator->getOutput());

    BatchOutputTest* batchOutput = output;
    auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(batchOutput->getVectorBatch());
    // print VectorBatch
    int rowCount = resultBatch->GetRowCount();
    int colCount = resultBatch->GetVectorCount();
    for (int i = 0; i < rowCount; i++) {
        for (int j = 0; j < colCount; j++) {
            long result = resultBatch->GetValueAt<int64_t>(j, i);
            std::cout << result;
            std::cout << " ";
        }
        std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
    }

    slicingWindowOperator->processBatch(vbatch);
    std::cout << "================step 5===================" << std::endl;
    slicingWindowOperator->ProcessWatermark(&watermark);
    std::cout << "================step 6===================" << std::endl;
    //    batchOutput = dynamic_cast<BatchOutputTest*>(slicingWindowOperator->getOutput());
    resultBatch = reinterpret_cast<omnistream::VectorBatch*>(batchOutput->getVectorBatch());
    // print VectorBatch
    rowCount = resultBatch->GetRowCount();
    colCount = resultBatch->GetVectorCount();
    for (int i = 0; i < rowCount; i++) {
        for (int j = 0; j < colCount; j++) {
            long result = resultBatch->GetValueAt<int64_t>(j, i);
            std::cout << result;
            std::cout << " ";
        }
        std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
    }
    std::cout << "Global WindowAggTest  SumAggTest" << std::endl;
}
