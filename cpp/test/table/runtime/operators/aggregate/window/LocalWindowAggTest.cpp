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
#include <nlohmann/json.hpp>
#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "core/graph/OperatorConfig.h"
#include "table/runtime/operators/window/LocalSlicingWindowAggOperator.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "streaming/runtime/tasks/OperatorChain.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "table/typeutils/RowDataSerializer.h"

using json = nlohmann::json;
using namespace omnistream;

std::string sliceDescription = R"DELIM({"input_channels":[0],
                            "operators":[{"description":{
                                        "aggInfoList":{"accTypes": ["BIGINT"],"aggValueTypes":["BIGINT"],"aggregateCalls":[{"aggregationFunction":"LongSumAggFunction","argIndexes":[1],"consumeRetraction":"false","filterArg":-1,"name":"SUM($0)"}],"indexOfCountStar":-1},
                                        "grouping":[0],
                                        "timeAttributeIndex": 2,
                                        "window": "TUMBLE(size=[10 s], offset=[0 s])",
										"windowSize": 10000,
                                        "inputTypes":["BIGINT","BIGINT","BIGINT"],
                                        "originDescription":"[3]:GroupAggregate(groupBy=[age], select=[age, SUM(id) AS EXPR$1])",
                                        "outputTypes":["BIGINT","BIGINT","BIGINT"],
                                        "distinctInfos":[]},
                                        "id":"org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator",
                                        "inputs":[{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}],
                                        "name":"GroupAggregate[3]",
                                        "output":{"kind":"Row","type":[{"isNull":true,"kind":"logical","type":"BIGINT"},{"isNull":true,"kind":"logical","type":"BIGINT"}]}}],
                            "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

std::string nexmarkQ7Description = R"DELIM({
	"partition": {
		"partitionName": "none",
		"channelNumber": 1
	},
	"operators": [{
		"output": {
			"kind": "Row",
			"type": [{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			}]
		},
		"inputs": [{
			"kind": "Row",
			"type": [{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"precision": 3,
				"type": "TIMESTAMP",
				"timestampKind": 1
			}]
		}],
		"name": "LocalWindowAggregate(window=[TUMBLE(time_col=[dateTime], size=[10 s])], select=[MAX(price) AS max$0, slice_end('w$) AS $slice_end])",
		"description": {
			"timeAttributeType": "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
			"inputTypes": ["BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"],
			"aggInfoList": {
				"aggregateCalls": [{
					"name": "MAX($0)",
					"filterArg": -1,
					"argIndexes": [0],
					"aggregationFunction": "LongMaxAggFunction",
					"consumeRetraction": "false"
				}],
				"indexOfCountStar": -1,
				"accTypes": ["BIGINT"],
				"aggValueTypes": ["BIGINT"]
			},
			"outputTypes": ["BIGINT", "BIGINT"],
			"window": "TUMBLE(size=[10 s])",
			"windowSize": 10000,
			"timeAttributeIndex": 1,
			"distinctInfos": [],
			"grouping": [],
			"originDescription": null
		},
		"id": "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator"
	}]
})DELIM";

std::string Operator_Chain_Local_Window = R"DELIM({
            "name": "",
            "id": "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator",
            "description": {
                "aggInfoList": {
                    "accTypes": ["BIGINT"],
                    "aggValueTypes": ["BIGINT"],
                    "aggregateCalls": [{
                        "aggregationFunction": "LongMaxAggFunction",
                        "argIndexes": [0],
                        "consumeRetraction": "false",
                        "filterArg": -1,
                        "name": "MAX($0)"
                    }],
                    "indexOfCountStar": -1
                },
                "grouping": [],
                "timeAttributeIndex": 1,
                "window": "TUMBLE(size=[10 s], offset=[0 s])",
				"windowSize": 10000,
                "inputTypes": ["BIGINT", "BIGINT"],
                "originDescription": "[3]:LocalWindowAggregate(window=[TUMBLE(time_col=[dateTime], size=[10 s])], select=[MAX(price) AS max$0, slice_end('w$) AS $slice_end])",
                "outputTypes": ["BIGINT", "BIGINT"],
                "distinctInfos": []
            }
        })DELIM";

std::string Operator_Chain_Local_Window_SUM = R"DELIM({
            "name": "",
            "id": "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator",
            "description": {
                "aggInfoList": {
                    "accTypes": ["BIGINT"],
                    "aggValueTypes": ["BIGINT"],
                    "aggregateCalls": [{
                        "aggregationFunction": "LongSumAggFunction",
                        "argIndexes": [0],
                        "consumeRetraction": "false",
                        "filterArg": -1,
                        "name": "SUM($0)"
                    }],
                    "indexOfCountStar": -1
                },
                "grouping": [],
                "timeAttributeIndex": 1,
                "window": "TUMBLE(size=[10 s], offset=[0 s])",
				"windowSize": 10000,
                "inputTypes": ["BIGINT", "BIGINT"],
                "originDescription": "[3]:LocalWindowAggregate(window=[TUMBLE(time_col=[dateTime], size=[10 s])], select=[MAX(price) AS max$0, slice_end('w$) AS $slice_end])",
                "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
                "distinctInfos": []
            }
        })DELIM";

std::string Operator_Chain_GLOBAL_Window_SUM = R"DELIM({
            "name": "",
            "id": "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator",
            "description": {
                "aggInfoList": {
                    "accTypes": ["BIGINT"],
                    "aggValueTypes": ["BIGINT"],
                    "aggregateCalls": [{
                        "aggregationFunction": "LongSumAggFunction",
                        "argIndexes": [1],
                        "consumeRetraction": "false",
                        "filterArg": -1,
                        "name": "SUM($0)"
                    }],
                    "indexOfCountStar": -1
                },
                "grouping": [],
                "timeAttributeIndex": 2,
                "window": "TUMBLE(size=[10 s], offset=[0 s])",
				"windowSize": 10000,
                "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
                "originDescription": "[3]:GlobalWindowAggregate(window=[TUMBLE(time_col=[dateTime], size=[10 s])], select=[MAX(price) AS max$0, slice_end('w$) AS $slice_end])",
                "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
                "distinctInfos": []
            }
        })DELIM";

std::string Operator_Chain_Calc =
    R"delimiter({
        "name" : "Calc(select=[price, time])",
        "description":{
            "originDescription":null,
            "inputTypes":["BIGINT","BIGINT"],
            "outputTypes":["BIGINT"],
            "indices":[
                {"exprType":"FIELD_REFERENCE","dataType":2,"colVal":1}
            ],
            "condition":null
        },
        "id":"StreamExecCalc"
    })delimiter";

omnistream::VectorBatch* newVectorBatchOneKeyOneValue1()
{
    auto* vbatch = new omnistream::VectorBatch(5);
    std::vector<int64_t> key = {1, 2, 1, 1, 3};
    std::vector<int64_t> value = {3, 4, 5, 6, 7};
    std::vector<int64_t> time = {1009, 1007, 1003, 2002, 2004};

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, key.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, value.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, time.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
    return vbatch;
}

omnistream::VectorBatch* nexmarkQ7Input()
{
    auto* vbatch = new omnistream::VectorBatch(5);
    std::vector<int64_t> price = {1000, 2000, 3005, 599, 2597};
    std::vector<int64_t> time = {1009, 1007, 1003, 2002, 2004}; // 待修改为时间类型

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, price.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, time.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
    return vbatch;
}

std::string Nexmark_Q5_Local_Window_Agg_1 =
    R"delimiter({
	"partition": {
		"partitionName": "hash",
		"channelNumber": 1,
		"hashFields": [{
			"fieldIndex": 0,
			"fieldName": "f0",
			"fieldTypeName": "BIGINT"
		}]
	},
	"operators": [{
		"output": {
			"kind": "Row",
			"type": [{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			}]
		},
		"inputs": [{
			"kind": "Row",
			"type": [{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"precision": 3,
				"type": "TIMESTAMP",
				"timestampKind": 1
			}]
		}],
		"name": "LocalWindowAggregate(groupBy=[auction], window=[HOP(time_col=[dateTime], size=[10 s], slide=[2 s])], select=[auction, COUNT(*) AS count1$0, slice_end('w$) AS $slice_end])",
		"description": {
			"timeAttributeType": "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
			"inputTypes": ["BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"],
			"aggInfoList": {
				"aggregateCalls": [{
					"name": "COUNT()",
					"filterArg": -1,
					"argIndexes": [],
					"aggregationFunction": "Count1AggFunction",
					"consumeRetraction": "false"
				}],
				"indexOfCountStar": 0,
				"accTypes": ["BIGINT"],
				"aggValueTypes": ["BIGINT"]
			},
			"timeAttributeIndex": 1,
			"outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
			"window": "HOP(size=[10 s], slide=[2 s])",
			"windowSize": 10000,
			"windowSlide": 2000,
			"distinctInfos": [],
			"grouping": [0],
			"originDescription": null
		},
		"id": "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator"
	}]
})delimiter";

std::string Nexmark_Q5_Local_Window_Agg_2 =
    R"delimiter({
	"partition": {
		"partitionName": "hash",
		"channelNumber": 1,
		"hashFields": [{
			"fieldIndex": 0,
			"fieldName": "f0",
			"fieldTypeName": "BIGINT"
		}]
	},
	"operators": [{
		"output": {
			"kind": "Row",
			"type": [{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			}]
		},
		"inputs": [{
			"kind": "Row",
			"type": [{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"precision": 3,
				"type": "TIMESTAMP",
				"timestampKind": 1
			}]
		}],
		"name": "LocalWindowAggregate(groupBy=[auction], window=[HOP(time_col=[dateTime], size=[10 s], slide=[2 s])], select=[auction, COUNT(*) AS count1$0, slice_end('w$) AS $slice_end])",
		"description": {
            "originDescription": null,
            "inputTypes": ["BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"],
            "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
            "grouping": [],
            "aggInfoList": {
                "accTypes": ["BIGINT", "BIGINT"],
                "aggValueTypes": ["BIGINT", "BIGINT"],
                "aggregateCalls": [{
                    "name": "MAX($2)",
                    "aggregationFunction": "LongMaxAggFunction",
                    "argIndexes": [2],
                    "consumeRetraction": "false",
                    "filterArg": -1
                },
                {
                    "name": "COUNT()",
                    "aggregationFunction": "Count1AggFunction",
                    "argIndexes": [],
                    "consumeRetraction": "false",
                    "filterArg": -1
                }],
                "indexOfCountStar": 1
            },
            "distinctInfos": [],
            "timeAttributeIndex": 2147483647,
            "windowSize": 10000,
            "windowSlide": 2000,
            "windowEndIndex": 1,
            "window": "HOP(size=[10 s], slide=[2 s])",
            "timeAttributeType": "TIMESTAMP_WITHOUT_TIME_ZONE(3)"
        },
		"id": "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator"
	}]
})delimiter";

omnistream::VectorBatch* nexmarkQ5Input2()
{
    auto* vbatch = new omnistream::VectorBatch(5);
    std::vector<int64_t> timeStart = {100, 100, 200, 200, 200}; // 待修改为时间类型
    std::vector<int64_t> timeEnd = {110, 110, 210, 210, 210};   // 待修改为时间类型
    std::vector<int64_t> num = {1, 2, 3, 1, 5};

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, timeStart.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, timeEnd.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, num.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
    return vbatch;
}

omnistream::VectorBatch* nexmarkQ5Input1()
{
    auto* vbatch = new omnistream::VectorBatch(5);
    std::vector<int64_t> auction = {1, 5, 20, 20, 20};
    std::vector<int64_t> datetime = {10, 10, 20, 21, 20}; // 待修改为时间类型
                                                          //    std::vector<TimestampData*> datetime;
    //    datetime.push_back(TimestampData::fromEpochMillis(10L, 5));
    //    datetime.push_back(TimestampData::fromEpochMillis(10L, 5));
    //    datetime.push_back(TimestampData::fromEpochMillis(20L, 5));
    //    datetime.push_back(TimestampData::fromEpochMillis(20L, 5));
    //    datetime.push_back(TimestampData::fromEpochMillis(21L, 5));

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, auction.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, datetime.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
    return vbatch;
}

std::string Nexmark_Q8_Local_Window_Agg_1 =
    R"delimiter({
	"partition": {
		"partitionName": "hash",
		"channelNumber": 1,
		"hashFields": [{
			"fieldIndex": 0,
			"fieldName": "f0",
			"fieldTypeName": "BIGINT"
		},
		{
			"fieldIndex": 1,
			"fieldName": "f1",
			"fieldTypeName": "VARCHAR"
		}]
	},
	"operators": [{
		"output": {
			"kind": "Row",
			"type": [{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"length": 2147483647,
				"type": "VARCHAR"
			},
			{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			}]
		},
		"inputs": [{
			"kind": "Row",
			"type": [{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"length": 2147483647,
				"type": "VARCHAR"
			},
			{
				"kind": "logical",
				"isNull": true,
				"precision": 3,
				"type": "TIMESTAMP",
				"timestampKind": 1
			}]
		}],
		"name": "LocalWindowAggregate(groupBy=[id, name], window=[TUMBLE(time_col=[dateTime], size=[10 s])], select=[id, name, slice_end('w$) AS $slice_end])",
		"description": {
			"timeAttributeType": "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
			"inputTypes": ["BIGINT", "VARCHAR(2147483647)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"],
			"aggInfoList": {
				"aggregateCalls": [],
				"indexOfCountStar": -1,
				"accTypes": [],
				"aggValueTypes": []
			},
			"outputTypes": ["BIGINT", "VARCHAR(2147483647)", "BIGINT"],
			"window": "TUMBLE(size=[10 s])",
			"windowSize": 10000,
			"timeAttributeIndex": 2,
			"distinctInfos": [],
			"grouping": [0, 1],
			"originDescription": null
		},
		"id": "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator"
	}]
})delimiter";

omnistream::VectorBatch* nexmarkQ8Input1()
{
    auto* vbatch = new omnistream::VectorBatch(5);
    std::vector<int64_t> id = {1, 1, 1, 15, 1};
    std::vector<std::string> name = {"A", "A", "B", "C", "D"}; // 待修改为string类型
                                                               //    std::vector<int64_t> name = {10, 10, 30, 40, 10};
    std::vector<int64_t> time = {1000, 1000, 1000, 1000, 10000};

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, id.data()));
    //    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, name.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVarcharVector(name.data(), 5));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, time.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
    return vbatch;
}

TEST(LocalWindowAggTest, SumAggTest)
{
    // Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator";
    json parsedJson = json::parse(sliceDescription);
    omnistream::OperatorConfig opConfig(
        uniqueName,                 // uniqueName:
        "LocalWindowAgg_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    auto* output = new BatchOutputTest();
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
    std::vector<omnistream::RowField> typeInfo{omnistream::RowField("col0", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto* windowAggOperator = dynamic_cast<LocalSlicingWindowAggOperator*>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    windowAggOperator->setup();
    windowAggOperator->initializeState(initializer, ser);
    windowAggOperator->open();
    omnistream::VectorBatch* vBatch = newVectorBatchOneKeyOneValue1();
    auto* streamRecord = new StreamRecord(vBatch);
    windowAggOperator->processBatch(streamRecord);
    auto watermark = Watermark(1000000);
    windowAggOperator->ProcessWatermark(&watermark);
    auto* batchOutput = dynamic_cast<BatchOutputTest*>(windowAggOperator->getOutput());
    std::cout << "=========== print result ==========" << std::endl;
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
    std::cout << "LocalWindowAggTest  SumAggTest" << std::endl;
}

TEST(LocalWindowAggTest, FlushesPendingBundleBeforeCheckpointBarrier)
{
    const json parsedJson = json::parse(sliceDescription);
    auto output = std::make_unique<OutputTestVectorBatch>();
    auto windowAggOperator =
        std::make_unique<LocalSlicingWindowAggOperator>(parsedJson["operators"][0]["description"], output.get());
    windowAggOperator->open();

    auto* streamRecord = new StreamRecord(newVectorBatchOneKeyOneValue1());
    windowAggOperator->processBatch(streamRecord);
    ASSERT_TRUE(output->getAll().empty());

    windowAggOperator->PrepareSnapshotPreBarrier(101);
    ASSERT_EQ(output->getAll().size(), 1);
    EXPECT_GT(output->getAll().front()->GetRowCount(), 0);

    // A repeated pre-barrier callback must not emit the already flushed bundle again.
    windowAggOperator->PrepareSnapshotPreBarrier(101);
    EXPECT_EQ(output->getAll().size(), 1);

    windowAggOperator->close();
}

TEST(LocalWindowAggTest, SlicingTest)
{
    json parsedJson = json::parse(sliceDescription);
    std::cout << "SlicingTest start" << std::endl;
    SliceAssigner* sliceAssigner = AssignerAtt::createSliceAssigner(parsedJson["operators"][0]["description"]);
    std::cout << "createSliceAssigner success" << std::endl;
    omnistream::VectorBatch* vBatch = newVectorBatchOneKeyOneValue1();
    int64_t sliceEndArr[vBatch->GetRowCount()];
    for (int i = 0; i < vBatch->GetRowCount(); i++) {
        int64_t sliceEnd = sliceAssigner->assignSliceEnd(vBatch, i, std::make_shared<ClockService>().get());
        sliceEndArr[i] = sliceEnd;
    }
    for (const auto& item : sliceEndArr) {
        std::cout << item << " " << std::endl;
    }
}

TEST(LocalWindowAggTest, NexmarkQ5Test1)
{
    // Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator";
    json parsedJson = json::parse(Nexmark_Q5_Local_Window_Agg_1);
    omnistream::OperatorConfig opConfig(
        uniqueName,                 // uniqueName:
        "LocalWindowAgg_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    auto* output = new BatchOutputTest();
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
    std::vector<omnistream::RowField> typeInfo{omnistream::RowField("col0", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto* windowAggOperator = dynamic_cast<LocalSlicingWindowAggOperator*>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    windowAggOperator->setup();
    windowAggOperator->initializeState(initializer, ser);
    windowAggOperator->open();
    std::cout << "NexmarkQ5Test2" << std::endl;
    omnistream::VectorBatch* vBatch = nexmarkQ5Input1();
    auto* streamRecord = new StreamRecord(vBatch);
    windowAggOperator->processBatch(streamRecord);
    std::cout << "NexmarkQ5Test3" << std::endl;
    auto watermark = Watermark(1000000);
    windowAggOperator->ProcessWatermark(&watermark);
    std::cout << "NexmarkQ5Test4" << std::endl;

    auto* batchOutput = dynamic_cast<BatchOutputTest*>(windowAggOperator->getOutput());
    std::cout << "=========== print result ==========" << std::endl;
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
    std::cout << "LocalWindowAggTest  NexmarkQ5Test1" << std::endl;
}

// 待确认NexmarkQ5Test2 aggValueTypes是否是2
TEST(LocalWindowAggTest, DISABLED_NexmarkQ5Test2)
{
    // Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator";
    json parsedJson = json::parse(Nexmark_Q5_Local_Window_Agg_2); // inputTypes 类型有问题, 第一二列应该是时间类型
    omnistream::OperatorConfig opConfig(
        uniqueName,                 // uniqueName:
        "LocalWindowAgg_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    auto* output = new BatchOutputTest();
    auto* windowAggOperator = dynamic_cast<LocalSlicingWindowAggOperator*>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    windowAggOperator->open();
    omnistream::VectorBatch* vBatch = nexmarkQ5Input2();
    auto* streamRecord = new StreamRecord(vBatch);
    windowAggOperator->processBatch(streamRecord);
    auto watermark = Watermark(1000000);
    windowAggOperator->ProcessWatermark(&watermark);

    //    auto* batchOutput = dynamic_cast<BatchOutputTest*>(windowAggOperator->getOutput());
    std::cout << "=========== print result ==========" << std::endl;
    auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
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
    std::cout << "LocalWindowAggTest  NexmarkQ5Test2" << std::endl;
}

TEST(LocalWindowAggTest, NexmarkQ8Test1)
{
    // Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator";
    json parsedJson = json::parse(Nexmark_Q8_Local_Window_Agg_1);
    omnistream::OperatorConfig opConfig(
        uniqueName,                 // uniqueName:
        "LocalWindowAgg_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    auto* output = new BatchOutputTest();
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
    std::vector<omnistream::RowField> typeInfo{omnistream::RowField("col0", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto* windowAggOperator = dynamic_cast<LocalSlicingWindowAggOperator*>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    windowAggOperator->setup();
    windowAggOperator->initializeState(initializer, ser);
    std::cout << "NexmarkQ8Test1" << std::endl;
    windowAggOperator->open();
    std::cout << "NexmarkQ8Test2" << std::endl;
    omnistream::VectorBatch* vBatch = nexmarkQ8Input1();
    auto* streamRecord = new StreamRecord(vBatch);
    windowAggOperator->processBatch(streamRecord);
    std::cout << "NexmarkQ8Test3" << std::endl;
    auto watermark = Watermark(1000000);
    windowAggOperator->ProcessWatermark(&watermark);
    std::cout << "NexmarkQ8Test4" << std::endl;

    std::cout << "=========== print result ==========" << std::endl;
    auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    // print VectorBatch
    std::vector types = {"BIGINT", "VARCHAR", "BIGINT"};
    int rowCount = resultBatch->GetRowCount();
    int colCount = resultBatch->GetVectorCount();
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
    std::cout << "LocalWindowAggTest  NexmarkQ8Test" << std::endl;
}

TEST(LocalWindowAggTest, NexmarkQ7Test)
{
    // Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator";
    json parsedJson = json::parse(nexmarkQ7Description);
    omnistream::OperatorConfig opConfig(
        uniqueName,                 // uniqueName:
        "LocalWindowAgg_By_Simple", // Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    auto* output = new BatchOutputTest();
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
    std::vector<omnistream::RowField> typeInfo{omnistream::RowField("col0", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto* windowAggOperator = dynamic_cast<LocalSlicingWindowAggOperator*>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    windowAggOperator->setup();
    windowAggOperator->initializeState(initializer, ser);
    windowAggOperator->open();
    omnistream::VectorBatch* vBatch = nexmarkQ7Input();
    auto* streamRecord = new StreamRecord(vBatch);
    windowAggOperator->processBatch(streamRecord);
    auto watermark = Watermark(1000000);
    windowAggOperator->ProcessWatermark(&watermark);

    auto* batchOutput = dynamic_cast<BatchOutputTest*>(windowAggOperator->getOutput());
    std::cout << "=========== print result ==========" << std::endl;
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
    std::cout << "LocalWindowAggTest  NexmarkQ7Test" << std::endl;
}

/*TEST(LocalWindowAggTest, OperatorChainTest) {
    std::vector<std::string> operatorDescriptors{Operator_Chain_Local_Window, Operator_Chain_Calc};
    std::vector<omnistream::OperatorConfig> opChainConfig;
    for (int i = 0; i < operatorDescriptors.size(); i++) {
        auto desc = operatorDescriptors[i];
        nlohmann::json parsedJson = nlohmann::json::parse(desc);
        omnistream::OperatorConfig opConfig(
                parsedJson["id"],  // uniqueName:
                parsedJson["name"], // Name
                parsedJson["description"]["inputType"],
                parsedJson["description"]["outputType"],
                parsedJson["description"]);
        opChainConfig.push_back(opConfig);
    }
    BatchOutputTest *output = new BatchOutputTest();
    OperatorChain* chain = new OperatorChain(opChainConfig);
    StreamOperator * headOp = chain->createMainOperatorAndCollector(opChainConfig, output);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new
TaskInfoImpl("OperatorChainTest", 1, 1, 0))); chain->initializeStateAndOpenOperators(initializer);

    auto localWindowAggOperator = dynamic_cast<LocalSlicingWindowAggOperator*>(headOp);
    omnistream::VectorBatch* vBatch = nexmarkQ7Input();
    auto* streamRecord = new StreamRecord(vBatch);
    localWindowAggOperator->processBatch(streamRecord);
    localWindowAggOperator->ProcessWatermark(new Watermark(1000000));

    std::cout << "=========== print result ==========" << std::endl;
    auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*> (output->getVectorBatch());
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
    std::cout << "LocalWindowAggTest  OperatorChainTest" << std::endl;
}*/
