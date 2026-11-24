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
#include "table/data/vectorbatch/VectorBatch.h"
#include <taskmanager/OmniRuntimeEnvironment.h>
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include <thread>
#include <chrono>

using json = nlohmann::json;
using namespace omnistream;

std::string Q12description = R"DELIM(
{
	"operators": [
		{
				"output": {
				"kind": "Row",
				"type": [
					{
						"kind": "logical",
						"isNull": true,
						"type": "BIGINT"
					},
					{
						"kind": "logical",
						"isNull": false,
						"precision": 3,
						"type": "TIMESTAMP",
						"timestampKind": 0
					},
					{
						"kind": "logical",
						"isNull": false,
						"precision": 3,
						"type": "TIMESTAMP",
						"timestampKind": 0
					},
					{
						"kind": "logical",
						"isNull": false,
						"precision": 3,
						"type": "TIMESTAMP",
						"timestampKind": 0
					}
				]
			},
			"inputs": [
				{
					"kind": "Row",
					"type": [
						{
							"kind": "logical",
							"isNull": true,
							"type": "BIGINT"
						},
						{
							"kind": "logical",
							"isNull": true,
							"type": "BIGINT"
						}
					]
				}
			],
			"name": "WindowAggregate(window=[TUMBLE(slice_end=[$slice_end], size=[10 s])], select=[MAX(max$0) AS maxprice, start('w$) AS window_start, end('w$) AS window_end])",
			"description": {
				"originDescription": null,
				"inputTypes": [
					"BIGINT",
					"BIGINT"
				],
				"outputTypes": [
					"BIGINT",
					"BIGINT",
					"BIGINT",
					"BIGINT"
				],
				"grouping": [
					0
				],
				"aggInfoList": {
					"aggregateCalls": [
						{
							"name": "COUNT()",
							"aggIndex": 0,
							"aggregationFunction": "Count1AggFunction",
							"argIndexes": [],
							"consumeRetraction": "false",
							"filterArg": -1
						}
					],
					"AccTypes": [
						"BIGINT"
					],
					"aggValueTypes": [
						"BIGINT"
					],
					"indexOfCountStar": -1
				},
				"generateUpdateBefore": false,
				"isWindowAggregate": true,
				"timeAttributeIndex": -1,
				"size": 10000,
				"window": "TUMBLE(size=[10 s])",
				"timeAttributeType": "TIMESTAMP_WITH_LOCAL_TIME_ZONE"
			},
			"id": "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator"
		}
	]
}
)DELIM";

omnistream::VectorBatch* newVectorBatchOneKeyOneValueQ12Test1()
{
    auto* vbatch = new omnistream::VectorBatch(9);
    std::vector<int64_t> bids = {2001, 2003, 2001, 2003, 2002, 2004, 2002, 2005, 2001};
    std::vector<int64_t> timeStamps = {1789000, 1789000, 1789000, 1789000, 1789000, 1788000, 1788000, 1789000, 1789000};

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(9, bids.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(9, timeStamps.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
    vbatch->setRowKind(5, RowKind::INSERT);
    vbatch->setRowKind(6, RowKind::INSERT);
    vbatch->setRowKind(7, RowKind::INSERT);
    vbatch->setRowKind(8, RowKind::INSERT);
    return vbatch;
}

TEST(NEXTMARKTESTQ12, DISABLED_COUNTTEST)
{
    omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneValueQ12Test1();

    // Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
    json parsedJson = json::parse(Q12description);
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
    slicingWindowOperator->open();
    slicingWindowOperator->processBatch(vbatch);

    std::this_thread::sleep_for(std::chrono::seconds(15));
    std::cout << "==========================================================" << std::endl;
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
}
