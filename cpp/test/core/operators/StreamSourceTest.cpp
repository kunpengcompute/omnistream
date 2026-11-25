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

#include "datagen/nexmark/NexmarkSourceFunction.h"
#include "streaming/api/operators/StreamSource.h"
#include "OutputTest.h"
#include "table/runtime/operators/TableOperatorConstants.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "runtime/executiongraph/operatorchain/OperatorPOD.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/TaskStateManager.h"

namespace {
class CancelTrackingSourceFunction : public SourceFunction<omnistream::VectorBatch> {
public:
    void run(SourceContext*) override
    {
    }

    void cancel() override
    {
        cancelCount++;
    }

    int getCancelCount() const
    {
        return cancelCount;
    }

private:
    int cancelCount = 0;
};
} // namespace

TEST(StreamSourceTest, StopCancelsSourceFunction)
{
    auto* sourceFunction = new CancelTrackingSourceFunction();
    StreamSource<omnistream::VectorBatch> source(sourceFunction, nullptr);

    source.stop(StopMode::DRAIN);
    source.stop(StopMode::NO_DRAIN);

    EXPECT_EQ(sourceFunction->getCancelCount(), 2);
    source.run();
}

TEST(StreamSourceTest, NexmarkSourceFunction)
{
    std::string description = R"({"format":"nexmark", "batchSize":10, "configMap":{"maxEvents":100}})";
    nlohmann::json opDescription = nlohmann::json::parse(description);
    OperatorPOD nexmarkPOD("nexmark_source", std::string(OPERATOR_NAME_STREAM_SOURCE), description, {}, {});
    nexmarkPOD.setVOperatorType(omnistream::Type_o::SQL);
    BatchOutputTest* output = new BatchOutputTest();
    // StreamTask is set to nullptr
    auto sourceOp = reinterpret_cast<StreamSource<omnistream::VectorBatch>*>(
        StreamOperatorFactory::createOperatorAndCollector(nexmarkPOD, output, nullptr));
    sourceOp->setup();
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
    sourceOp->initializeState(new StreamTaskStateInitializerImpl(env2), new IntSerializer());
    sourceOp->open();
    sourceOp->run();
    auto generatedOp = output->getVectorBatch();
}
