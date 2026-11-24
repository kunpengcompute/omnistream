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
#include "streaming/runtime/tasks/omni/OmniTwoInputStreamTask.h"
#include "runtime/checkpoint/CheckpointType.h"
#include "test/runtime/io/checkpointing/MockClasses.h"
#include "runtime/shuffle/ShuffleEnvironment.h"
#include "runtime/executiongraph/descriptor/TaskDeploymentDescriptorPOD.h"
#include "runtime/taskexecutor/TaskManagerServices.h"
#include "partition/consumer/RemoteInputChannel.h"
#include "runtime/io/network/api/writer/V2/RecordWriterBuilderV2.h"
#include "streaming/runtime/partitioner/V2/ForwardPartitionerV2.h"
#include "runtime/executiongraph/TaskInformationPOD.h"
#include <iostream>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include "core/utils/monitormmap/MetricManager.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
using namespace omnistream;
using namespace omnistream::runtime;
using nlohmann::json;

using ::std::shared_ptr;

static json loadJson(const std::string& path)
{
    std::ifstream f(path);
    if (!f.is_open()) {
        throw std::runtime_error("Failed to open file: " + path);
    }
    try {
        return json::parse(f);
    } catch (const json::parse_error& e) {
        throw std::runtime_error("JSON parse error in " + path + ": " + std::string(e.what()));
    }
}

static TypeDescriptionPOD parseTypeDesc(const json& j)
{
    return TypeDescriptionPOD(
        j.at("kind").get<std::string>(),
        j.at("isNull").get<bool>(),
        j.at("precision").get<int>(),
        j.at("type").get<std::string>(),
        j.at("timestampKind").get<int>(),
        j.at("fieldName").get<std::string>());
}

static OperatorPOD buildOperatorPod(const json& opDesc)
{
    // inputs
    std::vector<TypeDescriptionPOD> inputs;
    if (opDesc.contains("inputs") && opDesc["inputs"].is_array()) {
        for (const auto& inJ : opDesc["inputs"]) {
            inputs.emplace_back(parseTypeDesc(inJ));
        }
    }

    // output
    TypeDescriptionPOD output = parseTypeDesc(opDesc.at("output"));

    return OperatorPOD(
        opDesc.at("name").get<std::string>(),
        opDesc.at("id").get<std::string>(),
        opDesc.at("description").get<std::string>(),
        inputs,
        output,
        opDesc.at("operatorId").get<std::string>(),
        opDesc.at("vertexID").get<int>(),
        /* numInputs */ 2,
        /* numOutputs */ 2,
        /* chainingStrategy */ 2);
}

static StreamConfigPOD buildStreamConfig(const json& scJ)
{
    StreamConfigPOD cfg;
    cfg.setOperatorDescription(buildOperatorPod(scJ.at("operatorDescription")));
    cfg.setNumberOfNetworkInputs(scJ.at("numberOfNetworkInputs").get<int>());

    // In-stream edges (two, like original)
    StreamEdgePOD edge1(
        1, 5, 1, StreamPartitionerPOD("hash", std::vector<KeyFieldInfoPOD>{KeyFieldInfoPOD("f0", "BIGINT", 1)}), 100);
    StreamEdgePOD edge2(
        3, 5, 2, StreamPartitionerPOD("hash", std::vector<KeyFieldInfoPOD>{KeyFieldInfoPOD("f0", "BIGINT", 0)}), 100);
    cfg.setInStreamEdges(std::vector<StreamEdgePOD>{edge1, edge2});
    return cfg;
}

static TaskInformationPOD createTaskInformation(const json& joinTddJson)
{
    // task-level
    auto taskName = joinTddJson.at("taskName").get<std::string>();
    auto numberOfSubtasks = joinTddJson.at("numberOfSubtasks").get<int>();
    auto maxNumberOfSubs = joinTddJson.at("maxNumberOfSubtasks").get<int>();
    auto indexOfSubtask = joinTddJson.at("indexOfSubtask").get<int>();
    auto stateBackend = joinTddJson.at("stateBackend").get<std::string>();

    // stream config
    StreamConfigPOD streamCfg = buildStreamConfig(joinTddJson.at("streamConfig"));

    // chain config (original built a vector with only the same stream config)
    std::vector<StreamConfigPOD> chainedConfig;
    chainedConfig.push_back(streamCfg);

    // empty chain container (same as original)
    OperatorChainPOD operatorChainPod;

    // rocksdb store paths (original used empty)
    std::vector<std::string> rocksdbStorePaths;

    return TaskInformationPOD(
        taskName,
        numberOfSubtasks,
        maxNumberOfSubs,
        indexOfSubtask,
        streamCfg,
        operatorChainPod,
        chainedConfig,
        stateBackend,
        rocksdbStorePaths);
}

TEST(CheckpointingCombinedTest, DISABLED_Test1)
{
    // ---------------------------
    //          Input JSON
    // ---------------------------
    std::cout << "Opening join_tdd.json" << std::endl;
    const json joinTddJson = loadJson("./input/join_tdd.json");

    // TaskManagerServices configuration
    const json taskMCSConfJson = loadJson("./input/taskMCSConfJoin.json");
    auto taskManagerServices =
        std::shared_ptr<TaskManagerServices>(TaskManagerServices::fromConfiguration(taskMCSConfJson));
    auto shuffleEnv = taskManagerServices->getShuffleEnvironment();

    // ---------------------------
    //        Read fields
    // ---------------------------
    std::vector<omnistream::InputGateDeploymentDescriptorPOD> inputGatePods;
    joinTddJson.at("inputGates").get_to(inputGatePods);
    ASSERT_EQ(inputGatePods.size(), 2);

    // ---------------------------
    //       Build task info/env
    // ---------------------------
    const TaskInformationPOD taskInformationPod = createTaskInformation(joinTddJson);

    auto env = std::make_shared<RuntimeEnvironmentV2>();
    env->setTaskConfiguration(taskInformationPod);

    auto jobId = *omnistream::JobIDPOD::generate();
    auto attemptId = std::make_unique<ExecutionAttemptIDPOD>();
    auto jobVertexID = std::make_unique<JobVertexID>();
    auto stateStore = new TaskLocalStateStore(jobId, *jobVertexID, 0, nullptr);
    auto responder = new omnistream::NoOpCheckpoingResponder();
    auto taskStateManager = std::make_shared<omnistream::TaskStateManager>(jobId, *attemptId, stateStore, responder);
    env->SetTaskStateManager(taskStateManager);

    auto shuffleIOOwnerContext = shuffleEnv->createShuffleIOOwnerContext(
        "testShuffleIOOwnerContext", omnistream::ExecutionAttemptIDPOD(), nullptr);

    // ---------------------------
    //        Upstream TDDs
    // ---------------------------
    const json source1TddJson = loadJson("./input/source1_tdd.json");
    TaskDeploymentDescriptorPOD srcTddInfo1 = source1TddJson;

    const json source2TddJson = loadJson("./input/source2_tdd.json");
    TaskDeploymentDescriptorPOD srcTddInfo2 = source2TddJson;

    // ---------------------------
    //   Writers / RecordWriters
    // ---------------------------
    std::vector<std::shared_ptr<ResultPartitionWriter>> upstreamWriters;

    auto rpw1 = shuffleEnv->createResultPartitionWriters(shuffleIOOwnerContext, srcTddInfo1.getProducedPartitions(), 1);
    auto rpw2 = shuffleEnv->createResultPartitionWriters(shuffleIOOwnerContext, srcTddInfo2.getProducedPartitions(), 1);

    upstreamWriters.push_back(rpw1[0]);
    upstreamWriters.push_back(rpw2[0]);

    auto partitioner = std::make_shared<omnistream::ForwardPartitionerV2<StreamRecord>>();
    const int bufferTimeout = 1000;

    auto recordWriter1 = std::shared_ptr<omnistream::RecordWriterV2>(omnistream::RecordWriterBuilderV2()
                                                                         .withTaskName("testRecordWriter1")
                                                                         .withChannelSelector(partitioner.get())
                                                                         .withWriter(upstreamWriters[0])
                                                                         .withTimeout(bufferTimeout)
                                                                         .build());

    auto recordWriter2 = std::shared_ptr<omnistream::RecordWriterV2>(omnistream::RecordWriterBuilderV2()
                                                                         .withTaskName("testRecordWriter2")
                                                                         .withChannelSelector(partitioner.get())
                                                                         .withWriter(upstreamWriters[1])
                                                                         .withTimeout(bufferTimeout)
                                                                         .build());

    recordWriter1->postConstruct();
    recordWriter2->postConstruct();

    upstreamWriters[0]->setup();
    upstreamWriters[1]->setup();

    // ---------------------------
    //        Input Gates
    // ---------------------------
    std::vector<std::shared_ptr<omnistream::SingleInputGate>> singleInputGates = shuffleEnv->createInputGates(
        shuffleIOOwnerContext,
        nullptr,
        inputGatePods,
        /* taskType */ 1);

    const int targetSubpartition1 = srcTddInfo1.getProducedPartitions()[0].getPartitionId().getPartitionNum();
    const int targetSubpartition2 = srcTddInfo2.getProducedPartitions()[0].getPartitionId().getPartitionNum();

    std::vector<std::shared_ptr<omnistream::IndexedInputGate>> indexedInputGates;
    indexedInputGates.reserve(singleInputGates.size());
    for (const auto& sg : singleInputGates) {
        sg->setup();
        sg->RequestPartitions();
        indexedInputGates.push_back(sg);
    }
    env->SetInputGates(indexedInputGates);

    auto metrics = std::make_shared<omnistream::TaskMetricGroup>();
    env->SetTaskMetricGroup(metrics);

    omnistream::MetricManager* metric_manager = omnistream::MetricManager::GetInstance();
    bool result = metric_manager->Setup(32768);

    auto streamTask = std::make_shared<omnistream::OmniTwoInputStreamTask>(env);
    streamTask->restore();

    // Basic invariants
    auto coordinator =
        std::dynamic_pointer_cast<SubtaskCheckpointCoordinatorImpl>(streamTask->GetSubtaskCheckpointCoordinator());
    ASSERT_NE(coordinator, nullptr);
    ASSERT_NE(coordinator->getChannelStateWriter(), nullptr);
    ASSERT_NE(coordinator->getCheckpointStorage(), nullptr);

    auto barrierHandler =
        std::dynamic_pointer_cast<SingleCheckpointBarrierHandler>(streamTask->GetCheckpointBarrierHandler());
    ASSERT_NE(barrierHandler, nullptr);
    ASSERT_NE(barrierHandler->GetContext(), nullptr);
    ASSERT_NE(barrierHandler->GetCurrentState(), nullptr);

    // ---------------------------
    // Prepare checkpoint barrier
    // ---------------------------
    auto checkpointOptions = new CheckpointOptions(
        std::make_shared<CheckpointType>("FullCheckpoint", SnapshotType::SharingFilesStrategy::FORWARD),
        CheckpointStorageLocationReference::GetDefault());
    auto barrier = std::make_shared<CheckpointBarrier>(0, 10000, checkpointOptions);

    upstreamWriters[0]->broadcastEvent(barrier, targetSubpartition1);
    upstreamWriters[0]->finish();
    upstreamWriters[1]->broadcastEvent(barrier, targetSubpartition2);
    upstreamWriters[1]->finish();

    // Invoke task (all runs on correct thread now)
    streamTask->invoke();

    recordWriter1->close();
    recordWriter2->close();
    upstreamWriters[0]->close();
    upstreamWriters[1]->close();

    // Wait for checkpoint completion
    int retries = 50;
    bool finished = false;
    while (retries-- > 0) {
        auto checkpoints = coordinator->GetCheckpoints();
        auto it = checkpoints.find(0);
        if (it == checkpoints.end()) {
            finished = true;
            break;
        }
        auto currentRunnable = it->second;
        if (currentRunnable->IsFinished()) {
            finished = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_TRUE(finished) << "Checkpoint 0 did not complete in time";
}
