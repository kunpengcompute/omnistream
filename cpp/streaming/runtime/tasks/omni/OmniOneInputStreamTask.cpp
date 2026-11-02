/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "OmniOneInputStreamTask.h"

#include <typeinfo/TypeInfoFactory.h>

#include "streaming/runtime/io/StreamTaskNetworkInputFactory.h"
#include "streaming/runtime/io/StreamOneInputProcessor.h"
#include "streaming/runtime/io/OmniPushingAsyncDataInput.h"
#include "streaming/runtime/io/OmniStreamTaskNetworkInput.h"
#include "streaming/runtime/io/OmniStreamTaskNetworkInputFactory.h"
#include "streaming/runtime/io/StreamTaskNetworkOutput.h"
#include "streaming/runtime/io/OmniStreamOneInputProcessor.h"
#include "../../io/OmniStreamTaskNetworkOutput.h"
#include "partition/consumer/UnionInputGate.h"

namespace omnistream {
namespace {
constexpr int SQL_TASK_TYPE = 1;
constexpr const char* OMNI_SQL_UNION_ALL_INPUT = "omni.sql.union-all.input";
constexpr const char* TRUE_VALUE = "true";
} // namespace

// temporary set parameter type is int
OmniPushingAsyncDataInput::OmniDataOutput* OmniOneInputStreamTask::createDataOutput(
    std::shared_ptr<omnistream::SimpleCounter>& numRecordsIn, std::shared_ptr<omnistream::SimpleCounter>& numBytesIn)
{
    return new OmniStreamTaskNetworkOutput(
        dynamic_cast<OneInputStreamOperator*>(mainOperator_), numRecordsIn, numBytesIn);
}

OmniStreamTaskInput* OmniOneInputStreamTask::CreateTaskInput(std::shared_ptr<CheckpointedInputGate> inputGate)
{
    auto inputRescalingDescriptor = env_->getTaskStateManager()->getInputRescalingDescriptor();
    auto pod = env_->taskConfiguration().getStreamConfigPOD();
    const std::vector<StreamEdgePOD>& edges = pod.getInStreamEdges();
    auto getPartitionerFunction = std::function<StreamPartitioner<IOReadableWritable>*(int)>(
        [this, edges](int i) { return this->createPartitionerFromDesc(edges[i], true); });
    auto getSqlPartitionerFunction = std::function<StreamPartitionerV2<StreamRecord>*(int)>(
        [this, edges](int i) { return this->createPartitionerFromDesc(edges[i].getPartitioner()); });
    // initialize TypeInformation and channelInfos
    if (taskType == 1) {
        // todo: fix it later
        std::vector<long> channelInfoIndex;
        auto channelInfos = inputGate->GetChannelInfos();
        channelInfoIndex.reserve(channelInfos.size());
        for (size_t i = 0; i < channelInfos.size(); ++i) {
            channelInfoIndex.push_back(static_cast<long>(channelInfos[i].getComplexId()));
        }
        LOG("OperatorDescription "
            << this->taskConfiguration_.getStreamConfigPOD().getOperatorDescription().toString());
        std::vector<std::string> typeList;
        std::string description =
            this->taskConfiguration_.getStreamConfigPOD().getOperatorDescription().getDescription();
        auto descriptionJson = nlohmann::json::parse(description);
        if (!descriptionJson.contains("inputTypes")) {
            auto inputTypes = this->taskConfiguration_.getStreamConfigPOD().getOperatorDescription().getInputs();
            TypeDescriptionPOD inputType;
            inputType = inputTypes[0];
            auto types = inputType.getType();
            json typeArray = json::parse(types);
            for (const auto& obj : typeArray) {
                typeList.push_back(obj.at("type").get<std::string>());
            }
        } else {
            typeList = descriptionJson["inputTypes"].get<std::vector<std::string>>();
        }
        return OmniStreamTaskNetworkInputFactory::create(
            0,
            inputGate,
            taskType,
            new BinaryRowDataSerializer(typeList.size(), typeList),
            channelInfoIndex,
            inputRescalingDescriptor,
            getPartitionerFunction,
            getSqlPartitionerFunction,
            &taskConfiguration_);
    } else if (taskType == 2) {
        auto operatorPod = this->taskConfiguration_.getStreamConfigPOD().getOperatorDescription();

        auto inputTypes = operatorPod.getInputs();
        auto numberOfInputChannels = inputGate->GetNumberOfInputChannels();
        // Create a C++ normal array (vector)
        std::vector<long> channel_array(numberOfInputChannels);
        auto channelInfos = inputGate->GetChannelInfos();
        // Copy elements from JSON array to C++ array
        uint32_t i = 0;
        for (auto& item : channelInfos) {
            channel_array[i++] = item.getComplexId();
        }

        TypeDescriptionPOD inputType;
        inputType = inputTypes[0];

        TypeInformation* typeInfo;

        if (inputType.kind == "basic") {
            std::string inputTypeName = inputType.type;
            typeInfo = TypeInfoFactory::createTypeInfo(inputTypeName.c_str());
        } else if (inputType.kind == "Tuple") {
            auto filedType = ::json::parse(inputType.type);
            typeInfo = TypeInfoFactory::createTupleTypeInfo(filedType);
        } else {
            THROW_LOGIC_EXCEPTION("Unknown Input type" + inputType.kind);
        }

        TypeSerializer* inputSerializer = typeInfo->getTypeSerializer();
        inputSerializer->setSelfBufferReusable(true);

        return OmniStreamTaskNetworkInputFactory::create(
            0,
            inputGate,
            taskType,
            inputSerializer,
            channel_array,
            inputRescalingDescriptor,
            getPartitionerFunction,
            getSqlPartitionerFunction,
            &taskConfiguration_);
    } else {
        THROW_LOGIC_EXCEPTION("Unknown taskType " + taskType);
    }
}

void OmniOneInputStreamTask::init()
{
    OmniStreamTask::init();
    auto operatorPod = this->taskConfiguration_.getStreamConfigPOD().getOperatorDescription();
    auto counter = env_->taskMetricGroup()->GetInternalOperatorIOMetric(operatorPod.getName(), "numRecordsIn");
    auto bytesCounter = env_->taskMetricGroup()->GetInternalOperatorIOMetric(operatorPod.getName(), "numBytesIn");
    auto inputGate = CreateCheckpointedInputGate();
    auto output = createDataOutput(
        reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter>&>(counter),
        reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter>&>(bytesCounter));
    auto input = CreateTaskInput(inputGate);

    inputProcessor_ = new OmniStreamOneInputProcessor(input, output, operatorChain.get());
}

void OmniOneInputStreamTask::processInput(MailboxDefaultAction::Controller* controller)
{
    // LOG(">>>>OmniOneInputStreamTask::processInput")
    OmniStreamTask::processInput(controller);

#ifdef TROUBLE_SHOOTING
    // assume this is sink
    LOG_TRACE(taskName_ << "   Sink  Slow down  thefor 1 second to trigger the back pressure");
    std::this_thread::sleep_for(std::chrono::seconds(2));
#endif
}

const std::string OmniOneInputStreamTask::getName() const
{
    return std::string("OmniOneInputStreamTask");
}

std::shared_ptr<CheckpointedInputGate> OmniOneInputStreamTask::CreateCheckpointedInputGate()
{
    auto checkpointableInputs = env_->GetAllInputGates();
    if (taskType == 1 && checkpointableInputs.size() != 1) {
        throw std::invalid_argument("SQL one-input task requires exactly one physical input gate");
    }
    std::vector<std::shared_ptr<OmniStreamTaskSourceInput>> emptySourceInputs;

    taskConfiguration_ = env_->taskConfiguration();
    auto checkpointExecutionConfig = taskConfiguration_.getExecutionCheckpointConfig();
    const std::int64_t alignedCheckpointTimeoutMillis =
        checkpointExecutionConfig.getAlignedCheckpointTimeoutSecond() * 1000 +
        checkpointExecutionConfig.getAlignedCheckpointTimeoutNano() / 1000000;
    auto checkpointBarrierHandler = InputProcessorUtil::CreateCheckpointBarrierHandler(
        this,
        getName(),
        GetSubtaskCheckpointCoordinator(),
        mainMailboxExecutor_,
        systemTimerService,
        {checkpointableInputs},
        emptySourceInputs,
        checkpointExecutionConfig.getUnalignedCheckpointsEnabled(),
        alignedCheckpointTimeoutMillis,
        checkpointExecutionConfig.getCheckpointAfterTasksFinishEnabled());

    if (ShouldUseUnionInputGate(taskType, checkpointableInputs.size(), HasUnionAllInput())) {
        auto unionGate = std::make_shared<UnionInputGate>(checkpointableInputs);
        // The union gate reports one INNER_RECOVER after all physical channels recover.
        numberOfInnerRecover = 1;
        return std::make_shared<CheckpointedInputGate>(unionGate, checkpointBarrierHandler, mainMailboxExecutor_);
    }

    auto checkpointedInputGates = InputProcessorUtil::CreateCheckpointedMultipleInputGate(
        mainMailboxExecutor_, {checkpointableInputs}, checkpointBarrierHandler);

    return checkpointedInputGates[0];
}

bool OmniOneInputStreamTask::ShouldUseUnionInputGate(int taskType, size_t inputGateCount, bool hasUnionAllInput)
{
    return taskType == SQL_TASK_TYPE && inputGateCount > 1 && hasUnionAllInput;
}

long OmniOneInputStreamTask::GetChannelDeserializerId(const InputChannelInfo& channelInfo, bool useUnionAllChannelId)
{
    return useUnionAllChannelId ? channelInfo.getComplexId() : static_cast<long>(channelInfo.getInputChannelIdx());
}

bool OmniOneInputStreamTask::HasUnionAllInput() const
{
    const auto omniConf = taskConfiguration_.getStreamConfigPOD().getOmniConf();
    const auto it = omniConf.find(OMNI_SQL_UNION_ALL_INPUT);
    return it != omniConf.end() && it->second == TRUE_VALUE;
}
} // namespace omnistream
