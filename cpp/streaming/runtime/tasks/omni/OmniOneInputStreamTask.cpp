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

namespace omnistream {
// temporary set parameter type is int
OmniPushingAsyncDataInput::OmniDataOutput* OmniOneInputStreamTask::createDataOutput(
    std::shared_ptr<omnistream::SimpleCounter>& numRecordsIn)
{
    return new OmniStreamTaskNetworkOutput(dynamic_cast<OneInputStreamOperator*>(mainOperator_), numRecordsIn);
}

OmniStreamTaskInput* OmniOneInputStreamTask::CreateTaskInput(std::shared_ptr<CheckpointedInputGate> inputGate)
{
    auto inputRescalingDescriptor = env_->getTaskStateManager()->getInputRescalingDescriptor();
    auto pod = env_->taskConfiguration().getStreamConfigPOD();
    const std::vector<StreamEdgePOD>& edges = pod.getInStreamEdges();
    auto getPartitionerFunction = std::function<StreamPartitioner<IOReadableWritable>*(int)>(
        [this, edges](int i) { return this->createPartitionerFromDesc(edges[i], true); });
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
            &taskConfiguration_);
    } else {
        THROW_LOGIC_EXCEPTION("Unknown taskType " + taskType);
    }
}

void OmniOneInputStreamTask::init()
{
    OmniStreamTask::init();
    auto counter = env_->taskMetricGroup()->GetInternalOperatorIOMetric(mainOperator_->getTypeName(), "numRecordsOut");
    auto inputGate = CreateCheckpointedInputGate();
    auto output = createDataOutput(reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter>&>(counter));
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

    auto checkpointedInputGates = InputProcessorUtil::CreateCheckpointedMultipleInputGate(
        mainMailboxExecutor_, {checkpointableInputs}, checkpointBarrierHandler);

    return checkpointedInputGates[0];
}
} // namespace omnistream
