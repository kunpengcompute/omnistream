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

#include "OmniTwoInputStreamTask.h"
#include "streaming/runtime/io/OmniStreamTwoInputProcessorFactory.h"

namespace omnistream {
void OmniTwoInputStreamTask::init()
{
    auto pod = env_->taskConfiguration().getStreamConfigPOD();
    int32_t numberOfInputs = pod.getNumberOfNetworkInputs();
    std::vector<std::shared_ptr<IndexedInputGate>> inputList1;
    std::vector<std::shared_ptr<IndexedInputGate>> inputList2;
    const std::vector<StreamEdgePOD>& inEdges = pod.getInStreamEdges();
    for (int i = 0; i < numberOfInputs; i++) {
        int inputType = inEdges[i].getTypeNumber();
        std::shared_ptr<IndexedInputGate> ptr = env_->GetAllInputGates()[i];
        switch (inputType) {
            case 1: inputList1.push_back(ptr); break;
            case 2: inputList2.push_back(ptr); break;
            default: THROW_RUNTIME_ERROR("Invalid input type number:" + std::to_string(inputType));
        }
    }
    if (taskType == 1 && (inputList1.size() != 1 || inputList2.size() != 1)) {
        throw std::invalid_argument("SQL two-input task requires one physical gate per logical input");
    }
    auto getPartitionerFunction = std::function<StreamPartitioner<IOReadableWritable>*(int)>(
        [&inEdges, this](const int i) { return this->createPartitionerFromDesc(inEdges[i], true); });
    auto description = nlohmann::json::parse(pod.getOperatorDescription().getDescription());
    createInputProcessor(inputList1, inputList2, description, getPartitionerFunction);
}

void OmniTwoInputStreamTask::createInputProcessor(
    std::vector<std::shared_ptr<IndexedInputGate>> inputGates1,
    std::vector<std::shared_ptr<IndexedInputGate>> inputGates2,
    const json& description,
    std::function<StreamPartitioner<IOReadableWritable>*(int)> getPartitionerFunction)
{
    std::vector<std::shared_ptr<OmniStreamTaskSourceInput>> emptySourceInputs;
    auto taskConfiguration = env_->taskConfiguration();
    auto inputRescalingDescriptor = env_->getTaskStateManager()->getInputRescalingDescriptor();
    auto checkpointExecutionConfig = taskConfiguration_.getExecutionCheckpointConfig();
    const std::int64_t alignedCheckpointTimeoutMillis =
        checkpointExecutionConfig.getAlignedCheckpointTimeoutSecond() * 1000 +
        checkpointExecutionConfig.getAlignedCheckpointTimeoutNano() / 1000000;
    checkpointBarrierHandler = InputProcessorUtil::CreateCheckpointBarrierHandler(
        this,
        getName(),
        GetSubtaskCheckpointCoordinator(),
        mainMailboxExecutor_,
        systemTimerService,
        {inputGates1, inputGates2},
        emptySourceInputs,
        checkpointExecutionConfig.getUnalignedCheckpointsEnabled(),
        alignedCheckpointTimeoutMillis,
        checkpointExecutionConfig.getCheckpointAfterTasksFinishEnabled());

    // Flatten inputGates1&2 into one vector and wrap them in checkpointedInputGate
    auto checkpointedInputGates = InputProcessorUtil::CreateCheckpointedMultipleInputGate(
        mainMailboxExecutor_, {inputGates1, inputGates2}, checkpointBarrierHandler);
    inputProcessor_ = OmniStreamTwoInputProcessorFactory::create(
        operatorChain.get(),
        checkpointedInputGates,
        static_cast<TwoInputStreamOperator*>(mainOperator_),
        taskType,
        description,
        inputRescalingDescriptor,
        getPartitionerFunction,
        &taskConfiguration);
}

const shared_ptr<CheckpointBarrierHandler>& OmniTwoInputStreamTask::GetCheckpointBarrierHandler() const
{
    return checkpointBarrierHandler;
}
} // namespace omnistream
