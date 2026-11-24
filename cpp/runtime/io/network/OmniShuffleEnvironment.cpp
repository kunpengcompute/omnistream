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

#include "OmniShuffleEnvironment.h"

#include <utility>

namespace omnistream {
ShuffleIOOwnerContextPOD OmniShuffleEnvironment::createShuffleIOOwnerContext(
    const std::string& ownerName,
    const ExecutionAttemptIDPOD& executionAttemptID,
    std::shared_ptr<MetricGroup> parentGroup)
{
    ShuffleIOOwnerContextPOD context(ownerName, executionAttemptID);
    return context;
}

std::vector<std::shared_ptr<ResultPartitionWriter>> OmniShuffleEnvironment::createResultPartitionWriters(
    ShuffleIOOwnerContextPOD ownerContext,
    const std::vector<ResultPartitionDeploymentDescriptorPOD>& resultPartitionDeploymentDescriptors,
    int taskType)
{
    std::lock_guard<std::mutex> lockGuard(lock);

    if (isClosed_) {
        THROW_LOGIC_EXCEPTION("The OmniShuffleEnvironment has already been shut down.");
    }

    LOG_INFO_IMP("Before create writer");
    std::vector<std::shared_ptr<ResultPartitionWriter>> resultPartitions(resultPartitionDeploymentDescriptors.size());
    for (size_t partitionIndex = 0; partitionIndex < resultPartitions.size(); ++partitionIndex) {
        LOG("Creating partition " << std::to_string(partitionIndex) << " partition descriptor: "
                                  << resultPartitionDeploymentDescriptors[partitionIndex].toString());
        // todo: pass the parameter config to the create function
        resultPartitions[partitionIndex] = resultPartitionFactory->create(
            ownerContext.getOwnerName(),
            partitionIndex,
            resultPartitionDeploymentDescriptors[partitionIndex],
            // todo:add config
            config,
            taskType);
    }
    LOG_INFO_IMP("Before return  writer");
    return resultPartitions;
}

std::vector<std::shared_ptr<SingleInputGate>> OmniShuffleEnvironment::createInputGates(
    ShuffleIOOwnerContextPOD ownerContext,
    std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider,
    const std::vector<InputGateDeploymentDescriptorPOD>& inputGateDeploymentDescriptors,
    int taskType)
{
    std::lock_guard<std::mutex> lockGuard(lock);
    if (isClosed_) {
        THROW_LOGIC_EXCEPTION("The OmniShuffleEnvironment has already been shut down.");
    }
    std::vector<std::shared_ptr<SingleInputGate>> inputGates;
    for (size_t gateIndex = 0; gateIndex < inputGateDeploymentDescriptors.size(); ++gateIndex) {
        const auto& igdd = inputGateDeploymentDescriptors[gateIndex];
        auto inputGate = singleInputGateFactory->create(
            ownerContext.getOwnerName(),
            gateIndex,
            std::make_shared<InputGateDeploymentDescriptorPOD>(igdd),
            partitionProducerStateProvider,
            taskType);
        LOG("InputGate created successfully.");
        auto id = std::make_shared<InputGateID>(igdd.getConsumedResultId(), ownerContext.getExecutionAttemptID());
        (*inputGatesById)[id] = inputGate;
        std::shared_ptr<Runnable> removeIdTask = nullptr;
        {
            class RemoveIdTask : public Runnable {
            public:
                std::shared_ptr<std::map<std::shared_ptr<InputGateID>, std::shared_ptr<SingleInputGate>>> map;
                std::shared_ptr<InputGateID> id;
                RemoveIdTask(
                    std::shared_ptr<std::map<std::shared_ptr<InputGateID>, std::shared_ptr<SingleInputGate>>> map,
                    std::shared_ptr<InputGateID> id)
                    : map(std::move(map)),
                      id(std::move(id))
                {
                }

                void run() override
                {
                    (map)->erase(id);
                }
            };
            removeIdTask = std::make_shared<RemoveIdTask>(inputGatesById, id);
            // inputGate->getCloseFuture()->thenRun(removeIdTask.get());
        }

        LOG("InputGate CloseFuture set successfully");
        inputGates.push_back(inputGate);
    }

    return inputGates;
};

} // namespace omnistream
