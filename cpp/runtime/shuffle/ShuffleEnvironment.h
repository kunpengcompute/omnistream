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

#ifndef SHUFFLEENVIRONMENT_H
#define SHUFFLEENVIRONMENT_H

#include <iostream>
#include <vector>
#include <memory>
#include <string>
#include <set>
#include <executiongraph/descriptor/ExecutionAttemptIDPOD.h>
#include <executiongraph/descriptor/InputGateDeploymentDescriptorPOD.h>
#include <executiongraph/descriptor/PartitionInfoPOD.h>
#include <executiongraph/descriptor/ResultPartitionDeploymentDescriptorPOD.h>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>
#include <streaming/runtime/metrics/MetricGroup.h>
#include <partition/ResultPartitionWriter.h>
#include <partition/consumer/IndexedInputGate.h>
#include <partition/PartitionProducerStateProvider.h>

#include "ShuffleIOOwnerContextPOD.h"

namespace omnistream {
class ShuffleEnvironment {
public:
    virtual ~ShuffleEnvironment() = default;

    virtual int start() = 0;

    virtual ShuffleIOOwnerContextPOD createShuffleIOOwnerContext(
        const std::string& ownerName,
        const ExecutionAttemptIDPOD& executionAttemptID,
        std::shared_ptr<MetricGroup> parentGroup) = 0;

    virtual std::vector<std::shared_ptr<ResultPartitionWriter>> createResultPartitionWriters(
        ShuffleIOOwnerContextPOD ownerContext,
        const std::vector<ResultPartitionDeploymentDescriptorPOD>& resultPartitionDeploymentDescriptors,
        int taskType) = 0;

    virtual void releasePartitionsLocally(const std::set<ResultPartitionIDPOD>& partitionIds) = 0;

    virtual std::set<ResultPartitionIDPOD> getPartitionsOccupyingLocalResources() = 0;

    virtual std::vector<std::shared_ptr<SingleInputGate>> createInputGates(
        ShuffleIOOwnerContextPOD ownerContext,
        std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider,
        const std::vector<InputGateDeploymentDescriptorPOD>& inputGateDeploymentDescriptors,
        int taskType) = 0;

    virtual bool updatePartitionInfo(ExecutionAttemptIDPOD consumerID, PartitionInfoPOD partitionInfo) = 0;

    virtual void close() = 0;
};
} // namespace omnistream

#endif // SHUFFLEENVIRONMENT_H
