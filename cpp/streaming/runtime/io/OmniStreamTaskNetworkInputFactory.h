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

#ifndef OMNISTREAM_OMNISTREAMTASKNETWORKINPUTFACTORY_H
#define OMNISTREAM_OMNISTREAMTASKNETWORKINPUTFACTORY_H

#include "OmniStreamTaskNetworkInput.h"
#include "runtime/io/recover/OmniRescalingStreamTaskNetworkInput.h"

namespace omnistream {
class OmniStreamTaskNetworkInputFactory {
public:
    static OmniStreamTaskInput* create(
        int64_t inputIndex,
        const std::shared_ptr<CheckpointedInputGate>& inputGate,
        int taskType,
        TypeSerializer* inputSerializer,
        std::vector<long>& channelInfos,
        std::shared_ptr<InflightDataRescalingDescriptor> inflightDataRescalingDescriptor,
        std::function<StreamPartitioner<IOReadableWritable>*(int)> getPartitionerFunction,
        std::function<StreamPartitionerV2<StreamRecord>*(int)> getSqlPartitionerFunction,
        TaskInformationPOD* taskInfo)
    {
        INFO_RELEASE("inflight is null:" << (inflightDataRescalingDescriptor == nullptr));
        if (!inflightDataRescalingDescriptor ||
            inflightDataRescalingDescriptor == InflightDataRescalingDescriptor::noRescale) {
            INFO_RELEASE("create OmniStreamTaskNetworkInput");
            return new OmniStreamTaskNetworkInput(
                inputIndex,
                inputGate,
                taskType,
                inputSerializer,
                channelInfos,
                taskInfo->getExecutionCheckpointConfig().getCheckpointInterval());
        }
        INFO_RELEASE("create OmniRescalingStreamTaskNetworkInput");
        return new OmniRescalingStreamTaskNetworkInput(
            inputIndex,
            inputGate,
            taskType,
            inputSerializer,
            channelInfos,
            inflightDataRescalingDescriptor,
            getPartitionerFunction,
            getSqlPartitionerFunction,
            taskInfo);
    }
};
} // namespace omnistream

#endif
