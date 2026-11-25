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

#ifndef OMNISTREAM_OMNITWOINPUTSTREAMTASK_H
#define OMNISTREAM_OMNITWOINPUTSTREAMTASK_H

#include "OmniStreamTask.h"
#include "runtime/io/checkpointing/CheckpointBarrierHandler.h"
#include "runtime/io/checkpointing/InputProcessorUtil.h"

namespace omnistream {
class OmniTwoInputStreamTask : public OmniStreamTask {
public:
    explicit OmniTwoInputStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env) : OmniStreamTask(env)
    {
    }
    explicit OmniTwoInputStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env, int taskType)
        : OmniStreamTask(env, taskType)
    {
    }

    void init() override;
    const shared_ptr<CheckpointBarrierHandler>& GetCheckpointBarrierHandler() const;

protected:
    void createInputProcessor(
        std::vector<std::shared_ptr<IndexedInputGate>> inputGates1,
        std::vector<std::shared_ptr<IndexedInputGate>> inputGates2,
        const json& inputTypes,
        std::function<StreamPartitioner<IOReadableWritable>*(int)> getPartitionerFunction,
        std::function<StreamPartitionerV2<StreamRecord>*(int)> getSqlPartitionerFunction);

private:
    std::shared_ptr<CheckpointBarrierHandler> checkpointBarrierHandler;
};
} // namespace omnistream

#endif // OMNISTREAM_OMNITWOINPUTSTREAMTASK_H
