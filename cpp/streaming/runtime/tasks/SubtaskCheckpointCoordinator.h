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

#ifndef OMNISTREAM_SUBTASKCHECKPOINTCOORDINATOR_H
#define OMNISTREAM_SUBTASKCHECKPOINTCOORDINATOR_H

#include <memory>

#include "runtime/checkpoint/CheckpointMetaData.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/CheckpointMetricsBuilder.h"
#include "OperatorChain.h"

namespace omnistream {
class SubtaskCheckpointCoordinator {
public:
    virtual ~SubtaskCheckpointCoordinator() = default;

    /**
     * Triggers a checkpoint asynchronously (typically for source tasks)
     *
     * @param checkpointMetaData Meta data about this checkpoint
     * @param checkpointOptions Options for performing this checkpoint
     * @return future with value of false if checkpoint was not carried out, true otherwise
     */
    virtual void InitInputsCheckpoint(long id, CheckpointOptions* checkpointOptions) = 0;

    virtual std::shared_ptr<ChannelStateWriter> getChannelStateWriter() = 0;

    virtual void checkpointState(
        std::shared_ptr<CheckpointMetaData> metadata,
        std::shared_ptr<CheckpointOptions> options,
        std::shared_ptr<CheckpointMetricsBuilder> metrics,
        omnistream::OperatorChainV2* operatorChain,
        bool isTaskFinished,
        std::shared_ptr<omnistream::Supplier<bool>> isRunning) {};
    virtual void SetisRecoveredFlag(bool flag) {};
};
} // namespace omnistream
#endif // OMNISTREAM_SUBTASKCHECKPOINTCOORDINATOR_H
