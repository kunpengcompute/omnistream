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

#ifndef CHECKPOINTED_RESULT_PARTITION_H
#define CHECKPOINTED_RESULT_PARTITION_H

#include "partition/PipelinedSubpartition.h"

/**
 * Interface for partitions that are checkpointed, meaning they store data as part of unaligned
 * checkpoints.
 */
class CheckpointedResultPartition {
public:
    virtual ~CheckpointedResultPartition() = default;

    /** Gets the checkpointed subpartition with the given subpartitionIndex. */
    virtual std::shared_ptr<omnistream::PipelinedSubpartition> getCheckpointedSubpartition(int subpartitionIndex) = 0;

    virtual void finishReadRecoveredState(bool notifyAndBlockOnCompletion) = 0;
};

#endif // CHECKPOINTED_RESULT_PARTITION_H
