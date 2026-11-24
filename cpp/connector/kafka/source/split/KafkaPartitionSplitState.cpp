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

#include "KafkaPartitionSplitState.h"

KafkaPartitionSplitState::KafkaPartitionSplitState(KafkaPartitionSplit* partitionSplit)
    : KafkaPartitionSplit(
          partitionSplit->getTopicPartition(),
          partitionSplit->getStartingOffset(),
          partitionSplit->getStoppingOffset()),
      currentOffset(partitionSplit->getStartingOffset())
{
}

long KafkaPartitionSplitState::getCurrentOffset() const
{
    return currentOffset;
}

void KafkaPartitionSplitState::setCurrentOffset(long currentOffset_)
{
    this->currentOffset = currentOffset_;
}

KafkaPartitionSplit KafkaPartitionSplitState::toKafkaPartitionSplit() const
{
    return KafkaPartitionSplit(getTopicPartition(), getCurrentOffset(), getStoppingOffset());
}
