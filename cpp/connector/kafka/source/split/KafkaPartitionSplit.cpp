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

#include "KafkaPartitionSplit.h"
#include <stdexcept>
#include <sstream>
#include <limits>

const long KafkaPartitionSplit::NO_STOPPING_OFFSET = std::numeric_limits<long>::min();
const long KafkaPartitionSplit::LATEST_OFFSET = -1;
const long KafkaPartitionSplit::EARLIEST_OFFSET = -2;
const long KafkaPartitionSplit::COMMITTED_OFFSET = -3;

const std::set<long> KafkaPartitionSplit::VALID_STARTING_OFFSET_MARKERS = {
    KafkaPartitionSplit::EARLIEST_OFFSET, KafkaPartitionSplit::LATEST_OFFSET, KafkaPartitionSplit::COMMITTED_OFFSET};

const std::set<long> KafkaPartitionSplit::VALID_STOPPING_OFFSET_MARKERS = {
    KafkaPartitionSplit::LATEST_OFFSET, KafkaPartitionSplit::COMMITTED_OFFSET, KafkaPartitionSplit::NO_STOPPING_OFFSET};

KafkaPartitionSplit::KafkaPartitionSplit(const std::shared_ptr<RdKafka::TopicPartition>& tp, long startingOffset)
    : KafkaPartitionSplit(tp, startingOffset, NO_STOPPING_OFFSET)
{
}

KafkaPartitionSplit::KafkaPartitionSplit(
    const std::shared_ptr<RdKafka::TopicPartition>& tp, long startingOffset, long stoppingOffset)
    : tp_(tp),
      startingOffset_(startingOffset),
      stoppingOffset_(stoppingOffset)
{
    verifyInitialOffset(tp, startingOffset, stoppingOffset);
}

std::string KafkaPartitionSplit::getTopic() const
{
    return tp_->topic();
}

int KafkaPartitionSplit::getPartition() const
{
    return tp_->partition();
}

const std::shared_ptr<RdKafka::TopicPartition>& KafkaPartitionSplit::getTopicPartition() const
{
    return tp_;
}

int64_t KafkaPartitionSplit::getStartingOffset() const
{
    return startingOffset_;
}

int64_t KafkaPartitionSplit::getStoppingOffset() const
{
    return stoppingOffset_;
}

std::string KafkaPartitionSplit::splitId() const
{
    return toSplitId(tp_.get());
}

std::string KafkaPartitionSplit::toSplitId(RdKafka::TopicPartition* tp)
{
    return tp->topic() + "-" + std::to_string(tp->partition());
}

void KafkaPartitionSplit::verifyInitialOffset(
    const std::shared_ptr<RdKafka::TopicPartition>& tp, long startingOffset, long stoppingOffset)
{
    if (startingOffset < 0 &&
        VALID_STARTING_OFFSET_MARKERS.find(startingOffset) == VALID_STARTING_OFFSET_MARKERS.end()) {
        std::ostringstream oss;
        oss << "Invalid starting offset " << startingOffset << " is specified for partition "
            << tp->topic() + "-" + std::to_string(tp->partition())
            << ". It should either be non-negative or be one of the [" << EARLIEST_OFFSET << "(earliest), "
            << LATEST_OFFSET << "(latest), " << COMMITTED_OFFSET << "(committed)].";
        throw std::runtime_error(oss.str());
    }

    if (stoppingOffset < 0 &&
        VALID_STOPPING_OFFSET_MARKERS.find(stoppingOffset) == VALID_STOPPING_OFFSET_MARKERS.end()) {
        std::ostringstream oss;
        oss << "Illegal stopping offset " << stoppingOffset << " is specified for partition "
            << tp->topic() + "-" + std::to_string(tp->partition())
            << ". It should either be non-negative or be one of the [" << LATEST_OFFSET << "(latest), "
            << COMMITTED_OFFSET << "(committed), " << NO_STOPPING_OFFSET << "(Long.MIN_VALUE, no_stopping_offset)].";
        throw std::runtime_error(oss.str());
    }
}
