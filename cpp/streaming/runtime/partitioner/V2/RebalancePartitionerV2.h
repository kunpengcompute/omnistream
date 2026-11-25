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

#ifndef REBALANCEPARTITIONERV2_H
#define REBALANCEPARTITIONERV2_H

#include <random>

#include "table/data/vectorbatch/VectorBatch.h"
#include "StreamPartitionerV2.h"
#include "vector/vector_helper.h"

namespace omnistream {
template <typename T>
class RebalancePartitionerV2 : public StreamPartitionerV2<T> {
public:
    RebalancePartitionerV2() : randomEngine(std::random_device{}())
    {
    }

    void setup(int numberOfChannels) override
    {
        StreamPartitionerV2<T>::setup(numberOfChannels);
        std::uniform_int_distribution<int> distribution(0, this->numberOfChannels - 1);
        nextChannelToSendTo = distribution(randomEngine);
    }

    int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
    {
        nextChannelToSendTo = (nextChannelToSendTo + 1) % this->numberOfChannels;
        return nextChannelToSendTo;
    }

    int selectRowChannel(RowData* record) override
    {
        nextChannelToSendTo = (nextChannelToSendTo + 1) % this->numberOfChannels;
        return nextChannelToSendTo;
    }

    std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() override
    {
        return std::make_unique<SubtaskStateMapper>(RoundRobinMapper());
    }

    std::unique_ptr<SubtaskStateMapper> getUpstreamSubtaskStateMapper() override
    {
        return std::make_unique<SubtaskStateMapper>(ArbitraryMapper());
    }

    std::unique_ptr<StreamPartitionerV2<T>> copy() override
    {
        return std::make_unique<RebalancePartitionerV2<T>>(*this);
    }

    bool isPointwise() override
    {
        return false;
    }

    [[nodiscard]] std::string toString() const override
    {
        return "REBALANCE";
    }

private:
    int nextChannelToSendTo;
    std::default_random_engine randomEngine;
};
} // namespace omnistream

#endif
