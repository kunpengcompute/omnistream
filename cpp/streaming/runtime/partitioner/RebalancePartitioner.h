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

#ifndef REBALANCEPARTITIONER_H
#define REBALANCEPARTITIONER_H

#include <random>
#include "StreamPartitioner.h"

namespace omnistream::datastream {
template <typename T>
class RebalancePartitioner : public StreamPartitioner<T> {
public:
    RebalancePartitioner() : randomEngine(std::random_device{}())
    {
    }

    void setup(int numberOfChannels) override
    {
        StreamPartitioner<T>::setup(numberOfChannels);
        std::uniform_int_distribution<int> distribution(0, this->numberOfChannels - 1);
        nextChannelToSendTo = distribution(randomEngine);
    }

    int selectChannel(T* record) override
    {
        nextChannelToSendTo = (nextChannelToSendTo + 1) % this->numberOfChannels;
        return nextChannelToSendTo;
    }

    std::unique_ptr<StreamPartitioner<T>> copy() override
    {
        return std::make_unique<RebalancePartitioner<T>>(*this);
    }

    bool isPointWise() const override
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
} // namespace omnistream::datastream

#endif // REBALANCEPARTITIONER_H
