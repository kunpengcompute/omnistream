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

#ifndef SHUFFLEPARTITIONERV2_H
#define SHUFFLEPARTITIONERV2_H

#include <random>
#include "StreamPartitionerV2.h"

namespace omnistream {
template <typename T>
class ShufflePartitionerV2 : public StreamPartitionerV2<T> {
public:
    ShufflePartitionerV2() : randomEngine(std::random_device{}())
    {
    }

    ShufflePartitionerV2(const ShufflePartitionerV2& other) : randomEngine(other.randomEngine)
    {
    }

    ShufflePartitionerV2& operator=(const ShufflePartitionerV2& other)
    {
        randomEngine = other.randomEngine;
    }

    int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
    {
        std::uniform_int_distribution<int> distribution(0, this->numberOfChannels - 1);
        return distribution(randomEngine);
    }

    std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() override
    {
        return std::make_unique<SubtaskStateMapper>(RoundRobinMapper());
    }

    std::unique_ptr<StreamPartitionerV2<T>> copy() override
    {
        return std::make_unique<ShufflePartitionerV2<T>>(*this);
    }

    bool isPointwise() override
    {
        return false;
    }

    [[nodiscard]] std::string toString() const override
    {
        return "SHUFFLE";
    }

private:
    std::default_random_engine randomEngine;
};
} // namespace omnistream

#endif
