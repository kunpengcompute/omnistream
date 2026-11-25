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

#ifndef RESCALEPARTITIONERV2_H
#define RESCALEPARTITIONERV2_H

#include "StreamPartitionerV2.h"

namespace omnistream {
template <typename T>
class RescalePartitionerV2 : public StreamPartitionerV2<T> {
public:
    RescalePartitionerV2() : nextChannelToSendTo(-1)
    {
    }

    int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
    {
        if (++nextChannelToSendTo >= this->numberOfChannels) {
            nextChannelToSendTo = 0;
        }
        return nextChannelToSendTo;
    }

    int selectRowChannel(RowData* record) override
    {
        if (++nextChannelToSendTo >= this->numberOfChannels) {
            nextChannelToSendTo = 0;
        }
        return nextChannelToSendTo;
    }

    std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() override
    {
        return std::make_unique<SubtaskStateMapper>(UnsupportedMapper());
    }

    std::unique_ptr<SubtaskStateMapper> getUpstreamSubtaskStateMapper() override
    {
        return std::make_unique<SubtaskStateMapper>(UnsupportedMapper());
    }

    std::unique_ptr<StreamPartitionerV2<T>> copy() override
    {
        return std::make_unique<RescalePartitionerV2<T>>(*this);
    }

    bool isPointwise() override
    {
        return true;
    }

    std::string toString() const override
    {
        return "RESCALE";
    }

private:
    mutable int nextChannelToSendTo = -1;
};

} // namespace omnistream

#endif
