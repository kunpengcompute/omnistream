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

#ifndef FORWARDPARTITIONERV2_H
#define FORWARDPARTITIONERV2_H

#include "StreamPartitionerV2.h"

namespace omnistream {
template <typename T>
class ForwardPartitionerV2 : public StreamPartitionerV2<T> {
public:
    // result should be vector
    int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
    {
        return 0;
    }

    int selectRowChannel(RowData* record) override
    {
        return 0;
    }

    std::unique_ptr<StreamPartitionerV2<T>> copy() override
    {
        return std::make_unique<ForwardPartitionerV2<T>>(*this);
    }

    bool isPointwise() override
    {
        return true;
    }

    [[nodiscard]] std::string toString() const override
    {
        return "FORWARD";
    }

    std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() override
    {
        return std::make_unique<SubtaskStateMapper>(UnsupportedMapper());
    }

    std::unique_ptr<SubtaskStateMapper> getUpstreamSubtaskStateMapper() override
    {
        return std::make_unique<SubtaskStateMapper>(UnsupportedMapper());
    }
};
} // namespace omnistream

#endif
