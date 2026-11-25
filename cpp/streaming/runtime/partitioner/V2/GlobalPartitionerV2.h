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

#ifndef GLOBALPARTITIONERV2_H
#define GLOBALPARTITIONERV2_H

#include "StreamPartitionerV2.h"

namespace omnistream {
template <typename T>
class GlobalPartitionerV2 : public StreamPartitionerV2<T> {
public:
    GlobalPartitionerV2()
    {
    }

    int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
    {
        return 0;
    }

    int selectRowChannel(RowData* record) override
    {
        return 0;
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
        return std::make_unique<GlobalPartitionerV2<T>>(*this);
    }

    bool isPointwise() override
    {
        return false;
    }

    std::string toString() const override
    {
        return "GLOBAL";
    }
};

} // namespace omnistream

#endif
