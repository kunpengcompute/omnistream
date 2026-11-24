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

#ifndef STREAMPARTITIONER_H
#define STREAMPARTITIONER_H

#include <string>
#include <memory>
#include "ChannelSelector.h"

namespace omnistream::datastream {
template <typename T>
class StreamPartitioner : public ChannelSelector<T> {
public:
    StreamPartitioner() : numberOfChannels(0)
    {
    }

    virtual ~StreamPartitioner()
    {
    }

    void setup(int numberOfChannels_) override
    {
        this->numberOfChannels = numberOfChannels_;
    }

    bool isBroadcast() const override
    {
        return false;
    }

    virtual bool isPointWise() const
    {
        return false;
    }
    virtual std::unique_ptr<StreamPartitioner<T>> copy()
    {
        return nullptr;
    }
    [[nodiscard]] virtual std::string toString() const
    {
        return "";
    }

protected:
    int numberOfChannels;
};
} // namespace omnistream::datastream
#endif
