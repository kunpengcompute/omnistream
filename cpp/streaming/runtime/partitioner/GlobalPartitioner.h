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

#ifndef OMNISTREAM_GLOBALPARTITIONER_H
#define OMNISTREAM_GLOBALPARTITIONER_H

#include "StreamPartitioner.h"
namespace omnistream::datastream {
template <typename T>
class GlobalPartitioner : public StreamPartitioner<T> {
public:
    GlobalPartitioner() {};

    ~GlobalPartitioner() {};

    void setup(int numberOfChannels) override
    {
        StreamPartitioner<T>::setup(numberOfChannels);
    }

    int selectChannel(T* record) override
    {
        return 0;
    };

    bool isPointWise() const override
    {
        return true;
    };

    std::string toString() const
    {
        return "GLOBAL";
    };

    std::unique_ptr<StreamPartitioner<T>> copy() override
    {
        return std::make_unique<GlobalPartitioner<T>>(*this);
    }
};
} // namespace omnistream::datastream
#endif // OMNISTREAM_GLOBALPARTITIONER_H
