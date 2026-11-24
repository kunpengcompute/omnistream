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

#ifndef OMNISTREAM_DUMMYSTREAMPARTITIONER_H

#define OMNISTREAM_DUMMYSTREAMPARTITIONER_H

#include "runtime/io/network/api/writer/RecordWriter.h"
#include "streaming/runtime/io/RecordWriterOutput.h"

// using namespace omnistream;

class DummyStreamPartitioner : public omnistream::datastream::StreamPartitioner<IOReadableWritable> {
public:
    bool isPointWise() const override
    {
        return false;
    }

    void setup(int numberOfChannels) override
    {
    }

    int selectChannel(IOReadableWritable* record) override
    {
        return 0;
    }

    bool isBroadcast() const override
    {
        return false;
    }

    virtual std::unique_ptr<StreamPartitioner<IOReadableWritable>> copy()
    {
        return nullptr;
    };

    [[nodiscard]] virtual std::string toString() const
    {
        return {};
    }
};

#endif // OMNISTREAM_DUMMYSTREAMPARTITIONER_H
