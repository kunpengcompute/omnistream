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

#include "SpanningWrapper.h"
namespace omnistream::datastream {
SpanningWrapper::SpanningWrapper() : buffer_(1024)
{
    lengthBuffer_ = new ByteBuffer(LENGTH_BYTES);
    serializationReadBuffer_ = new DataInputDeserializer();

    recordLength_ = -1;
    accumulatedRecordBytes_ = 0;

    leftOverData_ = nullptr;
    leftOverStart_ = 0;
    leftOverLimit_ = 0;
}

SpanningWrapper::~SpanningWrapper()
{
    delete lengthBuffer_;
    delete serializationReadBuffer_;
}
} // namespace omnistream::datastream
