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

#include "ObjectBufferConsumerWithPartialRecordLength.h"

#include <sstream>
#include <stdexcept>

namespace omnistream {

ObjectBufferConsumerWithPartialRecordLength::ObjectBufferConsumerWithPartialRecordLength(
    std::shared_ptr<ObjectBufferConsumer> bufferConsumer, int partialRecordLength)
    : bufferConsumer(bufferConsumer),
      partialRecordLength(partialRecordLength)
{
    if (!bufferConsumer) {
        throw std::invalid_argument("bufferConsumer cannot be null");
    }
}

ObjectBufferConsumerWithPartialRecordLength::~ObjectBufferConsumerWithPartialRecordLength()
{
    LOG_TRACE("destructor");
}

std::shared_ptr<ObjectBufferConsumer> ObjectBufferConsumerWithPartialRecordLength::getBufferConsumer()
{
    return bufferConsumer;
}

int ObjectBufferConsumerWithPartialRecordLength::getPartialRecordLength()
{
    return partialRecordLength;
}

VectorBatchBuffer* ObjectBufferConsumerWithPartialRecordLength::build()
{
    return dynamic_cast<VectorBatchBuffer*>(bufferConsumer->build());
}

bool ObjectBufferConsumerWithPartialRecordLength::cleanupPartialRecord()
{
    if (partialRecordLength < 0) {
        throw std::runtime_error("Approximate local recovery does not yet work with unaligned checkpoint!");
    }

    if (partialRecordLength == 0 || !bufferConsumer->isStartOfDataBuffer()) {
        return true;
    }

    if (partialRecordLength > bufferConsumer->getBufferSize()) {
        throw std::runtime_error("Partial record length beyond max buffer capacity!");
    }

    bufferConsumer->skip(partialRecordLength);

    if (partialRecordLength < bufferConsumer->getBufferSize()) {
        return true;
    } else {
        return false;
    }
}

std::string ObjectBufferConsumerWithPartialRecordLength::toString() const
{
    std::stringstream ss;
    ss << "ObjectBufferConsumerWithPartialRecordLength{";
    ss << "bufferConsumer=" << (bufferConsumer ? "present" : "null") << ", ";
    ss << "partialRecordLength=" << partialRecordLength;
    ss << "}";
    return ss.str();
}

} // namespace omnistream
