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

#ifndef OMNISTREAM_OBJECTBUFFERCONSUMERWITHPARTIALRECORDLENGTH_H
#define OMNISTREAM_OBJECTBUFFERCONSUMERWITHPARTIALRECORDLENGTH_H

#include <memory>
#include <string>

#include "ObjectBuffer.h"
#include "ObjectBufferConsumer.h"
#include "VectorBatchBuffer.h"

namespace omnistream {

class ObjectBufferConsumerWithPartialRecordLength {
public:
    ObjectBufferConsumerWithPartialRecordLength(
        std::shared_ptr<ObjectBufferConsumer> bufferConsumer, int partialRecordLength);
    ~ObjectBufferConsumerWithPartialRecordLength();

    std::shared_ptr<ObjectBufferConsumer> getBufferConsumer();
    int getPartialRecordLength();
    VectorBatchBuffer* build();
    bool cleanupPartialRecord();
    std::string toString() const;

private:
    std::shared_ptr<ObjectBufferConsumer> bufferConsumer;
    int partialRecordLength;
};

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERCONSUMERWITHPARTIALRECORDLENGTH_H
