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

#ifndef BUFFERCONSUMERWITHPARTIALRECORDLENGTH_H
#define BUFFERCONSUMERWITHPARTIALRECORDLENGTH_H

#include <memory>

#include "BufferConsumer.h"

namespace omnistream {
class BufferConsumerWithPartialRecordLength {
public:
    BufferConsumerWithPartialRecordLength(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength);
    // delete in PipelinedSubpartition
    ~BufferConsumerWithPartialRecordLength();

    std::shared_ptr<BufferConsumer> getBufferConsumer();
    int getPartialRecordLength();
    Buffer* build();
    bool cleanupPartialRecord();
    std::string toString() const;

private:
    std::shared_ptr<BufferConsumer> bufferConsumer;
    int partialRecordLength;
};
} // namespace omnistream

#endif // BUFFERCONSUMERWITHPARTIALRECORDLENGTH_H
