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

#ifndef OMNISTREAM_CHECKPOINTEDRESULTSUBPARTITION_H
#define OMNISTREAM_CHECKPOINTEDRESULTSUBPARTITION_H
#include "ResultSubpartitionInfoPOD.h"
#include "runtime/buffer/BufferBuilder.h"
namespace omnistream {
class CheckpointedResultSubpartition {
public:
    virtual const ResultSubpartitionInfoPOD& getSubpartitionInfo() = 0;

    virtual BufferBuilder* requestBufferBuilderBlocking() = 0;

    virtual void addRecovered(std::shared_ptr<BufferConsumer> bufferConsumer) = 0;

    virtual void finishReadRecoveredState(bool notifyAndBlockOnCompletion) = 0;
};
} // namespace omnistream
#endif // OMNISTREAM_CHECKPOINTEDRESULTSUBPARTITION_H
