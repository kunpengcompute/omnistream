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

#include "RecordWriterV2.h"
#include "configuration/ExecutionOptions.h"
#include "common.h"

namespace omnistream {

const std::string RecordWriterV2::DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

RecordWriterV2::RecordWriterV2(
    const std::shared_ptr<ResultPartitionWriter>& targetPartitionWriter,
    long timeout_,
    std::string taskName_,
    int taskType)
    : targetPartitionWriter_(targetPartitionWriter),
      taskName(taskName_),
      outputFlusher(nullptr),
      taskType(taskType)
{
    this->numberOfChannels = targetPartitionWriter_->getNumberOfSubpartitions();
    this->timeout = timeout_;
    this->flushAlways = (this->timeout == ExecutionOptions::FLUSH_AFTER_EVERY_RECORD);
}

RecordWriterV2::~RecordWriterV2()
{
    // Tracing the teardown chain: OmniTask -> OmniStreamTask -> delegate -> here -> ~OutputFlusher.
    INFO_RELEASE("~RecordWriterV2 for " << taskName);
}

void RecordWriterV2::postConstruct()
{
    // share point of recored write , fix it later
    this->outputFlusher = std::make_shared<OutputFlusher>(taskName, timeout, this);
    this->outputFlusher->start();
}

std::shared_ptr<ResultPartitionWriter> RecordWriterV2::getTargetPartition() const
{
    return targetPartitionWriter_;
}

int RecordWriterV2::getNumberOfChannels() const
{
    return numberOfChannels;
}

bool RecordWriterV2::isFlushAlways() const
{
    return flushAlways;
}

void RecordWriterV2::setSerializationDelegate(SerializationDelegate* serializationDelegate_)
{
    this->serializationDelegate = serializationDelegate_;
}

void RecordWriterV2::broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent)
{
    targetPartitionWriter_->broadcastEvent(event, isPriorityEvent);
    if (flushAlways) {
        flushAll();
    }
}

void RecordWriterV2::cancel()
{
    LOG_INFO_IMP("RecordWriterV2::cancel" << taskName);
    // Stop the flusher here too. Source tasks take the cancel path, and without this their flusher
    // thread only stops when this writer is destroyed.
    if (outputFlusher) {
        outputFlusher->terminate();
    }
    targetPartitionWriter_->cancel();
    INFO_RELEASE("Task:" << taskName << " Total number of row emitted:" << counter_);
}

void RecordWriterV2::close()
{
    LOG_INFO_IMP("RecordWriterV2::close" << taskName);
    // Null until postConstruct() runs, so a close before then must not dereference it.
    if (outputFlusher) {
        outputFlusher->terminate();
    }
    INFO_RELEASE("Task:" << taskName << " Total number of row emitted:" << counter_);
}

std::shared_ptr<CompletableFuture> RecordWriterV2::GetAvailableFuture()
{
    return targetPartitionWriter_->GetAvailableFuture();
}
} // namespace omnistream
