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

#ifndef RECORDWRITERV2_H
#define RECORDWRITERV2_H

#include <string>

// Include necessary headers for dependencies
#include <io/IOReadableWritable.h>
#include <partition/ResultPartitionWriter.h>
#include "streaming/api/watermark/Watermark.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "runtime/watermark/WatermarkStatus.h"

#include "OutputFlusher.h" // Assuming this defines OutputFlusher

class SerializationDelegate;

namespace omnistream {

class RecordWriterV2 : public AvailabilityProvider, public std::enable_shared_from_this<RecordWriterV2> {
public:
    static const std::string DEFAULT_OUTPUT_FLUSH_THREAD_NAME;

    RecordWriterV2(
        const std::shared_ptr<ResultPartitionWriter>& targetPartitionWriter,
        long timeout,
        std::string taskName,
        int taskType);

    void postConstruct();

    virtual ~RecordWriterV2();

    std::shared_ptr<ResultPartitionWriter> getTargetPartition() const;
    int getNumberOfChannels() const;

    bool isFlushAlways() const;
    OutputFlusher* getOutputFlusher() const;

    virtual void emit(StreamRecord* record) = 0;
    virtual void broadcastEmit(Watermark* watermark) = 0;
    virtual void broadcastEmit(WatermarkStatus* status)
    {
        NOT_IMPL_EXCEPTION;
    }
    virtual void flushAll()
    {
        LOG_TRACE(">>>>");
        targetPartitionWriter_->flushAll();
    }

    void setSerializationDelegate(SerializationDelegate* serializationDelegate_);

    virtual void cancel();
    virtual void close();

    void broadcastEvent(std::shared_ptr<AbstractEvent> event)
    {
        broadcastEvent(event, false);
    }

    void broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriority);

    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;

protected:
    virtual void emit(StreamRecord* record, int targetSubpartition) = 0;

    SerializationDelegate* serializationDelegate = nullptr;

    std::shared_ptr<ResultPartitionWriter> targetPartitionWriter_;
    int numberOfChannels;

    std::string taskName;

    bool flushAlways = false;
    std::shared_ptr<OutputFlusher> outputFlusher;

    // 1 native sql task, 2 native datastream task. 3 future - hybrid java+cpp source task
    int taskType;
    int timeout = 100; // todo: it needs to be assigned a value.

    int32_t counter_{0};
};

} // namespace omnistream

#endif
