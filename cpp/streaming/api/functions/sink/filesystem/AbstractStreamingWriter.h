/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAM_ABSTRACT_STREAMING_WRITER_H
#define OMNISTREAM_ABSTRACT_STREAMING_WRITER_H

#include <memory>
#include <map>
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "Buckets.h"
#include "StreamingFileSinkHelper.h"
#include "StreamingFileSink.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "core/fs/Path.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"
#include "runtime/checkpoint/CheckpointOptions.h"

template <typename IN, typename OUT>
class AbstractStreamingWriter : public AbstractStreamOperator<OUT>, public OneInputStreamOperator {
public:
    AbstractStreamingWriter(long bucketCheckInterval, BulkFormatBuilder<IN, std::string>* bucketsBuilder)
        : buckets(nullptr),
          helper(nullptr),
          procTimeService(nullptr),
          currentWatermark(LONG_MIN),
          bucketCheckInterval(bucketCheckInterval),
          bucketsBuilder(bucketsBuilder)
    {
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        AbstractStreamOperator<OUT>::initializeState(initializer, keySerializer);
        // initializeState 可能被重复调用，先释放上一轮的定时服务和运行时对象，避免线程与定时器残留
        releaseRuntime();
        buckets = bucketsBuilder->createBuckets(this->getRuntimeContext()->getIndexOfThisSubtask());
        procTimeService = new SystemProcessingTimeService();
        helper = new StreamingFileSinkHelper<IN>(buckets, procTimeService, bucketCheckInterval);
    }

    void processWatermark(Watermark* mark)
    {
        if (this->timeServiceManager != nullptr) {
            this->timeServiceManager->advanceWatermark(mark);
        }
        this->output->emitWatermark(mark);
        currentWatermark = mark->getTimestamp();
        if (currentWatermark == LONG_MAX) {
            endInput();
        }
    }

    void processBatch(StreamRecord* element) override
    {
        auto batch = reinterpret_cast<omnistream::VectorBatch*>(element->getValue());

        for (int rowId = 0; rowId < batch->GetRowCount(); rowId++) {
            helper->onElement(batch, rowId, element->hasTimestamp() ? element->getTimestamp() : 0, currentWatermark);
        }
    }

    void endInput()
    {
        buckets->onProcessingTime(LONG_MAX);
        helper->close();
    }

    void snapshotState(long checkpointId)
    {
        buckets->snapshotState();
        LOG("AbstractStreamingWriter::snapshotState checkpointId=" << checkpointId);
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        AbstractStreamOperator<OUT>::notifyCheckpointComplete(checkpointId);
        if (buckets) {
            buckets->notifyCheckpointComplete(checkpointId);
        }
    }

    OperatorSnapshotFutures* SnapshotState(
        long checkpointId,
        long timestamp,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<CheckpointStreamFactory> storageLocation,
        const std::shared_ptr<OmniTaskBridge>& bridge) override
    {
        auto result = AbstractStreamOperator<OUT>::SnapshotState(
            checkpointId, timestamp, checkpointOptions, storageLocation, bridge);
        if (buckets) {
            buckets->snapshotState();
        }
        return result;
    }

    void PrepareSnapshotPreBarrier(long checkpointId) override
    {
        if (buckets) {
            buckets->onProcessingTime(LONG_MAX);
        }
    }

    void open() override {};

    void close() override
    {
        AbstractStreamOperator<OUT>::close();
        releaseRuntime();
    }

    virtual ~AbstractStreamingWriter()
    {
        close();
    }

    std::string getTypeName() override
    {
        std::string typeName = "AbstractStreamingWriter";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

private:
    void releaseRuntime()
    {
        // bucket 检查定时器每隔 bucketCheckInterval 会以 helper 为回调重新注册自己，所以必须先停服务
        // （内部会 join worker 线程）再删 helper，否则 worker 会在 helper 释放之后回调到已释放内存。
        if (procTimeService != nullptr) {
            procTimeService->shutdownService();
            delete procTimeService;
            procTimeService = nullptr;
        }
        if (helper != nullptr) {
            helper->close();
            delete helper;
            helper = nullptr;
        }
        if (buckets != nullptr) {
            delete buckets;
            buckets = nullptr;
        }
    }

protected:
    Buckets<IN, std::string>* buckets;
    StreamingFileSinkHelper<IN>* helper;
    SystemProcessingTimeService* procTimeService;
    long currentWatermark;

    virtual void partitionCreated(const std::string& partition)
    {
    }

    virtual void partitionInactive(const std::string& partition)
    {
    }

    virtual void onPartFileOpened(const std::string& partition, const std::string& newPath)
    {
    }

    virtual void commitUpToCheckpoint(long checkpointId)
    {
        if (helper) {
            helper->commitUpToCheckpoint(checkpointId);
        }
    }

private:
    long bucketCheckInterval;
    BulkFormatBuilder<IN, std::string>* bucketsBuilder;
};

#endif // OMNISTREAM_ABSTRACT_STREAMING_WRITER_H
