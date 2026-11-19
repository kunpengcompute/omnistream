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
        // initializeState 可能被重复调用：先按"关闭"语义回收上一轮的 helper/buckets，
        // 避免泄漏，以及上一轮未 finalize 的 part 文件一直挂在文件系统里。
        closeRuntime();
        buckets = bucketsBuilder->createBuckets(this->getRuntimeContext()->getIndexOfThisSubtask());
        // 定时器挂在 task 的 processing time service 上（由 StreamOperatorFactory::CreateStreamingFileWriterOp 注入）：
        // 回调经 mailbox 投递、在算子主线程执行，与 processBatch/endInput/snapshotState 天然串行。
        // 自建 SystemProcessingTimeService 会引入独立 worker 线程，与算子主线程并发访问 Buckets，导致 coredump。
        procTimeService = this->getProcessingTimeService();
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
        // 没有 endInput 的路径（流式作业 cancel、任务失败、JM 主动关闭）靠这里把未完成的 part 文件落盘收尾。
        closeRuntime();
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
    // 回收 sink 的运行时对象：
    //   1. helper->close() → Buckets::commitUpToCheckpoint(-1) → 每个活跃 bucket 的
    //      onProcessingTime(LONG_MAX)，即把正在写的 part 文件 finalize（与 endInput 同一语义）；
    //   2. 释放 helper 与 bucket 树。
    // 不在这里关闭定时器服务：processingTimeService 由 StreamOperatorFactory 注入、归 task 所有，
    // 由 ~OmniStreamTask 统一 shutdown（其回调只投递到 mailbox，这里也不会再有残留回调执行）。
    void closeRuntime()
    {
        procTimeService = nullptr;
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
    ProcessingTimeService* procTimeService;
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
