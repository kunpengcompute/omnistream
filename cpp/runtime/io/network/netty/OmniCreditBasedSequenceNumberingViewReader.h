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
#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include "NettyBufferPool.h"
#include "common.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/watermark/Watermark.h"
#include "partition/BufferAvailabilityListener.h"
#include "partition/ResultPartitionManager.h"
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"
#include "table/utils/VectorBatchSerializationUtils.h"
#include <buffer/NetworkBuffer.h>
namespace ds = ::datastream;
namespace omnistream {
using ::datastream::NetworkBuffer;

struct LongHash {
    size_t operator()(int64_t key) const
    {
        return std::hash<int64_t>{}(key);
    }
};

struct LongEqual {
    bool operator()(int64_t a, int64_t b) const
    {
        return a == b;
    }
};

class OmniCreditBasedSequenceNumberingViewReader : public BufferAvailabilityListener {
public:
    OmniCreditBasedSequenceNumberingViewReader(
        ResultPartitionIDPOD partitionId, int subPartitionIndex, long outputBufferStatus, bool downStreamIsNative);
    ~OmniCreditBasedSequenceNumberingViewReader() override;
    void notifyDataAvailable() override;
    void requestSubpartitionView(
        std::shared_ptr<ResultPartitionManager> resultPartitionManager,
        ResultPartitionIDPOD partitionId,
        int subPartitionId);
    int getAvailabilityAndBacklog();
    void getNextBufferInternal();
    int getNextBuffer();
    void RecycleNettyBuffer(long address);
    std::shared_ptr<NettyBufferInfo> RequestNettyBuffer(int expectedBufferSize);
    bool SerializeVectorBatch(VectorBatch* vectorBatch, std::shared_ptr<NettyBufferInfo>& bufferInfo);
    void DoSerializeVectorBatch(VectorBatch* vectorBatch, int vectorSize, std::shared_ptr<NettyBufferInfo>& bufferInfo);
    void AddNettyBufferInfoToQueue(std::shared_ptr<NettyBufferInfo>& bufferInfo);
    bool DoSerializeWaterMark(long timestamp, std::shared_ptr<NettyBufferInfo> bufferInfo);
    void SerializeBufferAndBacklog(VectorBatchBuffer* vectorBatchBuffer);
    void SerializeEvent(VectorBatchBuffer* vectorBatchBuffer);
    void SerializeVectorBatchBuffer(VectorBatchBuffer* vectorBatchBuffer);
    void RecycleNetworkBuffer(int64_t address);

    void ResumeConsumption();
    void SerializeVectorBatchBufferToRowData(VectorBatchBuffer* vectorBatchBuffer);

private:
    bool downStreamIsNative = true;
    std::atomic<bool> stopped_{false};
    std::shared_ptr<ResultSubpartitionView> subpartitionView;
    std::queue<std::shared_ptr<SerializedBatchInfo>> serializedBatchQueue;
    OutputBufferStatus* outputBufferStatus;
    std::recursive_mutex queueMutex;
    int nextDataAvailable = 0;
    std::unique_ptr<NettyBufferPool> nettyBufferPool;
    int bufferPoolSize = 100;
    // 32k
    int bufferSize = 32 * 1024;
    std::recursive_mutex fetchingDataMutex;
    int requestNextBufferWaitingTime = 10;
    int noBufferPrintCount = 200;
    std::unordered_map<int64_t, NetworkBuffer*> networkBufferPendingRecycling;
    std::recursive_mutex recycleNetworkBufferMutex;
};
} // namespace omnistream
