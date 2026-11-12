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

#ifndef OMNILOCALCHANNELREADER_H
#define OMNILOCALCHANNELREADER_H
#include <memory>
#include <common.h>
#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include "partition/BufferAvailabilityListener.h"
#include "partition/ResultPartitionManager.h"
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"
#include <buffer/ReadOnlySlicedNetworkBuffer.h>
#include "buffer/ReadOnlySlicedVectorBatchBuffer.h"
#include "table/data/RowData.h"
#include "table/data/vectorbatch/VectorBatch.h"

namespace omnistream {
struct MemorySegmentInfo {
    uint64_t memorySegmentAddress;
    int32_t readIndex;
    int32_t length;
    int32_t currentDataType;
    int32_t backlog;
    int32_t nextDataType;
    int32_t sequenceNumber;
};

class OmniLocalChannelReader : public BufferAvailabilityListener {
public:
    OmniLocalChannelReader(
        ResultPartitionIDPOD partitionId,
        int subPartitionIndex,
        long outputBufferStatus,
        std::string taskNameWithSubtas);
    ~OmniLocalChannelReader() override;

    void requestSubpartitionView(
        std::shared_ptr<ResultPartitionManager> resultPartitionManager,
        ResultPartitionIDPOD partitionId,
        int subPartitionId);

    void notifyDataAvailable();
    bool checkIfDataAvailable();
    uint8_t getNextBuffer();
    void recycleMemorySegment(long memorySegmentAddress);
    void Close();
    void ReleaseAllResources();
    void WrapBufferInfoIntoMemorySegmentInfo(
        ::datastream::ReadOnlySlicedNetworkBuffer* nBuffer, BufferAndBacklog* bufferAndLog);
    void WrapBufferInfoIntoBinaryRowDataInfo(omnistream::VectorBatchBuffer* nBuffer, BufferAndBacklog* bufferAndLog);
    void ResumeConsumption();

private:
    int calculateTotalRows(ObjectSegment* objectSegment, int offset, int vbNum);
    void setRowDataToPtr(
        RowData* binaryRowData,
        uint8_t* dataResultContainer,
        unsigned int& position,
        int vectorBatchCol,
        VectorBatch* element,
        int index);

    ResultPartitionIDPOD partitionId_;
    int subPartitionIndex_;
    std::shared_ptr<ResultSubpartitionView> subpartitionView;
    std::recursive_mutex createViewMutex;
    std::recursive_mutex fetchingDataMutex;
    MemorySegmentInfo* memorySegmentInfo;
    std::condition_variable_any dataAvailableCondition;
    std::recursive_mutex dataAvailableMutex;
    std::atomic<bool> dataAvailable = false;
    struct PendingSqlBuffer {
        Buffer* buffer;
        uint8_t* container;
        size_t remainingRows;
    };
    std::unordered_map<uint64_t, Buffer*> pendingRecyclingBufferMap;
    std::unordered_map<uint64_t, std::shared_ptr<PendingSqlBuffer>> pendingSqlRows;
    std::recursive_mutex recycleBufferMutex;
    std::atomic<bool> isStopped = false;
    std::string taskNameWithSubtask_;
};
} // namespace omnistream
#endif // OMNILOCALCHANNELREADER_H
