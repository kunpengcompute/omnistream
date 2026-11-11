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

#include "BufferWritingResultPartition.h"

#include <limits>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <vector>
#include <core/utils/ByteBuffer.h>
#include <streaming/runtime/streamrecord/StreamRecord.h>
#include <streaming/api/watermark/Watermark.h>

#include "PipelinedSubpartition.h"
#include "buffer/LocalObjectBufferPool.h"
#include "io/network/api/serialization/EventSerializer.h"
#include "runtime/buffer/MemoryBufferBuilder.h"
// check
// broadcast ability miss
#include "runtime/metrics/groups/VectorBatchBufferPoolMetricGroup.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/util/VectorBatchUtil.h"

namespace omnistream {
namespace {
constexpr int32_t MAX_SQL_BATCH_BYTES = 2 * 1024 * 1024;
constexpr int32_t MAX_SQL_SINGLE_ROW_BYTES = 8 * 1024 * 1024;

struct SqlBatchRecordDeleter {
    void operator()(StreamRecord* record) const
    {
        if (record != nullptr) {
            delete static_cast<VectorBatch*>(record->getValue());
            delete record;
        }
    }
};

using SqlBatchRecord = std::unique_ptr<StreamRecord, SqlBatchRecordDeleter>;

SqlBatchRecord CopySqlBatchRows(StreamRecord* source, int firstRow, int rowCount)
{
    auto* batch = static_cast<VectorBatch*>(source->getValue());
    std::vector<int32_t> rowIds(rowCount);
    std::iota(rowIds.begin(), rowIds.end(), firstRow);
    std::unique_ptr<VectorBatch> copy(VectorBatchUtil::buildNewVectorBatchByRowIds(batch, rowIds));
    SqlBatchRecord record(
        source->hasTimestamp() ? new StreamRecord(copy.get(), source->getTimestamp()) : new StreamRecord(copy.get()));
    copy.release();
    return record;
}
} // namespace

// origin realization
BufferWritingResultPartition::BufferWritingResultPartition(
    const std::string& owningTaskName,
    int partitionIndex,
    const ResultPartitionIDPOD& partitionId,
    int partitionType,
    std::vector<std::shared_ptr<ResultSubpartition>> subpartitions,
    int numTargetKeyGroups,
    std::shared_ptr<ResultPartitionManager> partitionManager,
    std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory)
    : ResultPartition(
          owningTaskName,
          partitionIndex,
          partitionId,
          partitionType,
          subpartitions.size(),
          numTargetKeyGroups,
          partitionManager,
          bufferPoolFactory),
      subpartitions_(subpartitions),
      unicastBufferBuilders(subpartitions.size(), nullptr),
      broadcastBufferBuilder(nullptr) {};

// omni use, then set subpartitions
BufferWritingResultPartition::BufferWritingResultPartition(
    const std::string& owningTaskName,
    int partitionIndex,
    const ResultPartitionIDPOD& partitionId,
    int partitionType,
    int numSubpartitions,
    int numTargetKeyGroups,
    std::shared_ptr<ResultPartitionManager> partitionManager,
    std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory,
    int taskType)
    : ResultPartition(
          owningTaskName,
          partitionIndex,
          partitionId,
          partitionType,
          numSubpartitions,
          numTargetKeyGroups,
          partitionManager,
          bufferPoolFactory,
          taskType),

      unicastBufferBuilders(numSubpartitions, nullptr),
      broadcastBufferBuilder(nullptr)
{
    LOG_PART("Body BufferWritingResultPartition constructor.");
};

void BufferWritingResultPartition::setSubpartitions(
    const std::vector<std::shared_ptr<ResultSubpartition>>& subpartitions)
{
    if (subpartitions.size() != static_cast<size_t>(numSubpartitions)) {
        THROW_LOGIC_EXCEPTION("sub partition size mismatched!");
    }

    this->subpartitions_ = subpartitions;
}

std::vector<std::shared_ptr<ResultSubpartition>> BufferWritingResultPartition::getAllPartitions()
{
    return subpartitions_;
}

void BufferWritingResultPartition::SetChannelStateWriter(const std::shared_ptr<ChannelStateWriter>& channelStateWriter)
{
    for (auto subpartition : subpartitions_) {
        if (auto subpartitionChild = std::dynamic_pointer_cast<PipelinedSubpartition>(subpartition)) {
            subpartitionChild->setChannelStateWriter(channelStateWriter);
        }
    }
}

void BufferWritingResultPartition::setup()
{
    ResultPartition::setup();

    if (bufferPool->getNumberOfRequiredSegments() < getNumberOfSubpartitions()) {
        throw std::runtime_error(
            "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers "
            "for this result partition.");
    }
}

int BufferWritingResultPartition::getNumberOfQueuedBuffers()
{
    int totalBuffers = 0;
    for (const auto& subpartition : subpartitions_) {
        totalBuffers += subpartition->unsynchronizedGetNumberOfQueuedBuffers();
    }
    return totalBuffers;
}

int BufferWritingResultPartition::getNumberOfQueuedBuffers(int targetSubpartition)
{
    if (targetSubpartition < 0 || targetSubpartition >= numSubpartitions) {
        throw std::invalid_argument("Invalid targetSubpartition index.");
    }
    return subpartitions_[targetSubpartition]->unsynchronizedGetNumberOfQueuedBuffers();
}

void BufferWritingResultPartition::flushSubpartition(int targetSubpartition, bool finishProducers)
{
    if (finishProducers) {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilder(targetSubpartition);
    }
    subpartitions_[targetSubpartition]->flush();
}

void BufferWritingResultPartition::flushAllSubpartitions(bool finishProducers)
{
    LOG_TRACE(" >>> ");
    if (finishProducers) {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();
    }

    for (const auto& subpartition : subpartitions_) {
        LOG_TRACE("Flush each subpartition");
        subpartition->flush();
    }
}

void BufferWritingResultPartition::emitRecord(void* record, int targetSubpartition)
{
    bool oversizedSqlRow = false;
    if (taskType == 1) {
        auto* element = static_cast<StreamElement*>(record);
        if (element->getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP ||
            element->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP) {
            auto* sqlRecord = static_cast<StreamRecord*>(element);
            auto* batch = static_cast<VectorBatch*>(sqlRecord->getValue());
            if (batch != nullptr && batch->getSizeInBytes() > MAX_SQL_BATCH_BYTES) {
                const int rowCount = batch->GetRowCount();
                if (rowCount <= 1) {
                    const int32_t rowBytes = batch->getSizeInBytes();
                    if (rowBytes > MAX_SQL_SINGLE_ROW_BYTES) {
                        SqlBatchRecord original(sqlRecord);
                        throw std::runtime_error(
                            "SQL row size " + std::to_string(rowBytes) + " exceeds native network single-row limit " +
                            std::to_string(MAX_SQL_SINGLE_ROW_BYTES));
                    }
                    oversizedSqlRow = true;
                } else {
                    SqlBatchRecord original(sqlRecord);
                    const int leftRows = rowCount / 2;
                    auto left = CopySqlBatchRows(sqlRecord, 0, leftRows);
                    emitRecord(left.release(), targetSubpartition);
                    auto right = CopySqlBatchRows(sqlRecord, leftRows, rowCount - leftRows);
                    emitRecord(right.release(), targetSubpartition);
                    return;
                }
            }
        }
    }
    auto buffer = appendUnicastDataForNewRecord(record, targetSubpartition);
    if (taskType == 2) {
        // todo：这里可能有问题，如果是水印的话，record应该对应的是StreamElement
        auto streamRecord = reinterpret_cast<StreamRecord*>(record);
        auto value = reinterpret_cast<ByteBuffer*>(streamRecord->getValue());
        totalWrittenBytes += value->remaining();
        while (value->hasRemaining()) {
            finishUnicastBufferBuilder(targetSubpartition);
            buffer = appendUnicastDataForRecordContinuation(streamRecord, targetSubpartition);
        }
    }

    /* possible need this notification */
    if (buffer->isFull() || oversizedSqlRow) {
        finishUnicastBufferBuilder(targetSubpartition);
    }
    if (oversizedSqlRow) {
        flushSubpartition(targetSubpartition, false);
    }
}

BufferBuilder* BufferWritingResultPartition::appendUnicastDataForRecordContinuation(
    void* record, int targetSubpartition)
{
    auto bufferBuilder = requestNewUnicastBufferBuilder(targetSubpartition);

    int partialRecordBytes = bufferBuilder->appendAndCommit(record);
    addToSubpartition(bufferBuilder, targetSubpartition, partialRecordBytes, partialRecordBytes);

    return bufferBuilder;
}

void BufferWritingResultPartition::broadcastRecord(void* record)
{
}

void BufferWritingResultPartition::broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent)
{
    checkInProduceState();
    finishBroadcastBufferBuilder();
    finishUnicastBufferBuilders();

    for (const auto& subpartition : subpartitions_) {
        auto eventBufferConsumer = EventSerializer::ToBufferConsumer(event, isPriorityEvent);
        if (eventBufferConsumer == nullptr) {
            INFO_RELEASE("eventBufferConsumer is null ");
            throw std::runtime_error("eventBufferConsumer is null.");
        }
        auto subPartitionInfo = subpartition->getSubpartitionInfo();
        auto index = subpartition->getSubPartitionIndex();
        subpartition->add(eventBufferConsumer, 0);
        //            INFO_DEBUG(" Send " << event->GetEventClassName() << " to subPartition " <<
        //            subPartitionInfo.toString() << ", index : " << index)
        LOG_DEBUG(
            "[RP=" << (void*)this << "]Send " << event->GetEventClassName() << " to subPartition "
                   << subPartitionInfo.toString() << ", index : " << index);
    }
    flushAllSubpartitions(false);
}

std::shared_ptr<ResultSubpartitionView> BufferWritingResultPartition::createSubpartitionView(
    int subpartitionIndex, BufferAvailabilityListener* availabilityListener)
{
    LOG_PART("Beginning");
    if (subpartitionIndex < 0 || subpartitionIndex >= numSubpartitions) {
        throw std::out_of_range("Subpartition not found.");
    }
    if (isReleased()) {
        throw std::runtime_error("Partition released.");
    }

    auto subpartition = subpartitions_[subpartitionIndex];
    auto readView = subpartition->createReadView(availabilityListener);

    return readView;
}

void BufferWritingResultPartition::finish()
{
    finishBroadcastBufferBuilder();
    finishUnicastBufferBuilders();
    for (const auto& subpartition : subpartitions_) {
        subpartition->finish();
    }
    ResultPartition::finish();
}

void BufferWritingResultPartition::cancel()
{
    if (bufferPool != nullptr) {
        bufferPool->cancel();
    }
}

void BufferWritingResultPartition::close()
{
    if (broadcastBufferBuilder) {
        broadcastBufferBuilder->close();
        broadcastBufferBuilder = nullptr;
    }
    for (auto& builder : unicastBufferBuilders) {
        if (builder) {
            builder->close();
            delete builder;
            builder = nullptr;
        }
    }
    unicastBufferBuilders.clear();
    ResultPartition::close();
}

/* no backpressure at this time
std::shared_ptr<TimerGauge> BufferWritingResultPartition::getBackPressuredTimeMsPerSecond() const
{
    return backPressuredTimeMsPerSecond_;
}
*/

BufferBuilder* BufferWritingResultPartition::appendUnicastDataForNewRecord(void* record, int targetSubpartition)
{
    LOG_PART(
        this->getOwningTaskName() << " appending data   " << std::to_string(reinterpret_cast<long>(record))
                                  << " targetPartition " << std::to_string(targetSubpartition));

    if (targetSubpartition < 0 || static_cast<size_t>(targetSubpartition) >= unicastBufferBuilders.size()) {
        throw std::out_of_range("targetSubpartition out of range");
    }

    BufferBuilder* buffer = unicastBufferBuilders[targetSubpartition];
    uint64_t bytes = 0;
    if (taskType == 1) {
        auto* element = reinterpret_cast<StreamElement*>(record);
        if (element != nullptr) {
            switch (element->getTag()) {
                case StreamElementTag::TAG_REC_WITH_TIMESTAMP:
                case StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP: {
                    auto* streamRecord = static_cast<StreamRecord*>(element);
                    auto* vectorBatch = static_cast<omnistream::VectorBatch*>(streamRecord->getValue());
                    bytes = vectorBatch == nullptr ? 0 : static_cast<uint64_t>(vectorBatch->getSizeInBytes());
                    break;
                }
                case StreamElementTag::TAG_WATERMARK: bytes = sizeof(int64_t); break;
                case StreamElementTag::TAG_STREAM_STATUS: bytes = sizeof(int32_t); break;
                default:
                    throw std::runtime_error(
                        "Unsupported stream element tag....... from appendUnicastDataForNewRecord");
            }
        }
        requestMemoryForVectorBatch(targetSubpartition, bytes);
        // requestMemoryForVectorBatch may finish the previous builder to release its memory.
        buffer = unicastBufferBuilders[targetSubpartition];
        if (buffer == nullptr) {
            buffer = requestNewUnicastBufferBuilder(targetSubpartition, bytes);
            LOG_PART("Add bufferbuilder: " << buffer.get() << " to subparition" << targetSubpartition);
            addToSubpartition(buffer, targetSubpartition, 0);
        }
        buffer->appendAndCommit(record);
    } else if (taskType == 2) {
        if (buffer == nullptr) {
            buffer = requestNewUnicastBufferBuilder(targetSubpartition, 0);
            LOG_PART("Add bufferbuilder: " << buffer << " to subparition" << targetSubpartition);

            auto streamRecord = reinterpret_cast<StreamRecord*>(record);
            auto value = reinterpret_cast<ByteBuffer*>(streamRecord->getValue());
            addToSubpartition(buffer, targetSubpartition, 0, value->remaining());
        }
        auto memoryBuffer = reinterpret_cast<datastream::MemoryBufferBuilder*>(buffer);
        // LOG("buffer->appendAndCommit will running")
        memoryBuffer->appendAndCommit(record);
    } else {
        THROW_LOGIC_EXCEPTION("NOT IMPLEMENT");
    }
    return buffer;
}

void BufferWritingResultPartition::addToSubpartition(
    BufferBuilder* buffer, int targetSubpartition, int partialRecordLength)
{
    LOG("addToSubpartition running , createBufferConsumerFromBeginning");
    int desirableBufferSize =
        subpartitions_[targetSubpartition]->add(buffer->createBufferConsumerFromBeginning(), partialRecordLength);
    if (desirableBufferSize > 0) {
        buffer->trim(desirableBufferSize);
    }
}

// for datastream, likely vanilla flink
void BufferWritingResultPartition::addToSubpartition(
    BufferBuilder* buffer, int targetSubpartition, int partialRecordLength, int minDesirableBufferSize)
{
    LOG("addToSubpartition running , createBufferConsumerFromBeginning");
    int desirableBufferSize =
        subpartitions_[targetSubpartition]->add(buffer->createBufferConsumerFromBeginning(), partialRecordLength);

    resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
}

void BufferWritingResultPartition::resizeBuffer(
    BufferBuilder* buffer, int desirableBufferSize, int minDesirableBufferSize)
{
    if (desirableBufferSize > 0) {
        buffer->trim(std::max(desirableBufferSize, minDesirableBufferSize));
    }
}

/**
std::shared_ptr<ObjectBufferBuilder> BufferWritingResultPartition::appendUnicastDataForRecordContinuation(
    const std::shared_ptr<ByteBuffer>& remainingRecordBytes, int targetSubpartition)
{
    std::shared_ptr<ObjectBufferBuilder> buffer = requestNewUnicastBufferBuilder(targetSubpartition);
    int partialRecordBytes = buffer->appendAndCommit(*remainingRecordBytes);
    addToSubpartition(buffer, targetSubpartition, partialRecordBytes);
    return buffer;
}
**/

/**

    std::shared_ptr<ObjectBufferBuilder> BufferWritingResultPartition::appendUnicastDataForNewRecord(
        std::shared_ptr<java::nio::ByteBuffer> record,
        int targetSubpartition)
    {
        if (targetSubpartition < 0 || static_cast<size_t>(targetSubpartition) >= unicastBufferBuilders.size())
        {
            throw std::out_of_range("Subpartition index out of bounds");
        }

        std::shared_ptr<ObjectBufferBuilder> buffer = unicastBufferBuilders[targetSubpartition];

        if (!buffer)
        {
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            addToSubpartition(buffer, targetSubpartition, 0);
        }

        buffer->appendAndCommit(record);

        return buffer;
    }
    **/

/**
    std::shared_ptr<ObjectBufferBuilder> BufferWritingResultPartition::appendUnicastDataForRecordContinuation(
        std::shared_ptr<java::nio::ByteBuffer> remainingRecordBytes,
        int targetSubpartition)
    {
        std::shared_ptr<ObjectBufferBuilder> buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        // Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first before consumer is created. Otherwise it would be confused with
        // the case where the buffer starts with a complete record.
        // The next two lines cannot change order.
        const int partialRecordBytes = buffer->appendAndCommit(remainingRecordBytes);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes);

        return buffer;
    }
 */

void BufferWritingResultPartition::createBroadcastBufferConsumers(
    std::shared_ptr<ObjectBufferBuilder> buffer, int partialRecordBytes)
{
    auto consumer = buffer->createBufferConsumerFromBeginning();
    try {
        for (const auto& subpartition : subpartitions_) {
            subpartition->add(consumer->copy(), partialRecordBytes);
        }
    } catch (...) {
        if (consumer) {
            consumer->close();
        }
        throw;
    }

    if (consumer) {
        consumer->close();
    }
}

BufferBuilder* BufferWritingResultPartition::requestNewUnicastBufferBuilder(int targetSubpartition, uint64_t bytes)
{
    checkInProduceState();
    ensureUnicastMode();
    BufferBuilder* bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition, bytes);
    unicastBufferBuilders[targetSubpartition] = bufferBuilder;
    LOG("set bufferBuilder to unicastBufferBuilders, targetSubpartition: " << std::to_string(targetSubpartition));
    return bufferBuilder;
}

BufferBuilder* BufferWritingResultPartition::requestNewBroadcastBufferBuilder()
{
    checkInProduceState();
    ensureBroadcastMode();

    BufferBuilder* bufferBuilder = requestNewBufferBuilderFromPool(0);
    broadcastBufferBuilder = bufferBuilder;
    return bufferBuilder;
}

BufferBuilder* BufferWritingResultPartition::requestNewBufferBuilderFromPool(int targetSubpartition, uint64_t bytes)
{
    if (isReleased()) {
        throw std::runtime_error("Partition is released.");
    }
    if (bufferPool == nullptr) {
        throw std::runtime_error("Result partition buffer pool is null.");
    }
    LOG("bufferPool->requestObjectBufferBuilder will running");
    BufferBuilder* bufferBuilder = bufferPool->requestBufferBuilder(targetSubpartition, bytes);
    if (bufferBuilder) {
        return bufferBuilder;
    }
    // todo backpressure start point....
    hardBackPressuredTimeMsPerSecond->MarkStart();
    try {
        LOG("bufferPool->requestObjectBufferBuilderBlocking will running");
        bufferBuilder = bufferPool->requestBufferBuilderBlocking(targetSubpartition, bytes);
        hardBackPressuredTimeMsPerSecond->MarkEnd();
        return bufferBuilder;
    } catch (const std::exception& e) {
        hardBackPressuredTimeMsPerSecond->MarkEnd();
        throw std::runtime_error("Interrupted while waiting for buffer");
    }
}

void BufferWritingResultPartition::finishUnicastBufferBuilder(int targetSubpartition)
{
    BufferBuilder* bufferBuilder = unicastBufferBuilders[targetSubpartition];
    LOG_PART("Finish the bufferbuilder " << bufferBuilder << "  of targetSubpartition " << targetSubpartition);

    if (bufferBuilder) {
        numBytesOut->Inc(bufferBuilder->finish());
        numBuffersOut->Inc();
        bufferBuilder->close();
        delete bufferBuilder;
        unicastBufferBuilders[targetSubpartition] = nullptr;
    }
}

void BufferWritingResultPartition::finishUnicastBufferBuilders()
{
    for (int channelIndex = 0; channelIndex < numSubpartitions; channelIndex++) {
        finishUnicastBufferBuilder(channelIndex);
    }
}

void BufferWritingResultPartition::finishBroadcastBufferBuilder()
{
    if (broadcastBufferBuilder) {
        numBytesOut->Inc(broadcastBufferBuilder->finish() * numSubpartitions);
        numBuffersOut->Inc(numSubpartitions);
        broadcastBufferBuilder->close();
        broadcastBufferBuilder = nullptr;
    }
}

void BufferWritingResultPartition::ensureUnicastMode()
{
    finishBroadcastBufferBuilder();
}

void BufferWritingResultPartition::ensureBroadcastMode()
{
    finishUnicastBufferBuilders();
}

void BufferWritingResultPartition::releaseInternal()
{
    for (auto subPartition : subpartitions_) {
        try {
            subPartition->release();
        } catch (const std::exception& e) {
            throw std::runtime_error("subpartition release error in class BufferWritingResultPartition");
        }
    }
}

void BufferWritingResultPartition::SetMetricGroup(std::shared_ptr<AbstractMetricGroup> metricGroup)
{
    if (metricGroup == nullptr) {
        return;
    }

    auto* parentMetricGroup = metricGroup->GetParent();
    if (parentMetricGroup != nullptr) {
        auto backPressureMetric = parentMetricGroup->GetMetric("hardBackPressuredTimeMsPerSecond");
        hardBackPressuredTimeMsPerSecond = std::dynamic_pointer_cast<TimerGauge>(backPressureMetric);
    }

    if (taskType != 1) {
        return;
    }

    auto localObjectBufferPool = std::dynamic_pointer_cast<LocalObjectBufferPool>(bufferPool);
    if (localObjectBufferPool == nullptr) {
        return;
    }

    auto toSizeGaugeValue = [](uint64_t value) {
        return value > static_cast<uint64_t>(std::numeric_limits<int>::max()) ? std::numeric_limits<int>::max()
                                                                              : static_cast<int>(value);
    };

    auto vectorBatchMetricGroup = std::dynamic_pointer_cast<VectorBatchBufferPoolMetricGroup>(metricGroup);
    if (vectorBatchMetricGroup == nullptr) {
        return;
    }

    vectorBatchMetricGroup->SetSizeSupplierFactory(
        [localObjectBufferPool, toSizeGaugeValue](const std::string& metricName) -> SizeGauge::SizeSupplier {
            if (metricName == "objectSegmentSize") {
                return [localObjectBufferPool]() { return localObjectBufferPool->getObjectSegmentSize(); };
            }
            if (metricName == "requiredMemory") {
                return [localObjectBufferPool, toSizeGaugeValue]() {
                    return toSizeGaugeValue(localObjectBufferPool->getRequiredMemory());
                };
            }
            if (metricName == "currentPoolMemoryBudget") {
                return [localObjectBufferPool, toSizeGaugeValue]() {
                    return toSizeGaugeValue(localObjectBufferPool->getCurrentPoolMemoryBudget());
                };
            }
            if (metricName == "maxAllowedMemory") {
                return [localObjectBufferPool, toSizeGaugeValue]() {
                    return toSizeGaugeValue(localObjectBufferPool->getMaxMemory());
                };
            }
            if (metricName == "usedMemory") {
                return [localObjectBufferPool, toSizeGaugeValue]() {
                    return toSizeGaugeValue(localObjectBufferPool->getUsedMemory());
                };
            }
            if (metricName == "availableMemory") {
                return [localObjectBufferPool, toSizeGaugeValue]() {
                    return toSizeGaugeValue(localObjectBufferPool->getAvailableMemory());
                };
            }
            if (metricName == "maxMemoryPerChannel") {
                return [localObjectBufferPool, toSizeGaugeValue]() {
                    return toSizeGaugeValue(localObjectBufferPool->getMaxMemoryPerChannel());
                };
            }
            if (metricName == "requestSegmentNumber") {
                return [localObjectBufferPool]() { return localObjectBufferPool->getRequestSegmentNumber(); };
            }
            if (metricName == "recycleSegmentNumber") {
                return [localObjectBufferPool]() { return localObjectBufferPool->getRecycleSegmentNumber(); };
            }
            throw std::runtime_error("Unknown VectorBatchBufferPool metric: " + metricName);
        });
}

void BufferWritingResultPartition::requestMemoryForVectorBatch(int targetSubpartition, uint64_t bytes)
{
    auto localObjectBufferPool = static_cast<LocalObjectBufferPool*>(bufferPool.get());
    if (!localObjectBufferPool->chargeMemory(targetSubpartition, bytes)) {
        // A pending builder can hold the memory needed by this batch. Publish it before
        // blocking so the consumer can recycle the previous batch.
        if (unicastBufferBuilders[targetSubpartition] != nullptr) {
            finishUnicastBufferBuilder(targetSubpartition);
            flushSubpartition(targetSubpartition, false);
        }
        hardBackPressuredTimeMsPerSecond->MarkStart();
        try {
            localObjectBufferPool->chargeMemoryBlocking(targetSubpartition, bytes);
        } catch (...) {
            hardBackPressuredTimeMsPerSecond->MarkEnd();
            throw;
        }
        hardBackPressuredTimeMsPerSecond->MarkEnd();
    }
}
} // namespace omnistream
