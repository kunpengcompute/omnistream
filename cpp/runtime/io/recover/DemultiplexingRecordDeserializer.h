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

#ifndef OMNISTREAM_DEMULTIPLEXINGRECORDDESERIALIZER_H
#define OMNISTREAM_DEMULTIPLEXINGRECORDDESERIALIZER_H
#pragma once

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <sstream>

#include "streaming/runtime/streamrecord/StreamElement.h"
#include "runtime/plugable/DeserializationDelegate.h"
#include "runtime/io/network/api/serialization/RecordDeserializer.h"
#include "runtime/streamrecord/StreamRecord.h"
#include "api/watermark/Watermark.h"
#include "watermark/WatermarkStatus.h"
#include "runtime/event/SubtaskConnectionDescriptor.h"
#include "partition/consumer/InputChannelInfo.h"
#include "checkpoint/InflightDataRescalingDescriptor.h"
#include "runtime/io/recover/RecordFilter.h"
#include "streaming/runtime/partitioner/V2/StreamPartitionerV2.h"
#include "table/data/util/VectorBatchUtil.h"

namespace omnistream {

class DemultiplexingRecordDeserializer : public RecordDeserializer {
public:
    static DemultiplexingRecordDeserializer* UNMAPPED;
    struct VirtualChannel {
        std::shared_ptr<RecordDeserializer> deserializer;
        std::function<bool(StreamRecord&)> recordFilter;
        std::function<StreamRecord*(StreamRecord&)> sqlRecordFilter;
        Watermark lastWatermark = Watermark::UNINITIALIZED;
        WatermarkStatus* watermarkStatus = WatermarkStatus::active();
        DeserializationResult* lastResult;

        VirtualChannel(
            std::shared_ptr<RecordDeserializer> deser,
            std::function<bool(StreamRecord&)> filter,
            std::function<StreamRecord*(StreamRecord&)> sqlFilter)
            :

              deserializer(std::move(deser)),
              recordFilter(std::move(filter)),
              sqlRecordFilter(std::move(sqlFilter))
        {
        }

        DeserializationResult* getNextRecord(DeserializationDelegate& delegate)
        {
            do {
                lastResult = &deserializer->getNextRecord(delegate);

                if (lastResult->isFullRecord()) {
                    auto* element = static_cast<StreamElement*>(delegate.getInstance());
                    // test if record belongs to this subtask if it comes from ambiguous channel
                    if (dynamic_cast<StreamRecord*>(element)) {
                        StreamRecord* streamRecord = dynamic_cast<StreamRecord*>(element);
                        if (recordFilter(*streamRecord)) {
                            return lastResult;
                        }
                    } else if (dynamic_cast<Watermark*>(element)) {
                        lastWatermark = *(dynamic_cast<Watermark*>(element));
                        return lastResult;
                    } else if (dynamic_cast<WatermarkStatus*>(element)) {
                        watermarkStatus = dynamic_cast<WatermarkStatus*>(element);
                        return lastResult;
                    }
                }
                // loop is only re-executed for filtered full records
            } while (!lastResult->isBufferConsumed());
            return &DeserializationResult_PARTIAL_RECORD;
        }

        void SetNextBuffer(ReadOnlySlicedNetworkBuffer* buffer)
        {
            deserializer->SetNextBuffer(buffer);
        }

        void clear()
        {
            deserializer->clear();
        }

        bool hasPartialData() const
        {
            return lastResult && !lastResult->isBufferConsumed();
        }
    };

    explicit DemultiplexingRecordDeserializer(std::map<long, std::shared_ptr<VirtualChannel>> channelMap)
        : channels(std::move(channelMap))
    {
    }

    void select(SubtaskConnectionDescriptor* descriptor)
    {
        auto it = channels.find(descriptor->getComplexId());
        if (it == channels.end()) {
            std::ostringstream oss;
            oss << "Cannot select " << descriptor->toString() << "; known channels are ";
            for (const auto& pair : channels) {
                oss << pair.first << " ";
            }
            INFO_RELEASE(oss.str().c_str());
            throw std::runtime_error(oss.str());
        }
        currentVirtualChannel = it->second;
    }

    StreamRecord* FilterRecordForSql(StreamRecord& record) override
    {
        if (!currentVirtualChannel) {
            throw std::runtime_error("Cannot filter SQL record before selecting a recovered channel.");
        }
        return currentVirtualChannel->sqlRecordFilter(record);
    }

    bool hasMappings() const
    {
        return !channels.empty();
    }

    void SetNextBuffer(ReadOnlySlicedNetworkBuffer* buffer) override
    {
        currentVirtualChannel->SetNextBuffer(buffer);
    }

    std::vector<omnistream::Buffer*> GetUnconsumedBuffer() override
    {
        throw std::runtime_error("Cannot checkpoint while recovering");
    }

    bool hasPartialData() const
    {
        for (const auto& pair : channels) {
            if (pair.second->hasPartialData()) {
                return true;
            }
        }
        return false;
    }

    DeserializationResult& getNextRecord(IOReadableWritable& target) override
    {
        auto delegate = static_cast<DeserializationDelegate*>(&target);
        DeserializationResult* result;
        do {
            result = currentVirtualChannel->getNextRecord(*delegate);

            if (result->isFullRecord()) {
                auto* element = static_cast<StreamElement*>(delegate->getInstance());
                if (dynamic_cast<StreamRecord*>(element)) {
                    return *result;
                } else if (dynamic_cast<Watermark*>(element)) {
                    // basically, do not emit a watermark if not all virtual channel are past it
                    Watermark minWatermark = Watermark::MAX_WATERMARK;
                    for (const auto& pair : channels) {
                        Watermark w = pair.second->lastWatermark;
                        if (w.getTimestamp() < minWatermark.getTimestamp()) {
                            minWatermark = w;
                        }
                    }
                    // at least one virtual channel has no watermark, don't emit any watermark yet
                    if (minWatermark.getTimestamp() == Watermark::UNINITIALIZED.getTimestamp()) {
                        continue;
                    }
                    delegate->setInstance(static_cast<void*>(&minWatermark));
                    return *result;
                } else if (dynamic_cast<WatermarkStatus*>(element)) {
                    // summarize statuses across all virtual channels
                    // duplicate statuses are filtered in StatusWatermarkValve
                    for (const auto& pair : channels) {
                        if (pair.second->watermarkStatus->IsActive()) {
                            delegate->setInstance(WatermarkStatus::active());
                            break;
                        }
                    }
                    return *result;
                }
            }

            // loop is only re-executed for suppressed watermark
        } while (!result->isBufferConsumed());
        return DeserializationResult_PARTIAL_RECORD;
    }

    void clear() override
    {
        for (auto& pair : channels) {
            pair.second->clear();
        }
    }

    static std::unique_ptr<DemultiplexingRecordDeserializer> create(
        const InputChannelInfo& channelInfo,
        const InflightDataRescalingDescriptor& rescalingDescriptor,
        std::function<std::shared_ptr<RecordDeserializer>(int)> deserializerFactory,
        std::function<std::function<bool(StreamRecord&)>(const InputChannelInfo&)> recordFilterFactory,
        std::function<std::function<StreamRecord*(StreamRecord&)>(const InputChannelInfo&)> sqlRecordFilterFactory)
    {
        std::vector<int> oldSubtaskIndexes = rescalingDescriptor.GetOldSubtaskIndexes(channelInfo.getGateIdx());
        if (oldSubtaskIndexes.empty()) {
            return std::make_unique<DemultiplexingRecordDeserializer>(
                std::map<long, std::shared_ptr<typename DemultiplexingRecordDeserializer::VirtualChannel>>());
        }
        auto channelMapping = rescalingDescriptor.GetChannelMapping(channelInfo.getGateIdx());
        std::vector<int> oldChannelIndexes = channelMapping->getMappedIndexes(channelInfo.getInputChannelIdx());
        if (oldChannelIndexes.empty()) {
            INFO_RELEASE(
                "DemultiplexingRecordDeserializer create old channel is empty:" << channelInfo.toString() << ",channel:"
                                                                                << channelMapping->ToString());
            return std::make_unique<DemultiplexingRecordDeserializer>(
                std::map<long, std::shared_ptr<typename DemultiplexingRecordDeserializer::VirtualChannel>>());
        }
        int totalChannels = oldSubtaskIndexes.size() * oldChannelIndexes.size();
        std::map<long, std::shared_ptr<VirtualChannel>> virtualChannels;
        for (int subtask : oldSubtaskIndexes) {
            for (int channel : oldChannelIndexes) {
                SubtaskConnectionDescriptor descriptor = *new SubtaskConnectionDescriptor(subtask, channel);
                virtualChannels[descriptor.getComplexId()] = std::make_shared<VirtualChannel>(
                    deserializerFactory(totalChannels),
                    rescalingDescriptor.IsAmbiguous(channelInfo.getGateIdx(), subtask)
                        ? recordFilterFactory(channelInfo)
                        : RecordFilter::all(),
                    rescalingDescriptor.IsAmbiguous(channelInfo.getGateIdx(), subtask)
                        ? sqlRecordFilterFactory(channelInfo)
                        : std::function<StreamRecord*(StreamRecord&)>([](StreamRecord& record) { return &record; }));
            }
        }
        INFO_RELEASE("DemultiplexingRecordDeserializer create channel size:" << virtualChannels.size());
        return std::make_unique<DemultiplexingRecordDeserializer>(virtualChannels);
    }

    std::string toString() const
    {
        std::ostringstream oss;
        oss << "DemultiplexingRecordDeserializer{channels=";
        for (const auto& pair : channels) {
            oss << pair.first << " ";
        }
        oss << "}";
        return oss.str();
    }

private:
    std::map<long, std::shared_ptr<VirtualChannel>> channels;
    std::shared_ptr<VirtualChannel> currentVirtualChannel;
};
} // namespace omnistream
#endif // OMNISTREAM_DEMULTIPLEXINGRECORDDESERIALIZER_H
