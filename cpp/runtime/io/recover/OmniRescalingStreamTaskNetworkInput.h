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

#ifndef OMNISTREAM_OMNIRESCALINGSTREAMTASKNETWORKINPUT_H
#define OMNISTREAM_OMNIRESCALINGSTREAMTASKNETWORKINPUT_H

#pragma once

#include <functional>
#include <future>
#include <memory>
#include <unordered_map>
#include <vector>

#include "api/common/TaskInfo.h"
#include "runtime/checkpoint/InflightDataRescalingDescriptor.h"
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "runtime/io/network/api/serialization/RecordDeserializer.h"
#include "runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.h"
#include "runtime/plugable/DeserializationDelegate.h"
#include "streaming/runtime/io/OmniAbstractStreamTaskNetworkInput.h"
#include "streaming/runtime/io/DataInputStatus.h"
#include "streaming/runtime/io/StreamTaskNetworkInput.h"
#include "streaming/runtime/partitioner/StreamPartitioner.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/runtime/tasks/StreamTask.h"
#include "DemultiplexingRecordDeserializer.h"
#include "io/checkpointing/CheckpointedInputGate.h"
#include "checkpoint/InflightDataRescalingDescriptor.h"

namespace omnistream {
class OmniRescalingStreamTaskNetworkInput : public OmniAbstractStreamTaskNetworkInput {
public:
    OmniRescalingStreamTaskNetworkInput(
        int64_t inputIndex,
        std::shared_ptr<CheckpointedInputGate> inputGate,
        int taskType,
        TypeSerializer* inputSerializer,
        std::vector<long>& channelInfos,
        std::shared_ptr<InflightDataRescalingDescriptor> inflightDataRescalingDescriptor,
        std::function<StreamPartitioner<IOReadableWritable>*(int)> getPartitionerFunction,
        std::function<StreamPartitionerV2<StreamRecord>*(int)> getSqlPartitionerFunction,
        TaskInformationPOD* taskInfo)
        : OmniAbstractStreamTaskNetworkInput(
              inputIndex,
              inputGate,
              taskType,
              inputSerializer,
              channelInfos,
              getRecordDeserializers(
                  inputGate,
                  inputSerializer,
                  taskType,
                  *inflightDataRescalingDescriptor,
                  getPartitionerFunction,
                  getSqlPartitionerFunction,
                  taskInfo),
              taskInfo->getExecutionCheckpointConfig().getCheckpointInterval())
    {
        INFO_RELEASE("create OmniRescalingStreamTaskNetworkInput");
    }

    datastream::RecordDeserializer* getActiveSerializer(long channelInfo) override
    {
        auto ser = OmniAbstractStreamTaskNetworkInput::getActiveSerializer(channelInfo);
        if (ser == nullptr) {
            INFO_RELEASE("error OmniRescalingStreamTaskNetworkInput::getActiveSerializer not find:" << channelInfo);
            throw std::runtime_error(
                "Channel " + std::to_string(channelInfo) + " should not receive data during recovery.");
        }
        auto deserializer = dynamic_cast<DemultiplexingRecordDeserializer*>(ser);
        if (!deserializer || !deserializer->hasMappings()) {
            INFO_RELEASE("error OmniRescalingStreamTaskNetworkInput::getActiveSerializer trans fail:" << channelInfo);
            throw std::runtime_error(
                "Channel " + std::to_string(channelInfo) + " should not receive data during recovery.");
        }
        return deserializer;
    }

    DataInputStatus processEvent(BufferOrEvent* bufferOrEvent) override
    {
        auto event = bufferOrEvent->getEvent();
        if (auto subtaskConnDesc = dynamic_cast<SubtaskConnectionDescriptor*>(event.get())) {
            auto channelId = bufferOrEvent->getChannelInfo().getComplexId();
            auto serializer = dynamic_cast<DemultiplexingRecordDeserializer*>(getActiveSerializer(channelId));
            serializer->select(subtaskConnDesc);
            INFO_RELEASE(
                "process SubtaskConnectionDescriptor event, channelInfo:"
                << bufferOrEvent->getChannelInfo().toString());
            return DataInputStatus::MORE_AVAILABLE;
        }
        return OmniAbstractStreamTaskNetworkInput::processEvent(bufferOrEvent);
    }

    OmniStreamTaskInput* finishRecover()
    {
        std::vector<long> channelIds;
        for (const auto& item : inputGate->GetChannelInfos()) {
            channelIds.emplace_back(item.getComplexId());
        }
        return new OmniAbstractStreamTaskNetworkInput(
            inputIndex, inputGate, taskType, inSerializer, channelIds, checkpointInterval);
    }

    class RecordFilterFactory {
    public:
        RecordFilterFactory(
            int subtaskIndex,
            TypeSerializer* inputSerializer,
            int numberOfChannels,
            std::function<StreamPartitioner<IOReadableWritable>*(int)> gatePartitioners,
            int maxParallelism)
            : gatePartitioners_(std::move(gatePartitioners)),
              inputSerializer_(std::move(inputSerializer)),
              numberOfChannels_(numberOfChannels),
              subtaskIndex_(subtaskIndex),
              maxParallelism_(maxParallelism)
        {
        }

        std::function<bool(StreamRecord&)> apply(const InputChannelInfo& channelInfo)
        {
            auto it = partitionerCache_.find(channelInfo.getGateIdx());
            if (it == partitionerCache_.end()) {
                auto partitioner = createPartitioner(channelInfo.getGateIdx());
                partitionerCache_[channelInfo.getGateIdx()] = partitioner;
                it = partitionerCache_.find(channelInfo.getGateIdx());
            }
            auto filter = std::make_shared<RecordFilter>(
                dynamic_cast<ChannelSelector<IOReadableWritable>*>(it->second), inputSerializer_, subtaskIndex_);
            auto function =
                std::function<bool(StreamRecord&)>([filter](StreamRecord& record) { return filter->apply(record); });
            return function;
        }

    private:
        std::unordered_map<int, StreamPartitioner<IOReadableWritable>*> partitionerCache_;
        std::function<StreamPartitioner<IOReadableWritable>*(int)> gatePartitioners_;
        TypeSerializer* inputSerializer_;
        int numberOfChannels_;
        int subtaskIndex_;
        int maxParallelism_;

        StreamPartitioner<IOReadableWritable>* createPartitioner(int index)
        {
            auto partitioner = gatePartitioners_(index);
            partitioner->setup(numberOfChannels_);
            return partitioner;
        }
    };

    class DeserializerFactory {
    public:
        explicit DeserializerFactory()
        {
        }

        static std::shared_ptr<RecordDeserializer> apply(int totalChannel)
        {
            return std::make_shared<SpillingAdaptiveSpanningRecordDeserializer>();
        }
    };

    class SqlRecordFilterFactory {
    public:
        SqlRecordFilterFactory(
            int subtaskIndex,
            int numberOfChannels,
            std::function<StreamPartitionerV2<StreamRecord>*(int)> gatePartitioners)
            : subtaskIndex_(subtaskIndex),
              numberOfChannels_(numberOfChannels),
              gatePartitioners_(std::move(gatePartitioners))
        {
        }

        std::function<StreamRecord*(StreamRecord&)> apply(const InputChannelInfo& channelInfo)
        {
            auto it = partitionerCache_.find(channelInfo.getGateIdx());
            if (it == partitionerCache_.end()) {
                auto* partitioner = gatePartitioners_(channelInfo.getGateIdx());
                partitioner->setup(numberOfChannels_);
                it = partitionerCache_.emplace(channelInfo.getGateIdx(), partitioner).first;
            }
            auto* partitioner = it->second;
            int subtaskIndex = subtaskIndex_;
            return [partitioner, subtaskIndex](StreamRecord& record) -> StreamRecord* {
                auto* batch = static_cast<VectorBatch*>(record.getValue());
                std::vector<int32_t> selectedRows;
                selectedRows.reserve(batch->GetRowCount());
                for (int32_t row = 0; row < batch->GetRowCount(); ++row) {
                    if (partitioner->selectRowChannel(batch, row) == subtaskIndex) {
                        selectedRows.push_back(row);
                    }
                }
                if (selectedRows.empty()) {
                    return nullptr;
                }

                auto* filteredBatch = VectorBatchUtil::buildNewVectorBatchByRowIds(batch, selectedRows);
                auto* filteredRecord = record.hasTimestamp() ? new StreamRecord(filteredBatch, record.getTimestamp())
                                                             : new StreamRecord(filteredBatch);
                filteredRecord->setTag(record.getTag());
                return filteredRecord;
            };
        }

    private:
        int subtaskIndex_;
        int numberOfChannels_;
        std::function<StreamPartitionerV2<StreamRecord>*(int)> gatePartitioners_;
        std::unordered_map<int, StreamPartitionerV2<StreamRecord>*> partitionerCache_;
    };

    static std::unique_ptr<std::unordered_map<long, std::unique_ptr<RecordDeserializer>>> getRecordDeserializers(
        std::shared_ptr<CheckpointedInputGate> checkpointedInputGate,
        TypeSerializer* inputSerializer,
        int taskType,
        const InflightDataRescalingDescriptor& rescalingDescriptor,
        std::function<StreamPartitioner<IOReadableWritable>*(int)> gatePartitioners,
        std::function<StreamPartitionerV2<StreamRecord>*(int)> sqlGatePartitioners,
        TaskInformationPOD* taskInfo)
    {
        auto recordFilterFactory = std::make_shared<RecordFilterFactory>(
            taskInfo->getIndexOfSubtask(),
            inputSerializer,
            taskInfo->getNumberOfSubtasks(),
            gatePartitioners,
            taskInfo->getMaxNumberOfSubtasks());

        auto function = std::function<std::function<bool(StreamRecord&)>(const InputChannelInfo&)>(
            [recordFilterFactory, taskType](const InputChannelInfo& channelInfo) {
                return taskType == 2 ? recordFilterFactory->apply(channelInfo) : RecordFilter::all();
            });
        auto sqlRecordFilterFactory = std::make_shared<SqlRecordFilterFactory>(
            taskInfo->getIndexOfSubtask(), taskInfo->getNumberOfSubtasks(), std::move(sqlGatePartitioners));
        auto sqlFunction = std::function<std::function<StreamRecord*(StreamRecord&)>(const InputChannelInfo&)>(
            [sqlRecordFilterFactory, taskType](const InputChannelInfo& channelInfo) {
                if (taskType == 1) {
                    return sqlRecordFilterFactory->apply(channelInfo);
                }
                return std::function<StreamRecord*(StreamRecord&)>([](StreamRecord& record) { return &record; });
            });
        auto deserializers = std::make_unique<std::unordered_map<long, std::unique_ptr<RecordDeserializer>>>();
        deserializers->reserve(checkpointedInputGate->GetChannelInfos().size());

        for (auto& channelInfo : checkpointedInputGate->GetChannelInfos()) {
            auto channelId = channelInfo.getComplexId();
            deserializers->emplace(
                channelId,
                DemultiplexingRecordDeserializer::create(
                    channelInfo, rescalingDescriptor, DeserializerFactory::apply, function, sqlFunction));
        }
        return deserializers;
    }
};
} // namespace omnistream

#endif // OMNISTREAM_OMNIRESCALINGSTREAMTASKNETWORKINPUT_H
