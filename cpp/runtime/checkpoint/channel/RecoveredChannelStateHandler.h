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

#ifndef RECOVERED_CHANNEL_STATE_HANDLER_H
#define RECOVERED_CHANNEL_STATE_HANDLER_H

#include "runtime/buffer/BufferBuilder.h"
#include "ChannelStateSerializer.h"

// #include "runtime/io/network/partition/CheckpointedResultPartition.h"
#include "runtime/partition/CheckpointedResultPartition.h"
#include "runtime/partition/PipelinedSubpartition.h"

#include "core/utils/lang/AutoCloseable.h"
#include "partition/ResultPartitionWriter.h"
#include "partition/consumer/InputGate.h"
#include "partition/consumer/RecoveredInputChannel.h"
#include "runtime/checkpoint/InflightDataRescalingDescriptor.h"
#include "runtime/partition/ResultSubpartitionInfoPOD.h"
#include "runtime/checkpoint/RescaleMappings.h"

namespace omnistream {

template <typename Info, typename Context>
class RecoveredChannelStateHandler : public AutoCloseable {
public:
    struct BufferWithContext {
        std::shared_ptr<ChannelStateByteBuffer> buffer_;
        Context context_;

        BufferWithContext(std::shared_ptr<ChannelStateByteBuffer> buffer, Context context)
            : buffer_(std::move(buffer)),
              context_(std::move(context))
        {
        }
        ~BufferWithContext() = default;

        void close()
        {
            buffer_->close();
        }
    };

    virtual ~RecoveredChannelStateHandler() = default;

    virtual BufferWithContext getBuffer(const Info& info) = 0;

    /**
     * Recover the data from buffer. This method is taking over the ownership of the
     * bufferWithContext and is fully responsible for cleaning it up both on the happy path and in
     * case of an error.
     */
    virtual void recover(const Info& info, int oldSubtaskIndex, const BufferWithContext& bufferWithContext) = 0;

    virtual void close() = 0;

    virtual bool usesObjectSegment(const InputChannelInfo& channelInfo) = 0;
};

class ResultSubpartitionRecoveredStateHandler
    : public RecoveredChannelStateHandler<ResultSubpartitionInfoPOD, BufferBuilder*> {
public:
    ResultSubpartitionRecoveredStateHandler(
        std::vector<std::shared_ptr<ResultPartitionWriter>> writers,
        bool notifyAndBlockOnCompletion,
        std::shared_ptr<InflightDataRescalingDescriptor> channelMapping);
    ~ResultSubpartitionRecoveredStateHandler() override;

    BufferWithContext getBuffer(const ResultSubpartitionInfoPOD& subpartitionInfo) override;

    void recover(
        const ResultSubpartitionInfoPOD& subpartitionInfo,
        int oldSubtaskIndex,
        const BufferWithContext& bufferWithContext) override;

    void close() override;

    bool usesObjectSegment(const InputChannelInfo& channelInfo) override
    {
        return false;
    }

private:
    std::vector<std::shared_ptr<ResultPartitionWriter>> writers_;
    bool notifyAndBlockOnCompletion_;
    std::shared_ptr<InflightDataRescalingDescriptor> channelMapping_;
    std::unordered_map<int, RescaleMappings> oldToNewMappings_;
    std::unordered_map<
        ResultSubpartitionInfoPOD,
        std::vector<std::shared_ptr<CheckpointedResultSubpartition>>,
        ResultSubpartitionInfoPODHash,
        IResultSubpartitionInfoPODEqual>
        rescaledChannels_;

    std::shared_ptr<CheckpointedResultSubpartition> getSubpartition(int partitionIndex, int subPartitionIdx);

    std::vector<std::shared_ptr<CheckpointedResultSubpartition>> getMappedChannels(
        const ResultSubpartitionInfoPOD& subpartitionInfo);

    std::vector<std::shared_ptr<CheckpointedResultSubpartition>> calculateMapping(
        const ResultSubpartitionInfoPOD& info);
};

class InputChannelRecoveredStateHandler : public RecoveredChannelStateHandler<InputChannelInfo, Buffer*> {
public:
    InputChannelRecoveredStateHandler(
        const std::vector<std::shared_ptr<InputGate>>& inputGates,
        const std::shared_ptr<InflightDataRescalingDescriptor>& channelMapping)
        : inputGates(inputGates),
          channelMapping(channelMapping)
    {
    }
    ~InputChannelRecoveredStateHandler() override;
    BufferWithContext getBuffer(const InputChannelInfo& inputChannelInfo) override;
    void recover(
        const InputChannelInfo& inputChannelInfo,
        int oldSubtaskIndex,
        const BufferWithContext& bufferWithContext) override;
    void close() override;
    bool usesObjectSegment(const InputChannelInfo& channelInfo) override;

private:
    std::shared_ptr<RecoveredInputChannel> getChannel(int gateIndex, int subPartitionIndex);
    std::vector<std::shared_ptr<RecoveredInputChannel>> calculateMapping(InputChannelInfo info);
    std::vector<std::shared_ptr<RecoveredInputChannel>> getMappedChannels(InputChannelInfo channelInfo);

    std::vector<std::shared_ptr<InputGate>> inputGates;
    std::shared_ptr<InflightDataRescalingDescriptor> channelMapping;
    std::unordered_map<
        InputChannelInfo,
        std::vector<std::shared_ptr<RecoveredInputChannel>>,
        InputChannelInfoHash,
        InputChannelInfoEqual>
        rescaledChannels;
    std::unordered_map<int, RescaleMappings> oldToNewMappings;
};
} // namespace omnistream

#endif // RECOVERED_CHANNEL_STATE_HANDLER_H
