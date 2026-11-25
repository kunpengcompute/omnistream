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

#ifndef SEQUENTIAL_CHANNEL_STATE_READER_IMPL_H
#define SEQUENTIAL_CHANNEL_STATE_READER_IMPL_H

#include <fstream>
#include <memory>
#include <vector>
#include <map>
#include "ChannelStateSerializer.h"
#include "RecoveredChannelStateHandler.h"
#include "SequentialChannelStateReader.h"
#include "runtime/buffer/VectorBatchBuffer.h"
#include "checkpoint/OperatorSubtaskState.h"
#include "runtime/checkpoint/TaskStateSnapshot.h"

#include "core/memory/MemorySegmentFactory.h"
#include "runtime/buffer/NetworkBuffer.h"
#include "runtime/buffer/BufferRecycler.h"
#include "runtime/buffer/OriginalNetworkBufferRecycler.h"

namespace omnistream {
template <typename Info>
class RescaledOffset {
public:
    long long offset;
    Info channelInfo;
    int oldSubtaskIndex;

    RescaledOffset(long long offset, Info channelInfo, int oldSubtaskIndex)
        : offset(offset),
          channelInfo(channelInfo),
          oldSubtaskIndex(oldSubtaskIndex)
    {
    }

    std::string toString() const
    {
        std::ostringstream oss;
        oss << "RescaledOffset{offset=" << offset << ", channelInfo=" << (channelInfo ? "Valid" : "Null")
            << ", oldSubtaskIndex=" << oldSubtaskIndex << "}";
        return oss.str();
    }
};

class ChannelStateChunkReader {
public:
    std::shared_ptr<ChannelStateSerializer> serializer;

    ChannelStateChunkReader(std::shared_ptr<ChannelStateSerializer>& serializer) : serializer(serializer)
    {
    }

    void readChunkByByteStreamForInputChannel(
        std::shared_ptr<ByteStateHandleInputStream>& source,
        long long sourceOffset,
        std::shared_ptr<InputChannelRecoveredStateHandler> stateHandler,
        const InputChannelInfo& channelInfo,
        int oldSubtaskIndex)
    {
        if (source->GetPos() != sourceOffset) {
            source->Seek(sourceOffset);
        }

        int length = serializer->ReadLength2(source);

        while (length > 0) {
            bool isVb = stateHandler->usesObjectSegment(channelInfo);
            auto bufferWithContext = isVb ? newVbTempInputBufferWithContext() : newTempInputBufferWithContext();

            try {
                while (length > 0 && bufferWithContext.buffer_->isWritable()) {
                    length -= serializer->ReadData2(source, bufferWithContext.buffer_, length);
                }
            } catch (const std::exception& e) {
                bufferWithContext.close();
                throw;
            }
            stateHandler->recover(channelInfo, oldSubtaskIndex, bufferWithContext);
        }
    }

    static RecoveredChannelStateHandler<InputChannelInfo, Buffer*>::BufferWithContext newTempInputBufferWithContext()
    {
        constexpr int kTempRestoreBufferSize = 32 * 1024;

        auto* memorySegment = MemorySegmentFactory::wrap(kTempRestoreBufferSize);

        auto buffer = new NetworkBuffer(memorySegment, std::make_shared<OriginalNetworkBufferRecycler>(), true);

        return {ChannelStateByteBuffer::wrap(buffer), buffer};
    }

    static RecoveredChannelStateHandler<InputChannelInfo, Buffer*>::BufferWithContext newVbTempInputBufferWithContext()
    {
        constexpr int kTempRestoreBufferSize = 32 * 1024;

        ObjectSegment* objectSegment = new ObjectSegment(kTempRestoreBufferSize);

        VectorBatchBuffer* buffer =
            new VectorBatchBuffer(objectSegment, std::make_shared<DeepCopiedObjectBufferRecycler>());

        return {ChannelStateByteBuffer::wrap(buffer), buffer};
    }

    void readChunkByByteStreamForResultSubpartition(
        std::shared_ptr<ByteStateHandleInputStream>& source,
        long long sourceOffset,
        std::shared_ptr<ResultSubpartitionRecoveredStateHandler> stateHandler,
        const ResultSubpartitionInfoPOD& channelInfo,
        int oldSubtaskIndex)
    {
        if (source->GetPos() != sourceOffset) {
            source->Seek(sourceOffset);
        }

        int length = serializer->ReadLength2(source);

        while (length > 0) {
            auto bufferWithContext = stateHandler->getBuffer(channelInfo);

            try {
                while (length > 0 && bufferWithContext.buffer_->isWritable()) {
                    length -= serializer->ReadData2(source, bufferWithContext.buffer_, length);
                }
            } catch (const std::exception& e) {
                bufferWithContext.close();
                throw;
            }
            stateHandler->recover(channelInfo, oldSubtaskIndex, bufferWithContext);
        }
    }

    void readChunkByFsForInputChannel(
        std::ifstream& source,
        long long sourceOffset,
        std::shared_ptr<InputChannelRecoveredStateHandler> stateHandler,
        const InputChannelInfo& channelInfo,
        int oldSubtaskIndex)
    {
        if (source.tellg() != sourceOffset) {
            source.seekg(sourceOffset);
        }

        int length = serializer->ReadLength(source);

        while (length > 0) {
            bool isVb = stateHandler->usesObjectSegment(channelInfo);
            auto bufferWithContext = isVb ? newVbTempInputBufferWithContext() : newTempInputBufferWithContext();

            try {
                while (length > 0 && bufferWithContext.buffer_->isWritable()) {
                    length -= serializer->ReadData(source, bufferWithContext.buffer_, length);
                }
            } catch (const std::exception& e) {
                bufferWithContext.close();
                throw;
            }
            stateHandler->recover(channelInfo, oldSubtaskIndex, bufferWithContext);
        }
    }

    void readChunkByFsForResultSubpartition(
        std::ifstream& source,
        long long sourceOffset,
        std::shared_ptr<ResultSubpartitionRecoveredStateHandler> stateHandler,
        const ResultSubpartitionInfoPOD& channelInfo,
        int oldSubtaskIndex)
    {
        if (source.tellg() != sourceOffset) {
            source.seekg(sourceOffset);
        }

        int length = serializer->ReadLength(source);

        while (length > 0) {
            auto bufferWithContext = stateHandler->getBuffer(channelInfo);

            try {
                while (length > 0 && bufferWithContext.buffer_->isWritable()) {
                    length -= serializer->ReadData(source, bufferWithContext.buffer_, length);
                }
            } catch (const std::exception& e) {
                bufferWithContext.close();
                throw;
            }
            stateHandler->recover(channelInfo, oldSubtaskIndex, bufferWithContext);
        }
    }
};
class SequentialChannelStateReaderOPImpl : public SequentialChannelStateReader {
public:
    void readInputData(const std::vector<std::shared_ptr<InputGate>>& inputGates) override
    {
    }
    void readOutputData(
        const std::vector<std::shared_ptr<ResultPartitionWriter>>& writers, bool notifyAndBlockOnCompletion) override
    {
    }
    void close() override
    {
    }
};

using InputChannelHandleGroup =
    std::map<std::string, std::pair<std::shared_ptr<StreamStateHandle>, std::vector<InputChannelStateHandle>>>;

using ResultSubpartitionHandleGroup =
    std::map<std::string, std::pair<std::shared_ptr<StreamStateHandle>, std::vector<ResultSubpartitionStateHandle>>>;
class SequentialChannelStateReaderImpl : public SequentialChannelStateReader {
public:
    explicit SequentialChannelStateReaderImpl(
        std::shared_ptr<TaskStateSnapshot> taskStateSnapshot, std::shared_ptr<OmniTaskBridge> omniTaskBridge)
        : taskStateSnapshot(taskStateSnapshot),
          serializer(std::make_shared<ChannelStateSerializerImpl>()),
          omniTaskBridge_(omniTaskBridge)
    {
        chunkReader = std::make_shared<ChannelStateChunkReader>(serializer);
    }

    void readInputData(const std::vector<std::shared_ptr<InputGate>>& inputGates) override;

    void readOutputData(
        const std::vector<std::shared_ptr<ResultPartitionWriter>>& writers, bool notifyAndBlockOnCompletion) override;

    void close() override;

    void readByInputChannel(
        std::shared_ptr<InputChannelRecoveredStateHandler> stateHandler,
        const InputChannelHandleGroup& streamStateHandleListMap);

    void readByResultSubpartition(
        std::shared_ptr<ResultSubpartitionRecoveredStateHandler> stateHandler,
        const ResultSubpartitionHandleGroup& streamStateHandleListMap);
    void readSequentiallyByInputChannel(
        std::shared_ptr<StreamStateHandle> streamStateHandle,
        std::vector<InputChannelStateHandle> channelStateHandles,
        std::shared_ptr<InputChannelRecoveredStateHandler> stateHandle);
    void readSequentiallyByResultSubpartition(
        std::shared_ptr<StreamStateHandle> streamStateHandle,
        std::vector<ResultSubpartitionStateHandle> channelStateHandles,
        std::shared_ptr<ResultSubpartitionRecoveredStateHandler> stateHandle);
    InputChannelHandleGroup groupByDelegateByInputChannel(std::vector<std::shared_ptr<OperatorSubtaskState>> states);
    ResultSubpartitionHandleGroup groupByDelegateByResultSubpartition(
        std::vector<std::shared_ptr<OperatorSubtaskState>> states);
    std::vector<std::shared_ptr<OperatorSubtaskState>> streamSubtaskStates();

private:
    std::vector<RescaledOffset<InputChannelInfo>> extractOffsetsSortedByInputChannel(
        const std::vector<InputChannelStateHandle>& channelStateHandles);
    std::vector<RescaledOffset<ResultSubpartitionInfoPOD>> extractOffsetsSortedByResultSubpartition(
        const std::vector<ResultSubpartitionStateHandle>& channelStateHandles);
    std::vector<RescaledOffset<InputChannelInfo>> extractOffsetsByInputChannel(const InputChannelStateHandle& handle);
    std::vector<RescaledOffset<ResultSubpartitionInfoPOD>> extractOffsetsByResultSubpartition(
        const ResultSubpartitionStateHandle& handle);
    std::shared_ptr<TaskStateSnapshot> taskStateSnapshot;
    std::shared_ptr<ChannelStateSerializer> serializer;
    std::shared_ptr<ChannelStateChunkReader> chunkReader;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
};
} // namespace omnistream
#endif // SEQUENTIAL_CHANNEL_STATE_READER_IMPL_H
