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

#include "RecoveredChannelStateHandler.h"
#include "runtime/buffer/MemoryBufferConsumer.h"
#include "runtime/buffer/OriginalNetworkBufferRecycler.h"
#include "event/SubtaskConnectionDescriptor.h"
#include "io/network/api/serialization/EventSerializer.h"
#include "core/include/omni_const.h"

#include <memory>

namespace omnistream {
namespace {
class BufferReferenceGuard {
public:
    explicit BufferReferenceGuard(Buffer* buffer) : buffer_(buffer)
    {
    }

    BufferReferenceGuard(const BufferReferenceGuard&) = delete;
    BufferReferenceGuard& operator=(const BufferReferenceGuard&) = delete;

    ~BufferReferenceGuard()
    {
        if (buffer_ == nullptr) {
            return;
        }
        try {
            buffer_->RecycleBuffer();
        } catch (const std::exception& e) {
            INFO_RELEASE("ERROR: Failed to recycle a recovered input buffer reference: " << e.what());
        } catch (...) {
            INFO_RELEASE("ERROR: Failed to recycle a recovered input buffer reference due to an unknown exception");
        }
    }

    Buffer* Get() const
    {
        return buffer_;
    }

    Buffer* Release()
    {
        Buffer* buffer = buffer_;
        buffer_ = nullptr;
        return buffer;
    }

private:
    Buffer* buffer_;
};
} // namespace

ResultSubpartitionRecoveredStateHandler::ResultSubpartitionRecoveredStateHandler(
    std::vector<std::shared_ptr<ResultPartitionWriter>> writers,
    bool notifyAndBlockOnCompletion,
    std::shared_ptr<InflightDataRescalingDescriptor> channelMapping)
    : writers_(std::move(writers)),
      notifyAndBlockOnCompletion_(notifyAndBlockOnCompletion),
      channelMapping_(std::move(channelMapping))
{
}

ResultSubpartitionRecoveredStateHandler::~ResultSubpartitionRecoveredStateHandler()
{
    this->close();
}

ResultSubpartitionRecoveredStateHandler::BufferWithContext ResultSubpartitionRecoveredStateHandler::getBuffer(
    const ResultSubpartitionInfoPOD& subpartitionInfo)
{
    auto channels = getMappedChannels(subpartitionInfo);

    if (channels.empty()) {
        throw std::runtime_error("No mapped channels found");
    }
    BufferBuilder* bufferBuilder = channels.at(0)->requestBufferBuilderBlocking();
    return BufferWithContext(ChannelStateByteBuffer::wrap(bufferBuilder), bufferBuilder);
}

void ResultSubpartitionRecoveredStateHandler::recover(
    const ResultSubpartitionInfoPOD& subpartitionInfo, int oldSubtaskIndex, const BufferWithContext& bufferWithContext)
{
    BufferBuilder* bufferBuilder = bufferWithContext.context_;
    auto builderGuard =
        std::unique_ptr<BufferBuilder, void (*)(BufferBuilder*)>(bufferBuilder, [](BufferBuilder* builder) {
            if (builder == nullptr) {
                return;
            }
            builder->close();
            delete builder;
        });
    auto bufferConsumer = bufferBuilder->createBufferConsumerFromBeginning();
    bufferBuilder->finish();
    if (!bufferConsumer->isDataAvailable()) {
        bufferConsumer->close();
        return;
    }

    auto channels = getMappedChannels(subpartitionInfo);
    if (channels.empty()) {
        throw std::runtime_error("No mapped channels found in recover()");
    }
    auto buffer = bufferConsumer->buildForPeek();
    auto peekBufferGuard = std::unique_ptr<Buffer, void (*)(Buffer*)>(buffer, [](Buffer* peekBuffer) {
        if (peekBuffer == nullptr) {
            return;
        }
        peekBuffer->RecycleBuffer();
        delete peekBuffer;
    });
    if (buffer == nullptr) {
        bufferConsumer->close();
        throw std::runtime_error("Failed to build recovered buffer");
    }
    int bufferSize = buffer->GetSize();
    if (bufferSize > IO_SIZE_512M) {
        INFO_RELEASE("Error: invalid buffer size:" << bufferSize);
        bufferConsumer->close();
        return;
    }
    bool currentConsumerTransferred = false;
    try {
        int cnt = 0;
        for (const auto& item : channels) {
            auto channelSelector =
                std::make_shared<SubtaskConnectionDescriptor>(subpartitionInfo.getSubPartitionIdx(), oldSubtaskIndex);
            INFO_RELEASE("send recover buffer :" << item->getSubpartitionInfo().toString());
            item->addRecovered(EventSerializer::ToBufferConsumer(channelSelector, false));
            if (cnt > 0) {
                auto segment = buffer->GetSegment();
                int bufferLength = buffer->GetSize();
                if (segment->isObjectSegment()) {
                    ObjectSegment* newObjectSegment = new ObjectSegment(bufferLength);
                    const ObjectSegment* objectSegment = dynamic_cast<const ObjectSegment*>(segment);
                    newObjectSegment->put(0, objectSegment, buffer->GetOffset(), bufferLength);
                    auto* copiedBuffer =
                        new VectorBatchBuffer(newObjectSegment, std::make_shared<DeepCopiedObjectBufferRecycler>());
                    copiedBuffer->SetSize(bufferLength);
                    bufferConsumer = std::make_shared<ObjectBufferConsumer>(copiedBuffer, bufferLength);
                } else {
                    MemorySegment* memorySegment = reinterpret_cast<NetworkBuffer*>(buffer)->getMemorySegment();
                    uint8_t* newBufferAddress = new uint8_t[bufferSize];
                    if (newBufferAddress == nullptr) {
                        INFO_RELEASE("Error: malloc failed.");
                        throw std::invalid_argument("malloc failed");
                    }
                    MemorySegment* newMemorySegment = new MemorySegment(newBufferAddress, bufferSize);
                    newMemorySegment->put(0, reinterpret_cast<uint8_t*>(memorySegment->getData()), 0, bufferSize);
                    ::datastream::NetworkBuffer* newNetworkBuffer = new ::datastream::NetworkBuffer(
                        newMemorySegment,
                        bufferSize,
                        0,
                        std::make_shared<OriginalNetworkBufferRecycler>(),
                        ObjectBufferDataType::DATA_BUFFER,
                        true);
                    bufferConsumer = std::make_shared<datastream::MemoryBufferConsumer>(newNetworkBuffer, bufferSize);
                }
                currentConsumerTransferred = false;
            }
            item->addRecovered(bufferConsumer);
            currentConsumerTransferred = true;
            cnt++;
        }
    } catch (const std::exception& e) {
        if (!currentConsumerTransferred && bufferConsumer != nullptr && !bufferConsumer->isClose()) {
            bufferConsumer->close();
        }
        INFO_RELEASE("ERROR: ResultSubpartitionRecoveredStateHandler::recover exception: " << e.what());
        throw;
    } catch (...) {
        if (!currentConsumerTransferred && bufferConsumer != nullptr && !bufferConsumer->isClose()) {
            bufferConsumer->close();
        }
        INFO_RELEASE("ERROR: ResultSubpartitionRecoveredStateHandler::recover unknown exception");
        throw;
    }
}

void ResultSubpartitionRecoveredStateHandler::close()
{
    for (auto& writer : writers_) {
        if (auto checkpointedWriter = std::dynamic_pointer_cast<CheckpointedResultPartition>(writer)) {
            checkpointedWriter->finishReadRecoveredState(notifyAndBlockOnCompletion_);
        }
    }
    INFO_RELEASE(
        "Close ResultSubpartitionRecoveredStateHandler, finishReadRecoveredState writers size:" << writers_.size());
}

std::shared_ptr<CheckpointedResultSubpartition> ResultSubpartitionRecoveredStateHandler::getSubpartition(
    int partitionIndex, int subPartitionIdx)
{
    LOG("ResultSubpartitionRecoveredStateHandler getSubpartition111");
    auto writer = writers_.at(partitionIndex);

    auto checkpointedWriter = std::dynamic_pointer_cast<omnistream::CheckpointedResultPartition>(writer);
    if (!checkpointedWriter) {
        LOG("ResultSubpartitionRecoveredStateHandler getSubpartition222");
        throw std::runtime_error(
            "Cannot restore state to a non-checkpointable partition type, partitionIndex=" +
            std::to_string(partitionIndex));
    }
    LOG("ResultSubpartitionRecoveredStateHandler getSubpartition333");
    auto checkpointedSubpartition = checkpointedWriter->getCheckpointedSubpartition(subPartitionIdx);
    if (!checkpointedSubpartition) {
        LOG("ResultSubpartitionRecoveredStateHandler getSubpartition555");
        throw std::runtime_error(
            "Checkpointed subpartition is not a PipelinedSubpartition, partitionIndex=" +
            std::to_string(partitionIndex) + ", subPartitionIdx=" + std::to_string(subPartitionIdx));
    }
    LOG("ResultSubpartitionRecoveredStateHandler getSubpartition666");
    return checkpointedSubpartition;
}

std::vector<std::shared_ptr<CheckpointedResultSubpartition>> ResultSubpartitionRecoveredStateHandler::getMappedChannels(
    const ResultSubpartitionInfoPOD& subpartitionInfo)
{
    LOG("ResultSubpartitionRecoveredStateHandler getMappedChannels111");
    auto it = rescaledChannels_.find(subpartitionInfo);
    if (it != rescaledChannels_.end()) {
        return it->second;
    }
    LOG("getMappedChannels add InfoPOD: " << subpartitionInfo.toString());
    auto pipelinedSubpartitions = calculateMapping(subpartitionInfo);
    rescaledChannels_.emplace(subpartitionInfo, pipelinedSubpartitions);
    return pipelinedSubpartitions;
}

std::vector<std::shared_ptr<CheckpointedResultSubpartition>> ResultSubpartitionRecoveredStateHandler::calculateMapping(
    const ResultSubpartitionInfoPOD& info)
{
    int pIdx = info.getPartitionIdx();

    auto mapping =
        channelMapping_ ? channelMapping_->GetChannelMapping(pIdx) : IdentityRescaleMappings::SYMMETRIC_IDENTITY;

    // 纯恢复 / 未 rescale：直接按 identity 映射，不要去 invert SYMMETRIC_IDENTITY
    if (!mapping || mapping->isIdentity()) {
        return {getSubpartition(pIdx, info.getSubPartitionIdx())};
    }

    if (oldToNewMappings_.find(pIdx) == oldToNewMappings_.end()) {
        oldToNewMappings_.emplace(pIdx, mapping->invert());
    }

    const auto& oldToNewMapping = oldToNewMappings_.at(pIdx);
    //        std::vector<std::shared_ptr<PipelinedSubpartition>> subpartitions;
    std::vector<std::shared_ptr<CheckpointedResultSubpartition>> subpartitions;

    auto mappedIndexes = oldToNewMapping.getMappedIndexes(info.getSubPartitionIdx());
    for (int newIndex : mappedIndexes) {
        subpartitions.push_back(getSubpartition(pIdx, newIndex));
    }

    if (subpartitions.empty()) {
        LOG("ERROR: Recovered a buffer that has no mapping, partitionIdx="
            << std::to_string(info.getPartitionIdx())
            << ", subPartitionIdx=" << std::to_string(info.getSubPartitionIdx()));
        throw std::runtime_error(
            "Recovered a buffer that has no mapping, partitionIdx=" + std::to_string(info.getPartitionIdx()) +
            ", subPartitionIdx=" + std::to_string(info.getSubPartitionIdx()));
    }
    return subpartitions;
}

InputChannelRecoveredStateHandler::~InputChannelRecoveredStateHandler()
{
    this->close();
}

RecoveredChannelStateHandler<InputChannelInfo, Buffer*>::BufferWithContext InputChannelRecoveredStateHandler::getBuffer(
    const InputChannelInfo& inputChannelInfo)
{
    auto channel = getMappedChannels(inputChannelInfo)[0];

    auto buffer = channel->requestBufferBlocking();
    // support nothing
    return BufferWithContext(ChannelStateByteBuffer::wrap(&*buffer), &*buffer);
}

void InputChannelRecoveredStateHandler::recover(
    const InputChannelInfo& inputChannelInfo, int oldSubtaskIndex, const BufferWithContext& bufferWithContext)
{
    Buffer* buffer = bufferWithContext.context_;
    BufferReferenceGuard originalReference(buffer);

    if (buffer->GetSize() == 0) {
        return;
    }

    try {
        if (buffer->GetSize() > 0) {
            auto channels = getMappedChannels(inputChannelInfo);
            if (channels.empty()) {
                throw std::runtime_error("No mapped channels found in InputChannelRecoveredStateHandler::recover");
            }

            for (size_t channelIndex = 0; channelIndex < channels.size(); channelIndex++) {
                const auto& item = channels[channelIndex];
                const bool lastChannel = channelIndex + 1 == channels.size();
                BufferReferenceGuard channelReference(
                    lastChannel ? originalReference.Release() : buffer->RetainBuffer());

                INFO_RELEASE("send input recover:" << item->getChannelInfo().toString());
                item->onRecoveredStateBuffer(
                    EventSerializer::toBuffer(
                        std::make_shared<SubtaskConnectionDescriptor>(
                            oldSubtaskIndex, inputChannelInfo.getInputChannelIdx()),
                        false));
                item->onRecoveredStateBuffer2(channelReference.Get());
                // RecoveredInputChannel now owns exactly one reference for this mapped channel.
                channelReference.Release();
            }

            INFO_RELEASE(
                "Recovered state for gate " << inputChannelInfo.getGateIdx() << ", channel "
                                            << inputChannelInfo.getInputChannelIdx() << ", size " << buffer->GetSize()
                                            << ", mappedChannels=" << channels.size());
        }
    } catch (const std::exception& e) {
        INFO_RELEASE("ERROR: InputChannelRecoveredStateHandler::recover exception: " << e.what());
        throw std::runtime_error("failed to InputChannelRecoveredStateHandler recover: " + std::string(e.what()));
    }
}

void InputChannelRecoveredStateHandler::close()
{
    for (const auto& inputGate : inputGates) {
        inputGate->FinishInnerRecoveredState();
    }
    INFO_RELEASE(
        "Close InputChannelRecoveredStateHandler, FinishInnerRecoveredState inputGate size:" << inputGates.size());
}

bool InputChannelRecoveredStateHandler::usesObjectSegment(const InputChannelInfo& channelInfo)
{
    const auto channels = getMappedChannels(channelInfo);
    if (channels.empty()) {
        throw std::runtime_error("Cannot determine recovered buffer kind: no mapped input channel");
    }

    const bool objectPool = channels.front()->isObjectBufferPool();

    return objectPool;
}

std::shared_ptr<RecoveredInputChannel> InputChannelRecoveredStateHandler::getChannel(
    int gateIndex, int subPartitionIndex)
{
    auto inputChannel = inputGates.at(gateIndex)->getChannel(subPartitionIndex);
    auto inputChannel2 = std::dynamic_pointer_cast<RecoveredInputChannel>(inputChannel);
    if (!inputChannel2) {
        INFO_RELEASE("ERROR: Cannot restore state to a non-checkpointable partition type");
        throw std::runtime_error("Cannot restore state to a non-checkpointable partition type");
    }
    return inputChannel2;
}

std::vector<std::shared_ptr<RecoveredInputChannel>> InputChannelRecoveredStateHandler::calculateMapping(
    InputChannelInfo info)
{
    LOG("InputChannelRecoveredStateHandler calculateMapping111");
    int pIdx = info.getGateIdx();

    auto mapping =
        channelMapping ? channelMapping->GetChannelMapping(pIdx) : IdentityRescaleMappings::SYMMETRIC_IDENTITY;

    if (!mapping || mapping->isIdentity()) {
        return {getChannel(pIdx, info.getInputChannelIdx())};
    }

    if (oldToNewMappings.find(pIdx) == oldToNewMappings.end()) {
        oldToNewMappings.emplace(pIdx, mapping->invert());
    }

    const auto& oldToNewMapping = oldToNewMappings.at(pIdx);
    std::vector<std::shared_ptr<RecoveredInputChannel>> channels;

    auto mappedIndexes = oldToNewMapping.getMappedIndexes(info.getInputChannelIdx());
    for (int newIndex : mappedIndexes) {
        channels.push_back(getChannel(pIdx, newIndex));
    }

    if (channels.empty()) {
        throw std::runtime_error("Recovered a buffer that has no mapping");
    }
    LOG("InputChannelRecoveredStateHandler calculateMapping end");
    return channels;
}

std::vector<std::shared_ptr<RecoveredInputChannel>> InputChannelRecoveredStateHandler::getMappedChannels(
    InputChannelInfo channelInfo)
{
    auto it = rescaledChannels.find(channelInfo);
    if (it != rescaledChannels.end()) {
        return it->second;
    }
    LOG("getMappedChannels add ChannelInfo: " << channelInfo.toString());
    auto channels = calculateMapping(channelInfo);
    rescaledChannels.emplace(channelInfo, channels);
    return channels;
}
} // namespace omnistream
