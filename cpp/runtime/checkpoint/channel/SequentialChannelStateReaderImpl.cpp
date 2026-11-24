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

#include "SequentialChannelStateReaderImpl.h"

#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <tuple>
#include <stdexcept>
#include <memory>
#include <sstream>
#include <functional>
#include <atomic>
#include <cstdint>
#include <thread>
#include <unistd.h>

std::string ExtractChkPath(const std::string& fullPath);

namespace {
std::atomic<uint64_t> restoreFileSequence{0};

std::string CreateUniqueRestorePath(const std::string& filePath, const char* stateType)
{
    const std::string checkpointPath = ExtractChkPath(filePath);
    if (checkpointPath.empty()) {
        throw std::runtime_error("Failed to extract checkpoint path from stream state handle: " + filePath);
    }

    std::ostringstream suffix;
    suffix << ".omnistream-restore-" << stateType << "-pid" << getpid() << "-thread" << std::this_thread::get_id()
           << "-seq" << restoreFileSequence.fetch_add(1, std::memory_order_relaxed);
    return "/tmp/" + checkpointPath + suffix.str();
}

class RestoreFileCleanupGuard {
public:
    RestoreFileCleanupGuard(std::ifstream& stream, std::string path) : stream_(stream), path_(std::move(path))
    {
    }

    ~RestoreFileCleanupGuard()
    {
        try {
            if (stream_.is_open()) {
                stream_.close();
            }
            const bool removed = std::filesystem::remove(path_);
            INFO_RELEASE("Channel state restore local file cleanup, path=" << path_ << ", removed=" << removed);
        } catch (const std::exception& e) {
            ERROR_RELEASE("Channel state restore local file cleanup failed, path=" << path_ << ", error=" << e.what());
        } catch (...) {
            ERROR_RELEASE("Channel state restore local file cleanup failed, path=" << path_ << ", unknown error");
        }
    }

private:
    std::ifstream& stream_;
    std::string path_;
};
} // namespace

void SequentialChannelStateReaderImpl::readInputData(const std::vector<std::shared_ptr<InputGate>>& inputGates)
{
    LOG("Read input data start.");
    auto stateHandler = std::make_shared<InputChannelRecoveredStateHandler>(
        inputGates, taskStateSnapshot->GetInputRescalingDescriptor());

    readByInputChannel(stateHandler, groupByDelegateByInputChannel(streamSubtaskStates()));
    LOG("Read input data end.");
}

void SequentialChannelStateReaderImpl::readOutputData(
    const std::vector<std::shared_ptr<ResultPartitionWriter>>& writers, bool notifyAndBlockOnCompletion)
{
    LOG("Read output data start.");
    auto stateHandler = std::make_shared<ResultSubpartitionRecoveredStateHandler>(
        writers, notifyAndBlockOnCompletion, taskStateSnapshot->GetOutputRescalingDescriptor());

    readByResultSubpartition(stateHandler, groupByDelegateByResultSubpartition(streamSubtaskStates()));
    LOG("Read output data end.");
}

void SequentialChannelStateReaderImpl::close()
{
}

void SequentialChannelStateReaderImpl::readByInputChannel(
    std::shared_ptr<InputChannelRecoveredStateHandler> stateHandler,
    const InputChannelHandleGroup& streamStateHandleListMap)
{
    LOG("readByInputChannel size: " << streamStateHandleListMap.size());
    for (auto& entry : streamStateHandleListMap) {
        readSequentiallyByInputChannel(entry.second.first, entry.second.second, stateHandler);
    }
}

void SequentialChannelStateReaderImpl::readByResultSubpartition(
    std::shared_ptr<ResultSubpartitionRecoveredStateHandler> stateHandler,
    const ResultSubpartitionHandleGroup& streamStateHandleListMap)
{
    LOG("readByResultSubpartition size: " << streamStateHandleListMap.size());
    for (auto& entry : streamStateHandleListMap) {
        readSequentiallyByResultSubpartition(entry.second.first, entry.second.second, stateHandler);
    }
}

std::string ExtractChkPath(const std::string& fullPath)
{
    size_t pos = fullPath.find("chk-");
    if (pos == std::string::npos) {
        LOG("ERROR: ExtractChkPath path failed.");
        return "";
    }
    return fullPath.substr(pos);
}

void SequentialChannelStateReaderImpl::readSequentiallyByInputChannel(
    std::shared_ptr<StreamStateHandle> streamStateHandle,
    std::vector<InputChannelStateHandle> channelStateHandles,
    std::shared_ptr<InputChannelRecoveredStateHandler> stateHandle)
{
    LOG("readSequentiallyByInputChannel start.");
    if (auto byteStreamStateHandle = std::dynamic_pointer_cast<ByteStreamStateHandle>(streamStateHandle)) {
        auto inputStream = byteStreamStateHandle->OpenInputStream();
        std::shared_ptr<ByteStateHandleInputStream> byteInputStream;
        if (byteInputStream = std::dynamic_pointer_cast<ByteStateHandleInputStream>(inputStream)) {
            serializer->ReadHeader2(byteInputStream);
        } else {
            LOG("ERROR: Failed to cast input stream to ByteStateHandleInputStream");
            throw std::runtime_error("Failed to cast input stream to ByteStateHandleInputStream");
        }

        for (const auto& offsetAndChannelInfo : extractOffsetsSortedByInputChannel(channelStateHandles)) {
            chunkReader->readChunkByByteStreamForInputChannel(
                byteInputStream,
                offsetAndChannelInfo.offset,
                stateHandle,
                offsetAndChannelInfo.channelInfo,
                offsetAndChannelInfo.oldSubtaskIndex);
        }
        LOG("readSequentiallyByInputChannel end");
    } else if (std::dynamic_pointer_cast<RelativeFileStateHandle>(streamStateHandle)) {
        auto filePath = streamStateHandle->GetStreamStateHandleID().getKeyString();
        auto tmpPath = CreateUniqueRestorePath(filePath, "input");
        INFO_RELEASE(
            "Channel state restore download start, type=input, source="
            << filePath << ", target=" << tmpPath << ", expectedSize=" << streamStateHandle->GetStateSize());
        if (!omniTaskBridge_->CallDownloadFileToLocal(*streamStateHandle, tmpPath)) {
            ERROR_RELEASE(
                "Channel state restore download failed, type=input, source=" << filePath << ", target=" << tmpPath);
            std::error_code cleanupError;
            const bool removed = std::filesystem::remove(tmpPath, cleanupError);
            INFO_RELEASE(
                "Channel state restore cleanup after download failure, type=input, target="
                << tmpPath << ", removed=" << removed << ", error=" << cleanupError.message());
            throw std::runtime_error("Failed to download input channel state file to " + tmpPath);
        }
        INFO_RELEASE(
            "Channel state restore download success, type=input, source=" << filePath << ", target=" << tmpPath);
        std::ifstream is(tmpPath, std::ios::binary);
        if (!is.is_open()) {
            ERROR_RELEASE(
                "Failed to open downloaded input channel state file, source=" << filePath << ", target=" << tmpPath);
            const bool removed = std::filesystem::remove(tmpPath);
            INFO_RELEASE(
                "Channel state restore cleanup after open failure, type=input, target=" << tmpPath
                                                                                        << ", removed=" << removed);
            throw std::ios_base::failure("Failed to open stream state handle input stream.");
        }
        INFO_RELEASE("Channel state restore local file opened, type=input, target=" << tmpPath);
        RestoreFileCleanupGuard fileCleanupGuard(is, tmpPath);

        serializer->ReadHeader(is);

        LOG("before extractOffsetsSortedByInputChannel");
        for (const auto& offsetAndChannelInfo : extractOffsetsSortedByInputChannel(channelStateHandles)) {
            LOG("before readChunkByFsForInputChannel");
            chunkReader->readChunkByFsForInputChannel(
                is,
                offsetAndChannelInfo.offset,
                stateHandle,
                offsetAndChannelInfo.channelInfo,
                offsetAndChannelInfo.oldSubtaskIndex);
            LOG("after readChunkByFsForInputChannel");
        }
        LOG("after extractOffsetsSortedByInputChannel");
        LOG("readSequentiallyByInputChannel end");
    } else {
        LOG("ERROR: streamStateHandle type is not excepted.");
    }
}

void SequentialChannelStateReaderImpl::readSequentiallyByResultSubpartition(
    std::shared_ptr<StreamStateHandle> streamStateHandle,
    std::vector<ResultSubpartitionStateHandle> channelStateHandles,
    std::shared_ptr<ResultSubpartitionRecoveredStateHandler> stateHandle)
{
    LOG("readSequentiallyByResultSubpartition start.");
    if (auto byteStreamStateHandle = std::dynamic_pointer_cast<ByteStreamStateHandle>(streamStateHandle)) {
        auto inputStream = byteStreamStateHandle->OpenInputStream();
        std::shared_ptr<ByteStateHandleInputStream> byteInputStream;
        if (byteInputStream = std::dynamic_pointer_cast<ByteStateHandleInputStream>(inputStream)) {
            serializer->ReadHeader2(byteInputStream);
        } else {
            LOG("ERROR: Failed to cast input stream to ByteStateHandleInputStream");
            throw std::runtime_error("Failed to cast input stream to ByteStateHandleInputStream");
        }

        for (const auto& offsetAndChannelInfo : extractOffsetsSortedByResultSubpartition(channelStateHandles)) {
            chunkReader->readChunkByByteStreamForResultSubpartition(
                byteInputStream,
                offsetAndChannelInfo.offset,
                stateHandle,
                offsetAndChannelInfo.channelInfo,
                offsetAndChannelInfo.oldSubtaskIndex);
        }
        LOG("readSequentiallyByResultSubpartition end");
    } else if (std::dynamic_pointer_cast<RelativeFileStateHandle>(streamStateHandle)) {
        auto filePath = streamStateHandle->GetStreamStateHandleID().getKeyString();
        LOG("readSequentiallyByResultSubpartition file path: " << filePath);
        auto tmpPath = CreateUniqueRestorePath(filePath, "output");
        INFO_RELEASE(
            "Channel state restore download start, type=output, source="
            << filePath << ", target=" << tmpPath << ", expectedSize=" << streamStateHandle->GetStateSize());
        if (!omniTaskBridge_->CallDownloadFileToLocal(*streamStateHandle, tmpPath)) {
            ERROR_RELEASE(
                "Channel state restore download failed, type=output, source=" << filePath << ", target=" << tmpPath);
            std::error_code cleanupError;
            const bool removed = std::filesystem::remove(tmpPath, cleanupError);
            INFO_RELEASE(
                "Channel state restore cleanup after download failure, type=output, target="
                << tmpPath << ", removed=" << removed << ", error=" << cleanupError.message());
            throw std::runtime_error("Failed to download result subpartition state file to " + tmpPath);
        }
        INFO_RELEASE(
            "Channel state restore download success, type=output, source=" << filePath << ", target=" << tmpPath);
        std::ifstream is(tmpPath, std::ios::binary);
        if (!is.is_open()) {
            ERROR_RELEASE(
                "Failed to open downloaded result subpartition state file, source=" << filePath
                                                                                    << ", target=" << tmpPath);
            const bool removed = std::filesystem::remove(tmpPath);
            INFO_RELEASE(
                "Channel state restore cleanup after open failure, type=output, target=" << tmpPath
                                                                                         << ", removed=" << removed);
            throw std::ios_base::failure("Failed to open stream state handle input stream.");
        }
        INFO_RELEASE("Channel state restore local file opened, type=output, target=" << tmpPath);
        RestoreFileCleanupGuard fileCleanupGuard(is, tmpPath);

        serializer->ReadHeader(is);

        for (const auto& offsetAndChannelInfo : extractOffsetsSortedByResultSubpartition(channelStateHandles)) {
            chunkReader->readChunkByFsForResultSubpartition(
                is,
                offsetAndChannelInfo.offset,
                stateHandle,
                offsetAndChannelInfo.channelInfo,
                offsetAndChannelInfo.oldSubtaskIndex);
        }
        LOG("readSequentiallyByResultSubpartition end");
    } else {
        LOG("ERROR: streamStateHandle type is not excepted.");
    }
}

std::vector<std::shared_ptr<OperatorSubtaskState>> SequentialChannelStateReaderImpl::streamSubtaskStates()
{
    std::vector<std::shared_ptr<OperatorSubtaskState>> subtaskStates;

    auto subtaskStateMappings = taskStateSnapshot->GetSubtaskStateMappings();

    for (const auto& entry : subtaskStateMappings) {
        subtaskStates.push_back(entry.second); // entry.second 应该是一个std::shared_ptr<OperatorSubtaskState>
    }

    return subtaskStates;
}

InputChannelHandleGroup SequentialChannelStateReaderImpl::groupByDelegateByInputChannel(
    std::vector<std::shared_ptr<OperatorSubtaskState>> states)
{
    InputChannelHandleGroup result;
    for (auto& state : states) {
        auto stateHandles = state->getInputChannelState();
        for (auto& handle : stateHandles) {
            auto delegate = handle->GetDelegate();
            auto key = delegate->GetStreamStateHandleID().getKeyString();

            auto& entry = result[key];
            if (!entry.first) {
                entry.first = delegate;
            }
            entry.second.push_back(*handle);
        }
    }
    return result;
}

ResultSubpartitionHandleGroup SequentialChannelStateReaderImpl::groupByDelegateByResultSubpartition(
    std::vector<std::shared_ptr<OperatorSubtaskState>> states)
{
    ResultSubpartitionHandleGroup result;
    for (auto& state : states) {
        auto stateHandles = state->getResultSubpartitionState();
        for (auto& handle : stateHandles) {
            auto delegate = handle->GetDelegate();
            auto key = delegate->GetStreamStateHandleID().getKeyString();

            auto& entry = result[key];
            if (!entry.first) {
                entry.first = delegate;
            }
            entry.second.push_back(*handle);
        }
    }
    return result;
}

std::vector<RescaledOffset<InputChannelInfo>> SequentialChannelStateReaderImpl::extractOffsetsSortedByInputChannel(
    const std::vector<InputChannelStateHandle>& channelStateHandles)
{
    std::vector<RescaledOffset<InputChannelInfo>> offsets;

    for (const auto& handle : channelStateHandles) {
        auto extractedOffsets = extractOffsetsByInputChannel(handle);
        offsets.insert(offsets.end(), extractedOffsets.begin(), extractedOffsets.end());
    }
    if (offsets.empty()) {
        LOG("channelStateHandles vec: " << channelStateHandles.size() << ", offsets vec: " << offsets.size());
        return offsets;
    }
    LOG("channelStateHandles vec: " << channelStateHandles.size() << ", offsets vec: " << offsets.size()
                                    << ", start off: " << offsets[0].offset
                                    << ", end off: " << offsets[offsets.size() - 1].offset
                                    << ", info: " << offsets[0].channelInfo.toString());

    std::sort(
        offsets.begin(),
        offsets.end(),
        [](const RescaledOffset<InputChannelInfo>& a, const RescaledOffset<InputChannelInfo>& b) {
            return a.offset < b.offset;
        });
    return offsets;
}

std::vector<RescaledOffset<ResultSubpartitionInfoPOD>>
SequentialChannelStateReaderImpl::extractOffsetsSortedByResultSubpartition(
    const std::vector<ResultSubpartitionStateHandle>& channelStateHandles)
{
    std::vector<RescaledOffset<ResultSubpartitionInfoPOD>> offsets;

    for (const auto& handle : channelStateHandles) {
        auto extractedOffsets = extractOffsetsByResultSubpartition(handle);
        offsets.insert(offsets.end(), extractedOffsets.begin(), extractedOffsets.end());
    }
    if (offsets.empty()) {
        LOG("channelStateHandles vec: " << channelStateHandles.size() << ", offsets vec: " << offsets.size());
        return offsets;
    }
    LOG("channelStateHandles vec: " << channelStateHandles.size() << ", offsets vec: " << offsets.size()
                                    << ", start off: " << offsets[0].offset
                                    << ", end off: " << offsets[offsets.size() - 1].offset
                                    << ", info: " << offsets[0].channelInfo.toString());
    std::sort(
        offsets.begin(),
        offsets.end(),
        [](const RescaledOffset<ResultSubpartitionInfoPOD>& a, const RescaledOffset<ResultSubpartitionInfoPOD>& b) {
            return a.offset < b.offset;
        });
    return offsets;
}

std::vector<RescaledOffset<InputChannelInfo>> SequentialChannelStateReaderImpl::extractOffsetsByInputChannel(
    const InputChannelStateHandle& handle)
{
    std::vector<RescaledOffset<InputChannelInfo>> offsets;

    auto handleOffsets = handle.GetOffsets();

    for (const auto& offset : handleOffsets) {
        offsets.emplace_back(offset, handle.GetInfo(), handle.GetSubtaskIndex());
    }
    return offsets;
}

std::vector<RescaledOffset<ResultSubpartitionInfoPOD>>
SequentialChannelStateReaderImpl::extractOffsetsByResultSubpartition(const ResultSubpartitionStateHandle& handle)
{
    std::vector<RescaledOffset<ResultSubpartitionInfoPOD>> offsets;

    auto handleOffsets = handle.GetOffsets();

    for (const auto& offset : handleOffsets) {
        offsets.emplace_back(offset, handle.GetInfo(), handle.GetSubtaskIndex());
    }
    return offsets;
}
