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

#ifndef OMNISTREAM_OPERATORSTATERESTOREOPERATION_H
#define OMNISTREAM_OPERATORSTATERESTOREOPERATION_H

#include <unordered_map>
#include <set>
#include <vector>
#include <string>
#include <memory>
#include <stdexcept>
#include <limits>
#include <utility>
#include <nlohmann/json.hpp>
#include "core/include/common.h"
#include "PartitionableListState.h"
#include "RegisteredBroadcastStateBackendMetaInfo.h"
#include "state/bridge/OmniTaskBridge.h"
#include "OperatorStreamStateHandle.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"
#include "RegisteredOperatorStateBackendMetaInfo.h"
#include "core/memory/DataInputDeserializer.h"
#include "state/memory/ByteStreamStateHandle.h"

class OperatorStateRestoreOperation {
public:
    OperatorStateRestoreOperation(
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates,
        const std::vector<std::shared_ptr<OperatorStateHandle>>& stateHandles,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge)
        : registeredOperatorStates_(registeredOperatorStates),
          registeredBroadcastStates_(registeredBroadcastStates),
          stateHandles_(stateHandles),
          omniTaskBridge_(omniTaskBridge)
    {
    }

    ~OperatorStateRestoreOperation() = default;

    void restore()
    {
        if (stateHandles_.empty()) {
            return;
        }
        for (auto& stateHandle : stateHandles_) {
            if (stateHandle == nullptr) {
                continue;
            }
            auto streamStateHandle = std::dynamic_pointer_cast<OperatorStreamStateHandle>(stateHandle);
            if (streamStateHandle) {
                auto stateNameToPartitionOffsets = stateHandle->getStateNameToPartitionOffsets();
                auto delegate = stateHandle->getDelegateStateHandle();
                auto json = TaskStateSnapshotSerializer::parseOperatorStreamStateHandle(streamStateHandle);
                std::string handleJson = to_string(json);
                auto stateMetaInfoSnapshots = omniTaskBridge_->readOperatorMetaData(handleJson);
                std::vector<uint8_t> stateData;
                bool stateDataLoaded = false;

                for (auto& snapshot : stateMetaInfoSnapshots) {
                    const std::string& stateName = snapshot.getName();
                    if (stateNameToPartitionOffsets.find(stateName) == stateNameToPartitionOffsets.end()) {
                        continue;
                    }
                    if (!isSupportedRestoredState(stateName)) {
                        continue;
                    }
                    auto metaInfo = std::make_shared<RegisteredOperatorStateBackendMetaInfo>(snapshot);
                    if (!stateDataLoaded) {
                        stateData = readOperatorStateData(delegate, handleJson);
                        stateDataLoaded = true;
                    }
                    deserializeOperatorStateValues(metaInfo, stateData, stateNameToPartitionOffsets);
                }
            }
        }
    }

    void deserializeOperatorStateValues(
        std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> metaInfo,
        const std::vector<uint8_t>& stateData,
        std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo> stateNameToPartitionOffsets)
    {
        if (stateData.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
            INFO_RELEASE("Error: Operator state handle is too large to deserialize in memory: " << stateData.size());
            THROW_LOGIC_EXCEPTION("Operator state handle is too large to deserialize in memory: " << stateData.size());
        }
        DataInputDeserializer in(stateData.empty() ? nullptr : stateData.data(), static_cast<int>(stateData.size()), 0);
        TypeSerializer* serializer = metaInfo->getStateSerializer();
        if (serializer == nullptr) {
            return;
        }
        for (auto& entry : stateNameToPartitionOffsets) {
            auto name = entry.first;
            if (name != metaInfo->getName()) {
                continue;
            }

            auto offsets = entry.second.getOffsets();
            // 根据 stateName 判断属于什么类型的数据
            if (typeByteStateNames.find(name) != typeByteStateNames.end()) {
                auto listState = getOrCreateOperatorListState<std::vector<uint8_t>>(name, metaInfo);
                for (auto& offset : offsets) {
                    validateOffset(name, offset, stateData.size());
                    in.setPosition(static_cast<size_t>(offset));
                    auto rawValue = static_cast<std::vector<uint8_t>*>(serializer->deserialize(in));
                    if (rawValue == nullptr) {
                        INFO_RELEASE(
                            "ERROR: Failed to deserialize operator state: " << name
                                                                            << ", offset=" << std::to_string(offset));
                        throw std::runtime_error(
                            "Failed to deserialize operator state: " + name + ", offset=" + std::to_string(offset));
                    }
                    std::unique_ptr<std::vector<uint8_t>> value(rawValue);
                    listState->add(*value);
                }
            }

            if (typeLongStateNames.find(name) != typeLongStateNames.end()) {
                auto listState = getOrCreateOperatorListState<long>(name, metaInfo);
                for (auto& offset : offsets) {
                    validateOffset(name, offset, stateData.size());
                    in.setPosition(static_cast<size_t>(offset));
                    auto rawValue = static_cast<long*>(serializer->deserialize(in));
                    if (rawValue == nullptr) {
                        INFO_RELEASE(
                            "ERROR: Failed to deserialize operator state: " << name
                                                                            << ", offset=" << std::to_string(offset));
                        throw std::runtime_error(
                            "Failed to deserialize operator state: " + name + ", offset=" + std::to_string(offset));
                    }
                    std::unique_ptr<long> value(rawValue);
                    listState->add(*value);
                }
            }
        }
    }

private:
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates_;
    std::vector<std::shared_ptr<OperatorStateHandle>> stateHandles_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;

    static constexpr size_t READ_CHUNK_SIZE = 4096;

    std::vector<uint8_t> readOperatorStateData(
        const std::shared_ptr<StreamStateHandle>& stateHandle, const std::string& handleJson)
    {
        if (stateHandle == nullptr) {
            INFO_RELEASE("Error: Operator state delegate handle is null.");
            THROW_LOGIC_EXCEPTION("Operator state delegate handle is null.");
        }

        auto inMemoryBytes = stateHandle->AsBytesIfInMemory();
        if (inMemoryBytes.has_value()) {
            return std::move(inMemoryBytes.value());
        }

        if (omniTaskBridge_ == nullptr) {
            INFO_RELEASE("Error: Cannot restore remote operator state without OmniTaskBridge.");
            THROW_LOGIC_EXCEPTION("Cannot restore remote operator state without OmniTaskBridge.");
        }

        jobject inputStream = nullptr;
        try {
            long expectedStateSize = stateHandle->GetStateSize();
            if (expectedStateSize < 0) {
                INFO_RELEASE("Error: Operator state handle has negative state size: " << expectedStateSize);
                THROW_LOGIC_EXCEPTION("Operator state handle has negative state size: " << expectedStateSize);
            }
            if (expectedStateSize == 0) {
                return {};
            }

            inputStream = omniTaskBridge_->getSavepointInputStream(handleJson);
            if (inputStream == nullptr) {
                INFO_RELEASE("Error: Failed to open operator state input stream through OmniTaskBridge.");
                THROW_LOGIC_EXCEPTION("Failed to open operator state input stream through OmniTaskBridge.");
            }

            std::vector<uint8_t> data;
            std::vector<uint8_t> chunk(READ_CHUNK_SIZE);
            while (data.size() < static_cast<size_t>(expectedStateSize)) {
                size_t remaining = static_cast<size_t>(expectedStateSize) - data.size();
                size_t readLength = remaining < READ_CHUNK_SIZE ? remaining : READ_CHUNK_SIZE;
                int read = omniTaskBridge_->ReadSavepointInputStream(
                    inputStream, reinterpret_cast<int8_t*>(chunk.data()), 0, readLength);
                if (read < 0) {
                    INFO_RELEASE(
                        "Error: Operator state input stream ended before expected size: expected="
                        << expectedStateSize << ", actual=" << data.size() << ", read=" << read);
                    THROW_LOGIC_EXCEPTION(
                        "Operator state input stream ended before expected size: expected="
                        << expectedStateSize << ", actual=" << data.size() << ", read=" << read);
                }
                if (read == 0) {
                    INFO_RELEASE(
                        "Error: Operator state input stream returned zero bytes before expected size: expected="
                        << expectedStateSize << ", actual=" << data.size());
                    THROW_LOGIC_EXCEPTION(
                        "Operator state input stream returned zero bytes before expected size: expected="
                        << expectedStateSize << ", actual=" << data.size());
                }
                data.insert(data.end(), chunk.begin(), chunk.begin() + read);
            }
            jobject inputStreamToClose = inputStream;
            inputStream = nullptr;
            omniTaskBridge_->closeSavepointInputStream(inputStreamToClose);
            return data;
        } catch (...) {
            if (inputStream != nullptr) {
                jobject inputStreamToClose = inputStream;
                inputStream = nullptr;
                try {
                    omniTaskBridge_->closeSavepointInputStream(inputStreamToClose);
                } catch (const std::exception& e) {
                    INFO_RELEASE(
                        "Error: Failed to close operator state input stream after restore failure: " << e.what());
                } catch (...) {
                    INFO_RELEASE("Error: Failed to close operator state input stream after restore failure.");
                }
            }
            throw;
        }
    }

    static void validateOffset(const std::string& stateName, long offset, size_t stateSize)
    {
        if (offset < 0 || static_cast<size_t>(offset) >= stateSize) {
            INFO_RELEASE(
                "Error: Invalid operator state offset for state " << stateName << ": offset=" << offset
                                                                  << ", stateSize=" << stateSize);
            THROW_LOGIC_EXCEPTION(
                "Invalid operator state offset for state " << stateName << ": offset=" << offset
                                                           << ", stateSize=" << stateSize);
        }
    }

    static bool isSupportedRestoredState(const std::string& stateName)
    {
        return typeByteStateNames.find(stateName) != typeByteStateNames.end() ||
               typeLongStateNames.find(stateName) != typeLongStateNames.end();
    }

    template <typename T>
    std::shared_ptr<PartitionableListState<T>> getOrCreateOperatorListState(
        const std::string& stateName, const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& metaInfo)
    {
        auto existing = registeredOperatorStates_->find(stateName);
        if (existing != registeredOperatorStates_->end()) {
            auto listState = std::dynamic_pointer_cast<PartitionableListState<T>>(existing->second);
            if (listState == nullptr) {
                INFO_RELEASE("Error: Restored operator state type mismatch for state: " << stateName);
                throw std::runtime_error("Restored operator state type mismatch for state: " + stateName);
            }
            validateOperatorStateMetaInfo(stateName, listState->getStateMetaInfo(), metaInfo);
            return listState;
        }

        auto listState = std::make_shared<PartitionableListState<T>>(metaInfo);
        registeredOperatorStates_->emplace(stateName, listState);
        return listState;
    }

    static void validateOperatorStateMetaInfo(
        const std::string& stateName,
        const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& existingMetaInfo,
        const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& restoredMetaInfo)
    {
        if (existingMetaInfo == nullptr || restoredMetaInfo == nullptr) {
            INFO_RELEASE("ERROR: Restored operator state meta info is null for state: " << stateName);
            throw std::runtime_error("Restored operator state meta info is null for state: " + stateName);
        }
        if (existingMetaInfo->getName() != restoredMetaInfo->getName() ||
            existingMetaInfo->getAssignmentMode() != restoredMetaInfo->getAssignmentMode()) {
            INFO_RELEASE("ERROR: Restored operator state meta info mismatch for state: " << stateName);
            throw std::runtime_error("Restored operator state meta info mismatch for state: " + stateName);
        }

        TypeSerializer* existingSerializer = existingMetaInfo->getStateSerializer();
        TypeSerializer* restoredSerializer = restoredMetaInfo->getStateSerializer();
        if (existingSerializer == nullptr || restoredSerializer == nullptr) {
            INFO_RELEASE("ERROR: Restored operator state serializer is null for state: " << stateName);
            throw std::runtime_error("Restored operator state serializer is null for state: " + stateName);
        }
        if (existingSerializer->getBackendId() != restoredSerializer->getBackendId()) {
            INFO_RELEASE("ERROR: Restored operator state serializer mismatch for state: " << stateName);
            throw std::runtime_error("Restored operator state serializer mismatch for state: " + stateName);
        }
    }

    inline static std::set<std::string> typeByteStateNames = {
        "SourceReaderState", "writer_raw_states", "streaming_committer_raw_states"};

    inline static std::set<std::string> typeLongStateNames = {"watermark", "elements-count-state"};
};

#endif // OMNISTREAM_OPERATORSTATERESTOREOPERATION_H
