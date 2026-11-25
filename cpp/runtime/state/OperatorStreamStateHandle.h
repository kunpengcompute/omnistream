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

#ifndef OMNISTREAM_OPERATORSTREAMSTATEHANDLE_H
#define OMNISTREAM_OPERATORSTREAMSTATEHANDLE_H

#include <typeinfo>
#include <string>
#include <unordered_map>
#include <vector>
#include <optional>
#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include <nlohmann/json.hpp>
#include "core/include/common.h"
#include "core/fs/FSDataInputStream.h"
#include "runtime/state/StreamStateHandleFactory.h"

#include "OperatorStateHandle.h"
#include "StreamStateHandle.h"
#include "PhysicalStateHandleID.h"

class OperatorStreamStateHandle : public OperatorStateHandle {
public:
    OperatorStreamStateHandle(
        std::unordered_map<std::string, StateMetaInfo>& stateNameToPartitionOffsets_,
        std::shared_ptr<StreamStateHandle> delegateStateHandle_)
        : stateNameToPartitionOffsets(stateNameToPartitionOffsets_),
          delegateStateHandle(delegateStateHandle_)
    {
    }

    explicit OperatorStreamStateHandle(const nlohmann::json& description)
    {
        if (!description.is_object()) {
            throw std::runtime_error("OperatorStreamStateHandle JSON must be an object.");
        }
        const nlohmann::json* delegateJson =
            FindFirstPresent(description, {"delegateStateHandle", "streamStateHandle", "metaDataState"});
        if (delegateJson == nullptr) {
            throw std::runtime_error(
                "OperatorStreamStateHandle JSON is missing delegateStateHandle/streamStateHandle/metaDataState.");
        }
        delegateStateHandle = StreamStateHandleFactory::from_json(*delegateJson);
        if (delegateStateHandle == nullptr) {
            throw std::runtime_error("OperatorStreamStateHandle delegate state handle is unsupported.");
        }

        const nlohmann::json* offsetsJson = FindFirstPresent(description, {"stateNameToPartitionOffsets"});
        if (offsetsJson == nullptr) {
            return;
        }
        if (!offsetsJson->is_object()) {
            throw std::runtime_error("OperatorStreamStateHandle stateNameToPartitionOffsets must be an object.");
        }
        for (auto& el : offsetsJson->items()) {
            if (el.key() == "@class") {
                continue;
            }
            std::string stateName = el.key();
            StateMetaInfo stateMetaInfo = parseStateMetaInfo(el.value());
            stateNameToPartitionOffsets[stateName] = stateMetaInfo;
        }
    }

    void DiscardState() override
    {
        if (delegateStateHandle == nullptr) {
            return;
        }
        delegateStateHandle->DiscardState();
    }

    long GetStateSize() const override
    {
        if (delegateStateHandle == nullptr) {
            return 0;
        }
        return delegateStateHandle->GetStateSize();
    }

    PhysicalStateHandleID getStreamStateHandleID() const
    {
        if (delegateStateHandle == nullptr) {
            throw std::runtime_error("OperatorStreamStateHandle delegate state handle is null.");
        }
        return delegateStateHandle->GetStreamStateHandleID();
    }

    std::shared_ptr<FSDataInputStream> OpenInputStream() const override
    {
        if (delegateStateHandle == nullptr) {
            throw std::runtime_error("OperatorStreamStateHandle delegate state handle is null.");
        }
        return delegateStateHandle->OpenInputStream();
    }

    std::optional<std::vector<uint8_t>> AsBytesIfInMemory() const override
    {
        if (delegateStateHandle == nullptr) {
            return std::nullopt;
        }
        return delegateStateHandle->AsBytesIfInMemory();
    }

    PhysicalStateHandleID GetStreamStateHandleID() const override
    {
        if (delegateStateHandle == nullptr) {
            throw std::runtime_error("OperatorStreamStateHandle delegate state handle is null.");
        }
        return delegateStateHandle->GetStreamStateHandleID();
    }

    std::unordered_map<std::string, StateMetaInfo> getStateNameToPartitionOffsets() const override
    {
        return stateNameToPartitionOffsets;
    }

    std::shared_ptr<StreamStateHandle> getDelegateStateHandle() const override
    {
        return delegateStateHandle;
    }

    bool operator==(const StreamStateHandle& other_) const override
    {
        const auto* other = dynamic_cast<const OperatorStreamStateHandle*>(&other_);
        return other && stateNameToPartitionOffsets == other->stateNameToPartitionOffsets &&
               delegateStateHandle == other->delegateStateHandle;
    }

    // 将stateNameToPartitionOffsets转换为json
    nlohmann::json toJson() const
    {
        nlohmann::json result;
        for (const auto& entry : stateNameToPartitionOffsets) {
            const auto& stateName = entry.first;
            const auto& stateMetaInfo = entry.second;
            nlohmann::json stateMetaInfoJson;
            stateMetaInfoJson["offsets"] = stateMetaInfo.getOffsets();
            // 将Mode枚举转换为字符串
            OperatorStateHandle::Mode mode = stateMetaInfo.getDistributionMode();
            std::string modeStr;
            switch (mode) {
                case OperatorStateHandle::Mode::SPLIT_DISTRIBUTE: modeStr = "SPLIT_DISTRIBUTE"; break;
                case OperatorStateHandle::Mode::UNION: modeStr = "UNION"; break;
                case OperatorStateHandle::Mode::BROADCAST: modeStr = "BROADCAST"; break;
                default: modeStr = "UNKNOWN"; break;
            }
            stateMetaInfoJson["distributionMode"] = modeStr;
            result[stateName] = stateMetaInfoJson;
        }
        return result;
    }

    std::string ToString() const override
    {
        nlohmann::json result;
        result["stateHandleName"] = "OperatorStreamStateHandle";
        if (getDelegateStateHandle() != nullptr) {
            result["streamStateHandle"] = nlohmann::json::parse(getDelegateStateHandle()->ToString());
            result["stateHandleID"] = nlohmann::json::parse(getStreamStateHandleID().ToString());
        } else {
            result["streamStateHandle"] = nullptr;
            result["stateHandleID"] = nullptr;
        }
        result["stateNameToPartitionOffsets"] = toJson();
        return result.dump();
    }

private:
    std::unordered_map<std::string, StateMetaInfo> stateNameToPartitionOffsets;
    std::shared_ptr<StreamStateHandle> delegateStateHandle;

    StateMetaInfo parseStateMetaInfo(const nlohmann::json& j)
    {
        if (!j.is_object()) {
            throw std::runtime_error("Operator state meta info JSON must be an object.");
        }
        if (!j.contains("distributionMode") || !j.at("distributionMode").is_string()) {
            throw std::runtime_error("Operator state meta info is missing string field 'distributionMode'.");
        }
        if (!j.contains("offsets") || !j.at("offsets").is_array()) {
            throw std::runtime_error("Operator state meta info is missing array field 'offsets'.");
        }

        auto mode = OperatorStateHandle::StrToMode(j.at("distributionMode").get<std::string>());
        const nlohmann::json* offsetsJson = &j.at("offsets");
        if (offsetsJson->size() == 2 && offsetsJson->at(0).is_string() && offsetsJson->at(1).is_array()) {
            offsetsJson = &offsetsJson->at(1);
        }

        std::vector<long> offsets;
        offsets.reserve(offsetsJson->size());
        for (const auto& offset : *offsetsJson) {
            if (!offset.is_number_integer()) {
                throw std::runtime_error("Operator state meta info offset must be an integer.");
            }
            offsets.push_back(offset.get<long>());
        }
        return StateMetaInfo(offsets, mode);
    }

    static const nlohmann::json* FindFirstPresent(const nlohmann::json& json, const std::vector<std::string>& fields)
    {
        for (const auto& field : fields) {
            if (json.contains(field) && !json.at(field).is_null()) {
                return &json.at(field);
            }
        }
        return nullptr;
    }
};

#endif // OMNISTREAM_OPERATORSTREAMSTATEHANDLE_H
