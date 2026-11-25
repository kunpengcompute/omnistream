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

#ifndef FLINK_TNEL_KEYGROUPSSTATEHANDLE_H
#define FLINK_TNEL_KEYGROUPSSTATEHANDLE_H
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/PhysicalStateHandleID.h"
#include "runtime/state/StreamStateHandleFactory.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <nlohmann/json.hpp>
#include <stdexcept>

class KeyGroupsStateHandle : public StreamStateHandle, public KeyedStateHandle {
public:
    KeyGroupsStateHandle(
        const KeyGroupRangeOffsets& groupRangeOffsets, std::shared_ptr<StreamStateHandle> streamStateHandle)
        : KeyGroupsStateHandle(
              groupRangeOffsets,
              std::move(streamStateHandle),
              StateHandleID(boost::uuids::to_string(boost::uuids::random_generator()())))
    {
    }

    KeyGroupsStateHandle(
        const KeyGroupRangeOffsets& groupRangeOffsets,
        std::shared_ptr<StreamStateHandle> streamStateHandle,
        const StateHandleID& stateHandleId)
        : keyGroupRangeOffsets_(groupRangeOffsets),
          stateHandle_(std::move(streamStateHandle)),
          stateHandleId_(stateHandleId)
    {
    }

    explicit KeyGroupsStateHandle(const nlohmann::json& description)
        : stateHandle_(ParseDelegateStateHandle(description)),
          keyGroupRangeOffsets_(ParseKeyGroupRangeOffsets(description)),
          stateHandleId_(ParseStateHandleId(description))
    {
    }

    ~KeyGroupsStateHandle() noexcept(true) override = default;

    std::shared_ptr<FSDataInputStream> OpenInputStream() const override
    {
        return stateHandle_->OpenInputStream();
    }

    std::shared_ptr<KeyedStateHandle> GetIntersection(const KeyGroupRange& keyGroupRange) const override
    {
        auto offsets = keyGroupRangeOffsets_.getIntersection(keyGroupRange);
        if (offsets.getKeyGroupRange().getNumberOfKeyGroups() <= 0) {
            return nullptr;
        }
        return std::make_shared<KeyGroupsStateHandle>(offsets, stateHandle_, stateHandleId_);
    }

    KeyGroupRange GetKeyGroupRange() const override
    {
        return keyGroupRangeOffsets_.getKeyGroupRange();
    }

    StateHandleID GetStateHandleId() const override
    {
        return stateHandleId_;
    }

    void RegisterSharedStates(SharedStateRegistry& stateRegistry, int64_t checkpointID) override
    {
        // No shared states
    }

    long GetStateSize() const override
    {
        return stateHandle_->GetStateSize();
    }

    long GetCheckpointedSize() override
    {
        return GetStateSize();
    }

    void DiscardState() override
    {
        stateHandle_->DiscardState();
    }

    static KeyGroupsStateHandle restore(
        const KeyGroupRangeOffsets& groupRangeOffsets,
        std::shared_ptr<StreamStateHandle> streamStateHandle,
        const StateHandleID& stateHandleId)
    {
        return KeyGroupsStateHandle(groupRangeOffsets, std::move(streamStateHandle), stateHandleId);
    }

    const KeyGroupRangeOffsets& getGroupRangeOffsets() const
    {
        return keyGroupRangeOffsets_;
    }

    std::shared_ptr<StreamStateHandle> getDelegateStateHandle() const
    {
        return stateHandle_;
    }

    int64_t getOffsetForKeyGroup(int keyGroupId) const
    {
        return keyGroupRangeOffsets_.getKeyGroupOffset(keyGroupId);
    }

    PhysicalStateHandleID GetStreamStateHandleID() const override
    {
        return stateHandle_->GetStreamStateHandleID();
    }

    bool operator==(const StreamStateHandle& other) const override
    {
        auto pOther = dynamic_cast<const KeyGroupsStateHandle*>(&other);
        if (!pOther) return false;
        return keyGroupRangeOffsets_ == pOther->keyGroupRangeOffsets_ &&
               stateHandle_->GetStreamStateHandleID() == pOther->stateHandle_->GetStreamStateHandleID() &&
               stateHandle_ == pOther->stateHandle_;
    }

    std::size_t hashCode()
    {
        std::size_t seed = 0x8A20E9D7;
        seed ^= (seed << 6) + (seed >> 2) + 0x198CE17B + keyGroupRangeOffsets_.hashCode();
        seed ^= (seed << 6) + (seed >> 2) + 0x2BC8E8D1 + stateHandle_->GetStreamStateHandleID().hashCode();
        seed ^= (seed << 6) + (seed >> 2) + 0x9B74C1E3 + stateHandleId_.hashCode();
        return seed;
    }

    std::string ToString() const override
    {
        // Java 侧 TaskStateSnapshotDeser.parseManagedKeyedStateArray 读取的字段：
        //   stateHandleName、groupRangeOffsets(含 keyGroupRange 和 offsets)、streamStateHandle。
        // 与 KeyGroupsSavepointStateHandle::ToString 的输出保持对称。
        nlohmann::json json;
        json["stateHandleName"] = "KeyGroupsStateHandle";
        json["stateHandleId"] = nlohmann::json::parse(stateHandleId_.ToString());
        json["groupRangeOffsets"] = nlohmann::json::parse(keyGroupRangeOffsets_.ToString());
        if (stateHandle_ != nullptr) {
            json["streamStateHandle"] = nlohmann::json::parse(stateHandle_->ToString());
        } else {
            json["streamStateHandle"] = nullptr;
        }
        return json.dump();
    }

    std::optional<std::vector<uint8_t>> AsBytesIfInMemory() const override
    {
        return stateHandle_->AsBytesIfInMemory();
    }

private:
    static KeyGroupRange ParseKeyGroupRangeJson(const nlohmann::json& rangeJson)
    {
        return KeyGroupRange(rangeJson.at("startKeyGroup").get<int>(), rangeJson.at("endKeyGroup").get<int>());
    }

    static std::vector<int64_t> ParseOffsets(const nlohmann::json& offsetsJson)
    {
        if (offsetsJson.is_array() && offsetsJson.size() == 2 && offsetsJson.at(0).is_string() &&
            offsetsJson.at(1).is_array()) {
            return offsetsJson.at(1).get<std::vector<int64_t>>();
        }
        return offsetsJson.get<std::vector<int64_t>>();
    }

    static KeyGroupRangeOffsets ParseKeyGroupRangeOffsets(const nlohmann::json& description)
    {
        const nlohmann::json& offsetsRoot =
            description.contains("groupRangeOffsets") ? description.at("groupRangeOffsets") : description;
        const nlohmann::json& rangeJson =
            offsetsRoot.contains("keyGroupRange") ? offsetsRoot.at("keyGroupRange") : description.at("keyGroupRange");
        KeyGroupRange keyGroupRange = ParseKeyGroupRangeJson(rangeJson);

        if (offsetsRoot.contains("offsets")) {
            return KeyGroupRangeOffsets(keyGroupRange, ParseOffsets(offsetsRoot.at("offsets")));
        }
        return KeyGroupRangeOffsets(
            keyGroupRange, std::vector<int64_t>(static_cast<size_t>(keyGroupRange.getNumberOfKeyGroups()), 0));
    }

    static std::shared_ptr<StreamStateHandle> ParseDelegateStateHandle(const nlohmann::json& description)
    {
        if (description.contains("stateHandle") && !description.at("stateHandle").is_null()) {
            return StreamStateHandleFactory::from_json(description.at("stateHandle"));
        }
        if (description.contains("streamStateHandle") && !description.at("streamStateHandle").is_null()) {
            return StreamStateHandleFactory::from_json(description.at("streamStateHandle"));
        }
        if (description.contains("metaDataState") && !description.at("metaDataState").is_null()) {
            return StreamStateHandleFactory::from_json(description.at("metaDataState"));
        }
        throw std::runtime_error("KeyGroupsStateHandle missing stateHandle/streamStateHandle/metaDataState.");
    }

    static StateHandleID ParseStateHandleId(const nlohmann::json& description)
    {
        if (description.contains("stateHandleId") && description.at("stateHandleId").is_object() &&
            description.at("stateHandleId").contains("keyString")) {
            return StateHandleID(description.at("stateHandleId").at("keyString").get<std::string>());
        }
        if (description.contains("stateHandleID") && description.at("stateHandleID").is_object() &&
            description.at("stateHandleID").contains("keyString")) {
            return StateHandleID(description.at("stateHandleID").at("keyString").get<std::string>());
        }
        return StateHandleID(boost::uuids::to_string(boost::uuids::random_generator()()));
    }

    std::shared_ptr<StreamStateHandle> stateHandle_;
    KeyGroupRangeOffsets keyGroupRangeOffsets_;
    StateHandleID stateHandleId_;
};

#endif // FLINK_TNEL_KEYGROUPSSTATEHANDLE_H
