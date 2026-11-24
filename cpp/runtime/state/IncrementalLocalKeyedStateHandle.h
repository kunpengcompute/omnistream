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

#ifndef FLINK_TNEL_INCREMENTALLOCALKEYEDSTATEHANDLE_H
#define FLINK_TNEL_INCREMENTALLOCALKEYEDSTATEHANDLE_H

#include "runtime/state/StateObject.h"
#include "runtime/state/filesystem/FileStateHandle.h"
#include "runtime/state/DirectoryStateHandle.h"
#include "runtime/state/DirectoryKeyedStateHandle.h"
#include "runtime/state/IncrementalKeyedStateHandle.h"
#include "runtime/state/UUID.h"
#include <map>
#include <nlohmann/json.hpp>
#include "runtime/state/StreamStateHandleFactory.h"
using json = nlohmann::json;

class IncrementalLocalKeyedStateHandle : virtual public DirectoryKeyedStateHandle,
                                         virtual public IncrementalKeyedStateHandle {
public:
    IncrementalLocalKeyedStateHandle(
        const UUID& backendIdentifier,
        int64_t checkpointId,
        DirectoryStateHandle* directoryStateHandle,
        const KeyGroupRange& keyGroupRange,
        std::shared_ptr<StreamStateHandle> metaDataState,
        const std::vector<HandleAndLocalPath>& sharedState)
        : DirectoryKeyedStateHandle(directoryStateHandle, keyGroupRange),
          checkpointId_(checkpointId),
          backendIdentifier_(backendIdentifier),
          metaDataState_(std::move(metaDataState)),
          sharedState_(sharedState)
    {
    }

    explicit IncrementalLocalKeyedStateHandle(const nlohmann::json& json)
        : DirectoryKeyedStateHandle(json),
          checkpointId_(0),
          backendIdentifier_(),
          metaDataState_(nullptr),
          sharedState_()
    {
        if (!json.contains("checkpointId")) {
            throw std::invalid_argument("'checkpointId' field missing");
        }
        checkpointId_ = json["checkpointId"].get<int64_t>();

        if (!json.contains("backendIdentifier")) {
            throw std::invalid_argument("'backendIdentifier' field missing");
        }
        backendIdentifier_ = UUID::FromString(json["backendIdentifier"].get<std::string>());

        if (json.contains("metaDataState")) {
            metaDataState_ = StreamStateHandleFactory::from_json(json["metaDataState"]);
        }

        if (json.contains("sharedState")) {
            for (const auto& item : json["sharedState"]) {
                if (item.contains("handle")) {
                    sharedState_.push_back(HandleAndLocalPath::from_json(item));
                }
            }
        }
    }

    const std::shared_ptr<StreamStateHandle>& GetMetaDataState() const
    {
        return metaDataState_;
    }

    int64_t GetCheckpointId() const override
    {
        return checkpointId_;
    }

    const UUID& GetBackendIdentifier() const
    {
        return backendIdentifier_;
    }

    const std::vector<HandleAndLocalPath>& GetSharedStateHandles() const override
    {
        return sharedState_;
    }

    std::shared_ptr<CheckpointBoundKeyedStateHandle> rebound(int64_t checkpointId) const override
    {
        return std::make_shared<IncrementalLocalKeyedStateHandle>(
            backendIdentifier_,
            checkpointId,
            getDirectoryStateHandle(),
            GetKeyGroupRange(),
            GetMetaDataState(),
            GetSharedStateHandles());
    }

    void DiscardState() override
    {
        std::exception_ptr collectedEx = nullptr;
        try {
            DirectoryKeyedStateHandle::DiscardState();
        } catch (...) {
            collectedEx = std::current_exception();
        }

        try {
            if (metaDataState_) {
                metaDataState_->DiscardState();
            }
        } catch (...) {
            if (!collectedEx) {
                collectedEx = std::current_exception();
            }
        }
        if (collectedEx) std::rethrow_exception(collectedEx);
    }

    KeyGroupRange GetKeyGroupRange() const override
    {
        return DirectoryKeyedStateHandle::GetKeyGroupRange();
    }

    std::shared_ptr<KeyedStateHandle> GetIntersection(const KeyGroupRange& other) const override
    {
        return DirectoryKeyedStateHandle::GetIntersection(other);
    }

    StateHandleID GetStateHandleId() const override
    {
        return DirectoryKeyedStateHandle::GetStateHandleId();
    }

    long GetCheckpointedSize() override
    {
        return DirectoryKeyedStateHandle::GetCheckpointedSize();
    }

    long GetStateSize() const override
    {
        return DirectoryKeyedStateHandle::GetStateSize() + (metaDataState_ ? metaDataState_->GetStateSize() : 0);
    }

    void RegisterSharedStates(SharedStateRegistry& stateRegistry, long checkpointID) override
    {
        // Nothing to do, this is for local use only.
    }

    bool operator==(const IncrementalLocalKeyedStateHandle& other) const
    {
        if (this == &other) return true;
        if (!DirectoryKeyedStateHandle::operator==(other)) return false;
        if (metaDataState_ && other.metaDataState_)
            return *metaDataState_ == *other.metaDataState_;
        else
            return metaDataState_ == other.metaDataState_;
    }

    bool operator!=(const IncrementalLocalKeyedStateHandle& other) const
    {
        return !(*this == other);
    }

    std::string ToString() const override
    {
        nlohmann::json json;
        json["directoryStateHandle"] = nlohmann::json::parse(this->GetDirectoryStateHandle()->ToString());
        json["keyGroupRange"] = nlohmann::json::parse(this->GetKeyGroupRange().ToString());
        json["backendIdentifier"] = backendIdentifier_.ToString();
        json["checkpointId"] = checkpointId_;
        json["metaDataState"] = metaDataState_ ? nlohmann::json::parse(metaDataState_->ToString()) : nullptr;
        json["sharedState"] = nlohmann::json::array();
        for (const auto& s : sharedState_) {
            json["sharedState"].push_back(nlohmann::json::parse(s.ToString()));
        }
        json["stateHandleName"] = "IncrementalLocalKeyedStateHandle";
        json["stateSize"] = GetStateSize();
        json["checkpointedSize"] = DirectoryKeyedStateHandle::GetStateSize();
        return json.dump();
    }

    std::size_t hashCode() const
    {
        std::size_t seed = 0x9e3779b9;

        seed ^= std::hash<int64_t>()(checkpointId_) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= backendIdentifier_.hashCode() + 0x9e3779b9 + (seed << 6) + (seed >> 2);

        return seed;
    }

private:
    int64_t checkpointId_;
    UUID backendIdentifier_;
    std::shared_ptr<StreamStateHandle> metaDataState_;
    std::vector<HandleAndLocalPath> sharedState_;
};

#endif // FLINK_TNEL_INCREMENTALLOCALKEYEDSTATEHANDLE_H
