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

#ifndef FLINK_TNEL_INCREMENTALKEYEDSTATEHANDLE_H
#define FLINK_TNEL_INCREMENTALKEYEDSTATEHANDLE_H

#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/StreamStateHandleFactory.h"
#include "runtime/state/CheckpointBoundKeyedStateHandle.h"
#include "runtime/state/UUID.h"
#include "runtime/state/StreamStateHandle.h"
#include <map>

class IncrementalKeyedStateHandle : virtual public CheckpointBoundKeyedStateHandle {
public:
    virtual ~IncrementalKeyedStateHandle() = default;

    /**
     * Returns the identifier of the state backend from which this handle was created.
     */
    virtual const UUID& GetBackendIdentifier() const = 0;

    /** A Holder of StreamStateHandle and the corresponding localPath. */
    class HandleAndLocalPath {
    public:
        static HandleAndLocalPath of(std::shared_ptr<StreamStateHandle> handle, const std::string& localPath)
        {
            if (!handle) {
                throw std::invalid_argument("streamStateHandle cannot be null");
            }
            if (localPath.empty()) {
                throw std::invalid_argument("localPath cannot be empty");
            }
            return HandleAndLocalPath(handle, localPath);
        }

        std::shared_ptr<StreamStateHandle> getHandle() const
        {
            return handle_;
        }

        const std::string& getLocalPath() const
        {
            return localPath_;
        }

        long GetStateSize() const
        {
            return handle_ ? handle_->GetStateSize() : 0;
        }

        // Replace handle
        void ReplaceHandle(std::shared_ptr<StreamStateHandle> registryReturned)
        {
            if (!registryReturned) {
                throw std::invalid_argument("registryReturned cannot be null");
            }
            handle_ = registryReturned;
        }

        bool operator==(const HandleAndLocalPath& other) const
        {
            if (this == &other) {
                return true;
            }
            bool handleEq = (handle_ && other.handle_) ? (*handle_ == *other.handle_) : (handle_ == other.handle_);
            return handleEq && localPath_ == other.localPath_;
        }

        bool operator!=(const HandleAndLocalPath& other) const
        {
            return !(*this == other);
        }

        // Hash function
        std::size_t hashCode() const
        {
            std::size_t seed = 0x811C9DC5;
            seed ^= std::hash<std::string>()(localPath_) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            return seed;
        }

        std::string ToString() const
        {
            nlohmann::json json;
            if (handle_) {
                json["handle"] = nlohmann::json::parse(handle_->ToString());
            } else {
                json["handle"] = nullptr;
            }
            json["localPath"] = localPath_;
            json["stateSize"] = GetStateSize();
            return json.dump();
        }

        static HandleAndLocalPath from_json(const nlohmann::json& json)
        {
            std::shared_ptr<StreamStateHandle> handle;
            if (!json["handle"].is_null()) {
                handle = StreamStateHandleFactory::from_json(json["handle"]);
            }
            std::string localPath = json["localPath"].get<std::string>();
            return HandleAndLocalPath(handle, localPath);
        }

    private:
        std::shared_ptr<StreamStateHandle> handle_;
        std::string localPath_;

        // Constructors
        HandleAndLocalPath(std::shared_ptr<StreamStateHandle> handle, std::string localPath)
            : handle_(std::move(handle)),
              localPath_(std::move(localPath))
        {
        }
    };

    /**
     * Returns a list of all shared states and the corresponding localPath in the backend at the
     * time this was created.
     */
    virtual const std::vector<HandleAndLocalPath>& GetSharedStateHandles() const = 0;
};

#endif // FLINK_TNEL_INCREMENTALLOCALKEYEDSTATEHANDLE_H
