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

#ifndef OMNISTREAM_FILESTATEHANDLE_H
#define OMNISTREAM_FILESTATEHANDLE_H

#include "core/fs/FSDataInputStream.h"
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/PhysicalStateHandleID.h"
#include "core/fs/Path.h"

#include <fstream>
#include <filesystem>
#include <nlohmann/json.hpp>

class FileStateHandle : public StreamStateHandle {
public:
    FileStateHandle(const Path& filePath, long stateSize) : filePath_(filePath), stateSize_(stateSize)
    {
        if (stateSize < -1) {
            throw std::invalid_argument("stateSize must be >= -1");
        }
    }

    FileStateHandle(const nlohmann::json& json)
        : filePath_(
              json.contains("filePath") ? Path(json["filePath"].get<std::string>())
                                        : throw std::invalid_argument("FileStateHandle 'filePath' field missing")),
          stateSize_(
              json.contains("stateSize") ? json["stateSize"].get<long>()
                                         : throw std::invalid_argument("FileStateHandle 'stateSize' field missing"))
    {
        if (stateSize_ < -1) {
            throw std::invalid_argument("stateSize must be >= -1");
        }
    }

    const Path& GetFilePath() const
    {
        return filePath_;
    }

    std::shared_ptr<FSDataInputStream> OpenInputStream() const override
    {
        return nullptr;
    }

    std::optional<std::vector<uint8_t>> AsBytesIfInMemory() const override
    {
        return std::nullopt;
    }

    PhysicalStateHandleID GetStreamStateHandleID() const override
    {
        return PhysicalStateHandleID(filePath_.toString());
    }

    void DiscardState() override
    {
        auto fsPath = filePath_.toFilesystemPath();
        bool removed = std::filesystem::remove(fsPath);
        if (!removed && std::filesystem::exists(fsPath)) {
            throw std::runtime_error("Failed to delete file " + filePath_.toString());
        }

        auto parent = fsPath.parent_path();
        if (!parent.empty() && parent != fsPath) {
            try {
                if (std::filesystem::is_empty(parent)) {
                    std::filesystem::remove(parent);
                }
            } catch (...) {
                //
            }
        }
    }

    long GetStateSize() const override
    {
        return stateSize_;
    }

    std::string ToString() const override
    {
        nlohmann::json j;
        j["@class"] = "org.apache.flink.runtime.state.filesystem.FileStateHandle";
        j["filePath"] = filePath_.toString();
        j["stateSize"] = stateSize_;
        j["relativePath"] = filePath_.toString();
        j["streamStateHandleID"] = nlohmann::json::parse(GetStreamStateHandleID().ToString());
        return j.dump();
    }

    bool operator==(const StreamStateHandle& other) const override
    {
        auto* o = dynamic_cast<const FileStateHandle*>(&other);
        if (!o) return false;
        return filePath_ == o->filePath_ && stateSize_ == o->stateSize_;
    }

private:
    Path filePath_;
    long stateSize_;
    std::string relativePath;
};

#endif // OMNISTREAM_FILESTATEHANDLE_H
