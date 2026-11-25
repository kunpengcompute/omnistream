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

#ifndef OMNISTREAM_DIRECTORYSTATEHANDLE_H
#define OMNISTREAM_DIRECTORYSTATEHANDLE_H

#include "StateObject.h"
#include "core/fs/Path.h"
#include <string>
#include <filesystem>

class DirectoryStateHandle : public StateObject {
public:
    DirectoryStateHandle(const Path& directory, long directorySize)
        : directoryString_(directory.toString()),
          directorySize_(directorySize),
          directory_(directory)
    {
    }

    DirectoryStateHandle(const nlohmann::json& json)
        : directoryString_(
              json.contains("directoryString")
                  ? json["directoryString"].get<std::string>()
                  : throw std::invalid_argument("DirectoryStateHandle 'directoryString' field missing")),
          directorySize_(
              json.contains("stateSize")
                  ? json["stateSize"].get<long>()
                  : throw std::invalid_argument("DirectoryStateHandle 'directorySize' field missing")),
          directory_(Path(directoryString_))
    {
    }

    static DirectoryStateHandle forPathWithSize(const Path& directory)
    {
        long size = 0;
        try {
            size = computeDirectorySize(directory.toString());
        } catch (...) {
            //
        }
        return DirectoryStateHandle(directory, size);
    }

    static std::unique_ptr<DirectoryStateHandle> forPathWithSizePtr(const Path& directory)
    {
        long size = 0;
        try {
            size = computeDirectorySize(directory.toString());
        } catch (...) {
            //
        }
        return std::make_unique<DirectoryStateHandle>(directory, size);
    }

    void DiscardState() override
    {
        std::error_code ec;
        std::filesystem::remove_all(directoryString_, ec);
        if (ec) {
            throw std::runtime_error("Failed to delete directory " + directoryString_ + ": " + ec.message());
        }
    }

    long GetStateSize() const override
    {
        return directorySize_;
    }

    const Path& getDirectory()
    {
        return directory_;
    }

    bool operator==(const DirectoryStateHandle& other) const
    {
        return directoryString_ == other.directoryString_;
    }

    std::string ToString() const override
    {
        nlohmann::json json;
        json["stateHandleName"] = "DirectoryStateHandle";
        json["directory"] = directoryString_;
        json["stateSize"] = directorySize_;
        return json.dump();
    }

private:
    static long computeDirectorySize(const std::filesystem::path& dir)
    {
        std::error_code ec;
        uintmax_t total = 0;

        for (auto it = std::filesystem::recursive_directory_iterator(dir, ec);
             it != std::filesystem::recursive_directory_iterator();
             it.increment(ec)) {
            if (ec) {
                continue;
            }

            if (std::filesystem::is_regular_file(it->path(), ec) && !ec) {
                total += std::filesystem::file_size(it->path(), ec);
            }
        }
        return static_cast<long>(total);
    }
    std::string directoryString_;
    long directorySize_;
    Path directory_;
};

#endif // OMNISTREAM_DIRECTORYSTATEHANDLE_H
