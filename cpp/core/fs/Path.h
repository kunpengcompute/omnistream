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

#ifndef OMNISTREAM_PATH_H
#define OMNISTREAM_PATH_H

#include <string>
#include <filesystem>
#include "core/include/common.h"

class Path {
    std::string pathStr;

public:
    Path(const std::string& path) : pathStr(path)
    {
    }
    Path(const Path& other) : pathStr(other.pathStr)
    {
    }
    Path(const Path& other, std::string child) : pathStr(other.toString() + child)
    {
    }
    Path(const Path& parent, const Path& child)
    {
        std::string parentPath = parent.pathStr;
        std::string childPath = child.pathStr;

        // Ensure parent path ends with a slash unless it's "/" or empty
        if (!(parentPath == "/" || parentPath.empty())) {
            if (parentPath.back() != '/') parentPath += '/';
        }

        // If child path starts with '/', remove it
        if (!childPath.empty() && childPath.front() == '/') childPath = childPath.substr(1);

        // Concatenate
        pathStr = parentPath + childPath;
    }

    operator std::string() const
    {
        return pathStr;
    }

    const std::string& toString() const
    {
        return pathStr;
    }

    int getFileSystem()
    {
        return 0; // To be implemented
    }

    std::filesystem::path toFilesystemPath() const
    {
        return std::filesystem::path(pathStr);
    }

    bool operator==(const Path& other) const
    {
        return pathStr == other.pathStr;
    }
};

#endif // OMNISTREAM_PATH_H
