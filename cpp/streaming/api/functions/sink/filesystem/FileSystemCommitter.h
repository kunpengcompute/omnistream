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

#ifndef FILE_SYSTEM_COMMITTER_H
#define FILE_SYSTEM_COMMITTER_H

#include <string>
#include <filesystem>
#include <fstream>
#include <vector>
#include <sys/stat.h>

#include "core/include/common.h"

namespace fs = std::filesystem;

class FileSystemCommitter {
public:
    static void createSuccessFile(const std::string& partitionPath)
    {
        fs::path successPath = fs::path(partitionPath) / "_SUCCESS";
        if (!fs::exists(successPath)) {
            std::ofstream out(successPath, std::ios::out);
            if (out.is_open()) {
                out.close();
                LOG("Created success file at: " + successPath.string());
            } else {
                LOG("Failed to create success file at: " + successPath.string());
            }
        }
    }

    static bool hasSuccessFile(const std::string& partitionPath)
    {
        return fs::exists(fs::path(partitionPath) / "_SUCCESS");
    }

    static void commitPartition(const std::string& partitionPath, const std::string& pendingPath)
    {
        if (!fs::exists(partitionPath)) {
            fs::create_directories(partitionPath);
        }

        if (fs::exists(pendingPath) && pendingPath != partitionPath) {
            for (const auto& entry : fs::directory_iterator(pendingPath)) {
                auto srcPath = entry.path();
                auto dstPath = fs::path(partitionPath) / srcPath.filename();
                fs::rename(srcPath, dstPath);
            }
            if (fs::is_empty(pendingPath)) {
                fs::remove(pendingPath);
            }
        }

        createSuccessFile(partitionPath);
    }

    static void movePendingFiles(const std::string& srcDir, const std::string& dstDir)
    {
        if (!fs::exists(srcDir)) {
            return;
        }

        if (!fs::exists(dstDir)) {
            fs::create_directories(dstDir);
        }

        for (const auto& entry : fs::directory_iterator(srcDir)) {
            if (entry.is_regular_file()) {
                auto srcPath = entry.path();
                auto dstPath = fs::path(dstDir) / srcPath.filename();
                fs::rename(srcPath, dstPath);
            }
        }
    }

    static std::vector<std::string> listPendingPartitions(
        const std::string& basePath, const std::vector<std::string>& partitionKeys)
    {
        std::vector<std::string> partitions;
        if (!fs::exists(basePath)) {
            return partitions;
        }

        for (const auto& entry : fs::directory_iterator(basePath)) {
            if (entry.is_directory() && entry.path().filename() != "_SUCCESS") {
                auto dirName = entry.path().filename().string();
                if (dirName.find('=') != std::string::npos) {
                    std::string partitionPath = entry.path().string();
                    if (!hasSuccessFile(partitionPath)) {
                        partitions.push_back(partitionPath);
                    }
                }
            }
        }
        return partitions;
    }

    static int64_t getFileModificationTime(const std::string& path)
    {
        struct stat attr;
        if (stat(path.c_str(), &attr) == 0) {
            return static_cast<int64_t>(attr.st_mtime) * 1000;
        }
        return 0;
    }
};

#endif // FILE_SYSTEM_COMMITTER_H
