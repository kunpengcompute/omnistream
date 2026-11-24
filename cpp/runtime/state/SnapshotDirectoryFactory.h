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

#ifndef OMNISTREAM_SNAPSHOTDIRECTORYFACTORY_H
#define OMNISTREAM_SNAPSHOTDIRECTORYFACTORY_H

#include <filesystem>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <vector>
#include "SnapshotDirectory.h"

class SnapshotDirectoryFactory {
public:
    static std::unique_ptr<SnapshotDirectory> createPermanentSnapshotDirectory(const fs::path& directory)
    {
        return std::make_unique<PermanentSnapshotDirectory>(directory);
    }

    static std::unique_ptr<SnapshotDirectory> createTemporarySnapshotDirectory(const fs::path& directory)
    {
        return std::make_unique<TemporarySnapshotDirectory>(directory);
    }

    // 创建临时快照目录
    static std::unique_ptr<SnapshotDirectory> temporary(const fs::path& directory)
    {
        return SnapshotDirectoryFactory::createTemporarySnapshotDirectory(directory);
    }

    // 创建永久快照目录
    static std::unique_ptr<SnapshotDirectory> permanent(const fs::path& directory)
    {
        return SnapshotDirectoryFactory::createPermanentSnapshotDirectory(directory);
    }
};

#endif // OMNISTREAM_SNAPSHOTDIRECTORYFACTORY_H
