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

#ifndef OMNISTREAM_ROCKSDBSTATEDOWNLOADER_H
#define OMNISTREAM_ROCKSDBSTATEDOWNLOADER_H

#include <filesystem>
#include <future>
#include <vector>

#include <jni.h>

#include "core/fs/CloseableRegistry.h"

#include "runtime/state/CheckpointStreamFactory.h"
#include "runtime/state/IncrementalKeyedStateHandle.h"

#include "RocksDBStateDataTransfer.h"
#include "state/IncrementalRemoteKeyedStateHandle.h"

#include "runtime/state/filesystem/FileStateHandle.h"
#include "runtime/state/filesystem/RelativeFileStateHandle.h"
#include "runtime/state/memory/ByteStreamStateHandle.h"
#include "runtime/state/bridge/OmniTaskBridge.h"

namespace fs = std::filesystem;
using HandleAndLocalPath = IncrementalRemoteKeyedStateHandle::HandleAndLocalPath;
class RocksDBStateDownloader : public RocksDBStateDataTransfer {
public:
    explicit RocksDBStateDownloader(int restoringThreadNum);

    void transferAllStateDataToDirectory(
        const IncrementalRemoteKeyedStateHandle& restoreStateHandle,
        const fs::path& dest,
        std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge)
    {
        std::vector<HandleAndLocalPath> sstFiles = restoreStateHandle.GetSharedState();
        std::vector<HandleAndLocalPath> miscFiles = restoreStateHandle.GetPrivateState();
        callDownloadDataForAllStateHandles(sstFiles, dest, omniTaskBridge);
        callDownloadDataForAllStateHandles(miscFiles, dest, omniTaskBridge);
    }

    void callDownloadDataForAllStateHandles(
        const std::vector<HandleAndLocalPath>& handleWithPaths,
        const fs::path& restoreInstancePath,
        std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge);

private:
    int restoringThreadNum_;
};
#endif // OMNISTREAM_ROCKSDBSTATEDOWNLOADER_H
