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

#ifndef OMNISTREAM_ROCKSDBSTATEUPLOADER_H
#define OMNISTREAM_ROCKSDBSTATEUPLOADER_H

#include <vector>
#include <future>
#include <filesystem>
#include <jni.h>
#include "runtime/state/CheckpointStreamFactory.h"
#include "runtime/state/IncrementalKeyedStateHandle.h"
#include "core/fs/CloseableRegistry.h"
#include "state/filesystem/FileStateHandle.h"
#include "state/memory/ByteStreamStateHandle.h"
#include "state/filesystem/RelativeFileStateHandle.h"
#include "state/IncrementalRemoteKeyedStateHandle.h"
#include "core/fs/Path.h"
#include "state/bridge/TaskStateManagerBridge.h"
#include "state/bridge/OmniTaskBridge.h"

namespace fs = std::filesystem;
using HandleAndLocalPath = IncrementalRemoteKeyedStateHandle::HandleAndLocalPath;

enum class StreamStateHandleType {
    Unknown,
    ByteStreamStateHandle,
    RelativeFileStateHandle,
    FileStateHandle
};

class RocksDBStateUploader {
public:
    explicit RocksDBStateUploader(int numberOfSnapshottingThreads);

    std::vector<HandleAndLocalPath> callUploadFilesToCheckpointFs(
        JNIEnv* env,
        const std::vector<fs::path>& files,
        CheckpointedStateScope& stateScope,
        jobject jCheckpointStreamFactory);

    std::vector<HandleAndLocalPath> callUploadFilesToCheckpointFs(
        std::shared_ptr<omnistream::OmniTaskBridge> bridge, const std::vector<fs::path>& files);

private:
    static const int READ_BUFFER_SIZE = 16 * 1024;

    jobject addToJavaPathList(
        JNIEnv* env,
        const std::vector<fs::path>& files,
        jobject javaList,
        jmethodID arrayListAdd,
        jclass pathsClass,
        jmethodID pathsGet);

    jobject createJavaPathList(JNIEnv* env, const std::vector<fs::path>& files);

    int numberOfSnapshottingThreads_;
};

#endif // OMNISTREAM_ROCKSDBSTATEUPLOADER_H
