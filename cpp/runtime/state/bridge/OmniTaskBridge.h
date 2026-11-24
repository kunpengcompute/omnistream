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

#ifndef OMNITASKBRIDGE_H
#define OMNITASKBRIDGE_H
#include <util/omni_exception.h>
#include <utils/exception/Throwable.h>
#include <vector>
#include <cstdint>
#include <jni.h>
#include "state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/state/restore/KeyGroupEntry.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "core/fs/Path.h"
#include "state/LocalRecoveryConfig.h"

namespace omnistream {
class OmniTaskBridge {
public:
    virtual ~OmniTaskBridge() = default;

    virtual void declineCheckpoint(
        std::string& checkpointIDJson, std::string& failure_reasonJson, std::string& exceptionJson) = 0;

    virtual std::shared_ptr<SnapshotResult<StreamStateHandle>> CallMaterializeMetaData(
        jlong checkpointId,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer) = 0;

    virtual jobject CallUploadFilesToCheckpointFs(
        const std::vector<Path>& filePaths, int numberOfSnapshottingThreads) = 0;

    virtual std::vector<StateMetaInfoSnapshot> readMetaData(const std::string& metaStateHandle) = 0;

    virtual std::vector<StateMetaInfoSnapshot> readOperatorMetaData(const std::string& metaStateHandle) = 0;

    virtual jobject AcquireSavepointOutputStream(long checkpointId, CheckpointOptions* checkpointOptions) = 0;

    virtual std::shared_ptr<SnapshotResult<StreamStateHandle>> CloseSavepointOutputStream(jobject provider) = 0;

    // Consumes provider's JNI global reference; never finalizes a state handle.
    virtual void AbortSavepointOutputStream(jobject provider) = 0;

    virtual void WriteSavepointOutputStream(jobject provider, const int8_t* chunk, size_t offset, size_t len) = 0;

    virtual jobject CreateSavepointOutputDirectBuffer(void* data, size_t capacity) = 0;

    virtual void ReleaseSavepointOutputDirectBuffer(jobject directBuffer) = 0;

    virtual bool WriteSavepointOutputStreamDirect(jobject provider, jobject directBuffer, size_t len) = 0;

    virtual void WriteSavepointMetadata(
        jobject provider,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots,
        std::string keySerializer) = 0;

    virtual void WriteOperatorMetaData(
        jobject provider,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& operatorStateMetaInfoSnapshots,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& broadcastStateMetaInfoSnapshots) = 0;

    virtual long GetSavepointOutputStreamPos(jobject provider) = 0;

    virtual void getKeyGroupEntries(
        jobject inputStream,
        int& currentKvStateId,
        bool isUsingKeyGroupCompression,
        std::vector<KeyGroupEntry>& entries) = 0;

    virtual jobject getSavepointInputStream(const std::string& metaStateHandle) = 0;

    virtual void setSavepointInputStreamOffset(jobject inputStream, int64_t offset) = 0;

    virtual int ReadSavepointInputStream(jobject inputStream, int8_t* chunk, size_t offset, size_t len) = 0;

    virtual bool isUsingKeyGroupCompression(jobject inputStream) = 0;

    virtual void closeSavepointInputStream(jobject inputStream) = 0;

    virtual JNIEnv* getJNIEnv() = 0;

    virtual bool CallDownloadFileToLocal(
        const StreamStateHandle& cppHandle, const std::string& restoreInstancePath) = 0;
};
} // namespace omnistream

#endif // OMNITASKBRIDGE_H
