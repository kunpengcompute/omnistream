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

#ifndef OMNITASKBRIDGEIMPL2_H
#define OMNITASKBRIDGEIMPL2_H

#include <common.h>
#include <memory>
#include <jni.h>
#include <state/bridge/OmniTaskBridge.h>
#include <state/metainfo/StateMetaInfoSnapshot.h>
#include <common/global.h>
#include "core/fs/Path.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/restore/KeyGroupEntry.h"
#include "state/LocalRecoveryConfig.h"
#include "runtime/checkpoint/CheckpointOptions.h"

class OmniTaskBridgeImpl2 : public omnistream::OmniTaskBridge {
public:
    explicit OmniTaskBridgeImpl2(jobject mGlobalOmniTaskRef) : m_globalOmniTaskRef(mGlobalOmniTaskRef)
    {
    }

    virtual ~OmniTaskBridgeImpl2();

    void declineCheckpoint(
        std::string& checkpointIDJson, std::string& failure_reasonJson, std::string& exceptionJson) override;

    std::vector<StateMetaInfoSnapshot> readMetaData(const std::string& metaStateHandle) override;

    std::vector<StateMetaInfoSnapshot> readOperatorMetaData(const std::string& metaStateHandle) override;

    void getKeyGroupEntries(
        jobject inputStream,
        int& currentKvStateId,
        bool isUsingKeyGroupCompression,
        std::vector<KeyGroupEntry>& entries) override;

    jobject getSavepointInputStream(const std::string& metaStateHandle) override;

    void setSavepointInputStreamOffset(jobject inputStream, int64_t offset) override;

    int ReadSavepointInputStream(jobject inputStream, int8_t* chunk, size_t offset, size_t len) override;

    bool isUsingKeyGroupCompression(jobject inputStream) override;

    void closeSavepointInputStream(jobject inputStream) override;

    std::shared_ptr<SnapshotResult<StreamStateHandle>> CallMaterializeMetaData(
        jlong checkpointId,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer) override;

    jobject CallUploadFilesToCheckpointFs(const std::vector<Path>& filePaths, int numberOfSnapshottingThreads) override;

    JNIEnv* getJNIEnv() override;

    jobject AcquireSavepointOutputStream(long checkpointId, CheckpointOptions* checkpointOptions) override;
    std::shared_ptr<SnapshotResult<StreamStateHandle>> CloseSavepointOutputStream(jobject provider) override;
    void AbortSavepointOutputStream(jobject provider) override;
    void WriteSavepointOutputStream(jobject provider, const int8_t* chunk, size_t offset, size_t len) override;
    jobject CreateSavepointOutputDirectBuffer(void* data, size_t capacity) override;
    void ReleaseSavepointOutputDirectBuffer(jobject directBuffer) override;
    bool WriteSavepointOutputStreamDirect(jobject provider, jobject directBuffer, size_t len) override;
    void WriteSavepointMetadata(
        jobject provider,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots,
        std::string keySerializer) override;

    void WriteOperatorMetaData(
        jobject provider,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& operatorStateMetaInfoSnapshots,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& broadcastStateMetaInfoSnapshots) override;

    long GetSavepointOutputStreamPos(jobject provider) override;

    /**
     * @brief Download a remote file to the local file system.
     *
     * @param cppHandle The handle of the remote file (StreamStateHandle object).
     * @param restoreInstancePath The destination local file path where the downloaded file will be stored.
     * @return true if the file is successfully downloaded and saved; false otherwise.
     */
    bool CallDownloadFileToLocal(const StreamStateHandle& cppHandle, const std::string& restoreInstancePath) override;

public:
    jobject m_globalOmniTaskRef;
};
#endif // OMNITASKBRIDGEIMPL2_H
