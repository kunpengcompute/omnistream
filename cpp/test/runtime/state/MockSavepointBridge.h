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

#ifndef OMNISTREAM_TEST_MOCKSAVEPOINTBRIDGE_H
#define OMNISTREAM_TEST_MOCKSAVEPOINTBRIDGE_H

#include <gmock/gmock.h>
#include <jni.h>
#include <memory>
#include "state/bridge/OmniTaskBridge.h"

/** JNI jobject 在 C++ 中为不透明指针，测试中使用固定的 sentinel 值。 */
static jobject kMockProvider = reinterpret_cast<jobject>(0x10001);
static jobject kMockDirectBuffer = reinterpret_cast<jobject>(0x20001);

/** Mock OmniTaskBridge：只实现 savepoint 输出路径，其余方法 gmock stub。 */
class MockSavepointBridge : public omnistream::OmniTaskBridge {
public:
    MOCK_METHOD(void, declineCheckpoint, (std::string&, std::string&, std::string&), (override));
    MOCK_METHOD(
        std::shared_ptr<SnapshotResult<StreamStateHandle>>,
        CallMaterializeMetaData,
        (jlong,
         std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&,
         std::shared_ptr<LocalRecoveryConfig>,
         CheckpointOptions*,
         std::string),
        (override));
    MOCK_METHOD(jobject, CallUploadFilesToCheckpointFs, (const std::vector<Path>&, int), (override));
    MOCK_METHOD(std::vector<StateMetaInfoSnapshot>, readMetaData, (const std::string&), (override));
    MOCK_METHOD(std::vector<StateMetaInfoSnapshot>, readOperatorMetaData, (const std::string&), (override));
    MOCK_METHOD(jobject, AcquireSavepointOutputStream, (long, CheckpointOptions*), (override));
    MOCK_METHOD(std::shared_ptr<SnapshotResult<StreamStateHandle>>, CloseSavepointOutputStream, (jobject), (override));
    MOCK_METHOD(void, AbortSavepointOutputStream, (jobject), (override));
    MOCK_METHOD(void, WriteSavepointOutputStream, (jobject, const int8_t*, size_t, size_t), (override));
    MOCK_METHOD(jobject, CreateSavepointOutputDirectBuffer, (void*, size_t), (override));
    MOCK_METHOD(void, ReleaseSavepointOutputDirectBuffer, (jobject), (override));
    MOCK_METHOD(bool, WriteSavepointOutputStreamDirect, (jobject, jobject, size_t), (override));
    MOCK_METHOD(
        void,
        WriteSavepointMetadata,
        (jobject, const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&, std::string),
        (override));
    MOCK_METHOD(
        void,
        WriteOperatorMetaData,
        (jobject,
         const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&,
         const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&),
        (override));
    MOCK_METHOD(long, GetSavepointOutputStreamPos, (jobject), (override));
    MOCK_METHOD(void, getKeyGroupEntries, (jobject, int&, bool, std::vector<KeyGroupEntry>&), (override));
    MOCK_METHOD(jobject, getSavepointInputStream, (const std::string&), (override));
    MOCK_METHOD(void, setSavepointInputStreamOffset, (jobject, int64_t), (override));
    MOCK_METHOD(int, ReadSavepointInputStream, (jobject, int8_t*, size_t, size_t), (override));
    MOCK_METHOD(bool, isUsingKeyGroupCompression, (jobject), (override));
    MOCK_METHOD(void, closeSavepointInputStream, (jobject), (override));
    MOCK_METHOD(JNIEnv*, getJNIEnv, (), (override));
    MOCK_METHOD(bool, CallDownloadFileToLocal, (const StreamStateHandle&, const std::string&), (override));
};

#endif
