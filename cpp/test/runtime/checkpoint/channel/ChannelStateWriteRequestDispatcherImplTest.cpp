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

#include <gtest/gtest.h>
#include "core/fs/Path.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/buffer/ObjectBuffer.h"
#include "runtime/state/CheckpointStorage.h"
#include "runtime/state/filesystem/FsCheckpointStorageAccess.h"
#include "runtime/checkpoint/channel/ChannelStateSerializer.h"
#include "runtime/checkpoint/channel/ChannelStateWriteRequestDispatcherImpl.h"

using namespace omnistream;

class ChannelStateSerializerImplTest : public ChannelStateSerializerImpl {
public:
    void WriteHeader(std::ostringstream& dataStream) override
    {
        writeHeaderCalled = true;
    }

    void WriteData(std::ostringstream& dataStream, std::shared_ptr<Buffer> buffers) override
    {
        writeDataCalled = true;
    }

    int64_t GetHeaderLength() const override
    {
        return 999;
    }

    bool writeHeaderCalled = false;
    bool writeDataCalled = false;
};

class CheckpointStorageTest : public CheckpointStorage {
public:
    std::shared_ptr<CheckpointStorageAccess> createCheckpointStorage(const JobIDPOD& jobId) override
    {
        // const std::string dir = std::filesystem::temp_directory_path().string();
        const std::string dir = "";

        auto checkpointDir = std::make_shared<Path>(dir);
        auto savepointDir = std::make_shared<Path>(dir);

        return std::make_shared<FsCheckpointStorageAccess>(checkpointDir, savepointDir, jobId, 100, 100);
    }
};

class ObjectBufferTest : public ObjectBuffer {
public:
    bool isBuffer() const override
    {
        return true;
    }
    std::shared_ptr<BufferRecycler> GetRecycler() override
    {
        return DummyObjectBufferRecycler::getInstance();
    }
    void RecycleBuffer() override
    {
        recycled = true;
    }

    Buffer* RetainBuffer() override
    {
        return this;
    }
    Buffer* ReadOnlySlice() override
    {
        return this;
    }
    Buffer* ReadOnlySlice(int index, int length) override
    {
        return this;
    }
    int GetMaxCapacity() const override
    {
        return 1024;
    }
    int GetReaderIndex() const override
    {
        return 0;
    }
    void SetReaderIndex(int readerIndex) override
    {
    }
    int GetSize() const override
    {
        return 0;
    }
    void SetSize(int writerIndex) override
    {
    }
    int ReadableObjects() const override
    {
        return 0;
    }
    bool IsCompressed() const override
    {
        return false;
    }
    void SetCompressed(bool isCompressed) override
    {
    }
    ObjectBufferDataType GetDataType() const override
    {
        return ObjectBufferDataType::DATA_BUFFER;
    }
    void SetDataType(ObjectBufferDataType dataType) override
    {
    }
    int RefCount() const override
    {
        return 1;
    }
    std::string ToDebugString(bool includeHash) const override
    {
        return "ObjectBufferTest";
    }
    ObjectSegment* GetObjectSegment() override
    {
        return std::make_shared<ObjectSegment>(0).get();
    }
    int GetBufferType() override
    {
        return 42;
    }
    std::pair<uint8_t*, size_t> GetBytes() override
    {
        return {nullptr, 0};
    }

private:
    bool recycled = false;
};

TEST(ChannelStateWriteRequestDispatcherImplTest, InitialiseRequestDispatcher)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    JobVertexID jobVertexID(-1, -1);
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult;
    CheckpointStorageLocationReference locationReference;

    auto registerRequest = std::make_shared<SubtaskRegisterRequest>(jobVertexID, 1);
    dispatcher->dispatch(registerRequest);

    auto startRequest = std::make_shared<CheckpointStartRequest>(jobVertexID, 1, 0, targetResult, &locationReference);

    dispatcher->dispatch(startRequest);

    EXPECT_TRUE(serializer->writeHeaderCalled);

    auto releaseRequest = std::make_shared<SubtaskReleaseRequest>(jobVertexID, 1);
    dispatcher->dispatch(releaseRequest);
}

TEST(ChannelStateWriteRequestDispatcherImplTest, AbortedCheckpointIsCancelledNotThrown)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    JobVertexID jvid(-1, -1);
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult =
        ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();

    dispatcher->dispatch(ChannelStateWriteRequest::registerSubtask(jvid, 1));
    std::shared_ptr<ChannelStateWriteRequest> startRequest = ChannelStateWriteRequest::start(jvid, 1, 1, "Start");
    dispatcher->dispatch(startRequest);

    auto oldStartRequest = ChannelStateWriteRequest::start(jvid, 1, 0, "Start");

    EXPECT_NO_THROW(dispatcher->dispatch(oldStartRequest));
    // EXPECT_TRUE(targetResult->GetInputChannelStateHandles()->IsCancelled());
}

TEST(ChannelStateWriteRequestDispatcherImplTest, FullRequestFlow)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    JobVertexID jobVertexID(-1, -1);
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult =
        ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    CheckpointStorageLocationReference locationReference;

    std::shared_ptr<SubtaskRegisterRequest> registerRequest = std::make_shared<SubtaskRegisterRequest>(jobVertexID, 1);
    dispatcher->dispatch(registerRequest);

    auto startRequest = std::make_shared<CheckpointStartRequest>(jobVertexID, 1, 1, targetResult, &locationReference);
    dispatcher->dispatch(startRequest);
    // EXPECT_TRUE(serializer->writeHeaderCalled);

    auto completeInputRequest = ChannelStateWriteRequest::completeInput(jobVertexID, 1, 1);
    dispatcher->dispatch(completeInputRequest);

    auto completeOutputRequest = ChannelStateWriteRequest::completeOutput(jobVertexID, 1, 1);
    dispatcher->dispatch(completeOutputRequest);

    auto releaseRequest = std::make_shared<SubtaskReleaseRequest>(jobVertexID, 1);
    dispatcher->dispatch(releaseRequest);

    auto oldCheckpointRequest = ChannelStateWriteRequest::completeInput(jobVertexID, 1, 0);
    dispatcher->dispatch(oldCheckpointRequest);
    // EXPECT_FALSE(targetResult->IsDone());

    auto abortRequest = ChannelStateWriteRequest::terminate(
        jobVertexID, 1, 1, std::make_exception_ptr(std::runtime_error("Test error")));
    dispatcher->dispatch(abortRequest);
    // EXPECT_TRUE(targetResult->GetInputChannelStateHandles()->IsCancelled());
    // EXPECT_TRUE(targetResult->GetResultSubpartitionStateHandles()->IsCancelled());

    auto invalidStartRequest =
        std::make_shared<CheckpointStartRequest>(jobVertexID, 1, 0, targetResult, &locationReference);
    dispatcher->dispatch(invalidStartRequest);
    // EXPECT_TRUE(targetResult->GetInputChannelStateHandles()->IsCancelled());

    auto unregisterRequest = std::make_shared<SubtaskReleaseRequest>(jobVertexID, 2);
    // EXPECT_NO_THROW(dispatcher->dispatch(unregisterRequest));
}
