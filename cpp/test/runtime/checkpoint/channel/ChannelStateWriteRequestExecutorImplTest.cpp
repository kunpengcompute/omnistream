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
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <memory>
#include <vector>

#include "runtime/checkpoint/channel/ChannelStateWriteRequestExecutorImpl.h"
#include "runtime/checkpoint/channel/ChannelStateWriteRequestDispatcherImpl.h"
#include "runtime/checkpoint/channel/ChannelStateWriteRequest.h"
#include "runtime/checkpoint/channel/ChannelStateSerializer.h"
#include "runtime/state/CheckpointStorage.h"
#include "runtime/state/filesystem/FsCheckpointStorageAccess.h"
#include "core/utils/threads/CompletableFutureV2.h"

using namespace omnistream;
using namespace std::chrono_literals;

class ChannelStateSerializerImplTest : public ChannelStateSerializer {
public:
    void WriteHeader(std::ostringstream&) override
    {
        writeHeaderCalled = true;
    }
    void WriteData(std::ostringstream& dataStream, std::shared_ptr<Buffer> buffer) override
    {
        writeDataCalled = true;
    }
    int64_t GetHeaderLength() const override
    {
        return 999;
    }

    bool writeHeaderCalled{false};
    bool writeDataCalled{false};
};

class CheckpointStorageTest : public CheckpointStorage {
public:
    std::shared_ptr<CheckpointStorageAccess> createCheckpointStorage(const JobIDPOD& jobId) override
    {
        return std::make_shared<FsCheckpointStorageAccess>(
            std::make_shared<Path>(""), std::make_shared<Path>(""), jobId, 100, 100);
    }
};

class ChannelStateWriteRequestDispatcherImplTest : public ChannelStateWriteRequestDispatcherImpl {
public:
    ChannelStateWriteRequestDispatcherImplTest(
        std::shared_ptr<CheckpointStorage> checkpointStorage,
        const JobIDPOD& jobID,
        std::shared_ptr<ChannelStateSerializer> serializer,
        std::shared_ptr<CheckpointStorageWorkerView> streamFactoryResolver)
        : ChannelStateWriteRequestDispatcherImpl(checkpointStorage, jobID, serializer, streamFactoryResolver)
    {
    }

    void dispatch(std::shared_ptr<ChannelStateWriteRequest> request) override
    {
        ChannelStateWriteRequestDispatcherImpl::dispatch(request);
        {
            std::lock_guard<std::mutex> lock(mutex);
            processedRequests.push_back(request->getName());
            processedSubtaskIDs.push_back(request->getSubtaskIndex());
            count++;
            cv.notify_all();
        }
    }

    bool waitFor(int expected, std::chrono::milliseconds timeout = 100ms)
    {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, timeout, [this, expected] { return count >= expected; });
    }
    std::mutex mutex;
    std::condition_variable cv;
    std::atomic<int> count{0};
    std::vector<std::string> processedRequests;
    std::vector<int> processedSubtaskIDs;
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
    std::pair<uint8_t*, size_t> GetBytes() override
    {
        return {nullptr, 0};
    }
    int GetBufferType() override
    {
        return 2;
    };

    bool recycled = false;
};

TEST(ChannelStateWriteRequestExecutorImplTest, ProcessesFullLifecycle)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    std::mutex registerLock;
    ChannelStateWriteRequestExecutorImpl executor(dispatcher);
    executor.start();

    JobVertexID jvid1(1, 1);
    JobVertexID jvid2(2, 2);
    auto targetResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    CheckpointStorageLocationReference locationRef;

    executor.registerSubtask(jvid1, 0);
    executor.registerSubtask(jvid2, 0);

    executor.submit(ChannelStateWriteRequest::start(jvid1, 0, 1, "Start"));
    executor.submit(ChannelStateWriteRequest::start(jvid2, 0, 1, "Start"));

    executor.submit(ChannelStateWriteRequest::writeInput(jvid1, 0, 1, InputChannelInfo{}, {}));
    executor.submit(ChannelStateWriteRequest::writeInput(jvid2, 0, 1, InputChannelInfo{}, {}));

    executor.submit(ChannelStateWriteRequest::completeInput(jvid1, 0, 1));
    executor.submit(ChannelStateWriteRequest::completeOutput(jvid1, 0, 1));
    executor.submit(ChannelStateWriteRequest::completeInput(jvid2, 0, 1));
    executor.submit(ChannelStateWriteRequest::completeOutput(jvid2, 0, 1));

    executor.releaseSubtask(jvid1, 0);
    executor.releaseSubtask(jvid2, 0);

    std::this_thread::sleep_for(50ms);

    // EXPECT_TRUE(serializer->writeHeaderCalled);

    executor.shutdown();
}

TEST(ChannelStateWriteRequestExecutorImplTest, DISABLED_PriorityRequests)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImplTest>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    std::mutex registerLock;
    ChannelStateWriteRequestExecutorImpl executor(dispatcher);
    executor.start();

    JobVertexID jvid(1, 1);
    executor.registerSubtask(jvid, 0);

    auto targetResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    CheckpointStorageLocationReference locationRef;
    executor.submit(ChannelStateWriteRequest::start(jvid, 1, 1, "Start"));

    auto buffer = std::make_shared<ObjectBufferTest>();
    std::vector<std::shared_ptr<Buffer>> buffers;
    buffers.push_back(buffer);
    executor.submit(ChannelStateWriteRequest::writeInput(jvid, 2, 1, InputChannelInfo{}, buffers));
    executor.submit(ChannelStateWriteRequest::writeInput(jvid, 3, 1, InputChannelInfo{}, buffers));
    executor.submitPriority(ChannelStateWriteRequest::writeInput(jvid, 4, 1, InputChannelInfo{}, buffers));
    executor.submit(ChannelStateWriteRequest::writeInput(jvid, 5, 1, InputChannelInfo{}, buffers));
    executor.submit(ChannelStateWriteRequest::completeInput(jvid, 6, 1));

    dispatcher->waitFor(6);

    // EXPECT_EQ(dispatcher->processedRequests.size(), 6);
    // EXPECT_EQ(dispatcher->processedRequests[0], "Register");
    // EXPECT_EQ(dispatcher->processedSubtaskIDs[0], 0);

    // EXPECT_TRUE((dispatcher->processedSubtaskIDs[1] == 4 && dispatcher->processedRequests[1] == "WriteInput") ||
    //             (dispatcher->processedSubtaskIDs[2] == 4 && dispatcher->processedRequests[2] == "WriteInput") ||
    //             (dispatcher->processedSubtaskIDs[3] == 4 && dispatcher->processedRequests[3] == "WriteInput") )
    //             << "Priority subtask ID 4 not priortized";

    // EXPECT_NE(dispatcher->processedSubtaskIDs[4], 4);
    // EXPECT_EQ(dispatcher->processedRequests[5], "WriteInput");
    // EXPECT_EQ(dispatcher->processedSubtaskIDs[5], 5);

    executor.shutdown();
}

TEST(ChannelStateWriteRequestExecutorImplTest, UnreadyRequests)
{
    // auto storage = std::make_shared<CheckpointStorageTest>();
    // auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    // auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
    //     storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1))
    // );
    //
    // std::mutex registerLock;
    // ChannelStateWriteRequestExecutorImpl executor(dispatcher);
    // executor.start();
    //
    // JobVertexID jvid(1, 1);
    // executor.registerSubtask(jvid, 0);
    //
    // auto targetResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    // CheckpointStorageLocationReference locationRef;
    // executor.submit(ChannelStateWriteRequest::start(jvid, 0, 1, "Start"));
    //
    // auto buffer = std::make_shared<ObjectBufferTest>();
    // std::vector<std::shared_ptr<Buffer>> buffers;
    // buffers.push_back(buffer);
    // executor.submit(ChannelStateWriteRequest::writeInput(
    //     jvid,
    //     0,
    //     1,
    //     InputChannelInfo{},
    //     buffers
    // ));
    // executor.submit(ChannelStateWriteRequest::completeInput(jvid, 0, 1));
    // executor.submit(ChannelStateWriteRequest::completeOutput(jvid, 0, 1));
    // std::this_thread::sleep_for(500ms);
    // EXPECT_TRUE(serializer->writeHeaderCalled);
    // EXPECT_TRUE(serializer->writeDataCalled);
    // executor.shutdown();
}

TEST(ChannelStateWriteRequestExecutorImplTest, CleanUpOnSubtaskRelease)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    std::mutex registerLock;
    ChannelStateWriteRequestExecutorImpl executor(dispatcher);
    executor.start();

    JobVertexID jvid(1, 1);
    executor.registerSubtask(jvid, 0);

    auto future = std::make_shared<CompletableFutureV2<void>>();
    int subtaskIndex = 0;
    auto request = std::make_shared<CheckpointInProgressRequest>(
        "TestRequest", jvid, 0, 1, [jvid, subtaskIndex](std::shared_ptr<ChannelStateCheckpointWriter>& w) {
            w->CompleteInput(jvid, subtaskIndex);
        });

    executor.submit(std::move(request));
    executor.releaseSubtask(jvid, 0);

    EXPECT_THROW(future->Get(), std::runtime_error);

    executor.shutdown();
}

TEST(ChannelStateWriteRequestExecutorImplTest, ShutdownCancelsPendingRequests)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    std::mutex registerLock;
    ChannelStateWriteRequestExecutorImpl executor(dispatcher);
    executor.start();

    JobVertexID jvid(1, 1);
    executor.registerSubtask(jvid, 0);

    auto future = std::make_shared<CompletableFutureV2<void>>();
    int subtaskIndex = 0;
    auto request = std::make_shared<CheckpointInProgressRequest>(
        "TestRequest", jvid, 0, 1, [jvid, subtaskIndex](std::shared_ptr<ChannelStateCheckpointWriter>& w) {
            w->CompleteInput(jvid, subtaskIndex);
        });

    executor.submit(std::move(request));
    executor.shutdown();

    EXPECT_THROW(future->Get(), std::runtime_error);
}

TEST(ChannelStateWriteRequestExecutorImplTest, RegistrationFlow)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    std::mutex registerLock;
    std::atomic<int> callbackCount{0};
    std::mutex callbackMutex;
    std::condition_variable callbackCv;

    ChannelStateWriteRequestExecutorImpl executor(dispatcher);
    executor.start();

    JobVertexID jvid1(1, 1);
    JobVertexID jvid2(2, 2);

    executor.registerSubtask(jvid1, 0);
    EXPECT_EQ(callbackCount.load(), 0);

    executor.registerSubtask(jvid2, 0);

    {
        std::unique_lock<std::mutex> lock(callbackMutex);
        bool callbackReceived = callbackCv.wait_for(lock, 100ms, [&] { return callbackCount.load() == 1; });
        EXPECT_TRUE(callbackReceived);
    }

    JobVertexID jvid3(3, 3);
    EXPECT_THROW(executor.registerSubtask(jvid3, 0), std::logic_error);

    executor.shutdown();
}

TEST(ChannelStateWriteRequestExecutorImplTest, ConcurrentRequests)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    std::mutex registerLock;
    ChannelStateWriteRequestExecutorImpl executor(dispatcher);
    executor.start();

    for (int i = 0; i < 5; i++) {
        executor.registerSubtask(JobVertexID(i, i), i);
    }

    auto targetResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    CheckpointStorageLocationReference locationRef;
    for (int i = 0; i < 5; i++) {
        executor.submit(ChannelStateWriteRequest::start(JobVertexID(i, i), i, 1, "Start"));
    }

    int threadCount = 5;
    int requestsPerThread = 10;
    std::vector<std::thread> threads;
    for (int t = 0; t < threadCount; t++) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < requestsPerThread; i++) {
                JobVertexID jvid(t % 5, t % 5);
                std::vector<std::shared_ptr<Buffer>> buffers;
                buffers.push_back(std::make_shared<ObjectBufferTest>());
                executor.submit(ChannelStateWriteRequest::writeInput(jvid, t % 5, 1, InputChannelInfo{}, buffers));
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    for (int i = 0; i < 5; i++) {
        executor.releaseSubtask(JobVertexID(i, i), i);
    }

    std::this_thread::sleep_for(100ms);
    EXPECT_TRUE(serializer->writeDataCalled);

    executor.shutdown();
}

TEST(ChannelStateWriteRequestExecutorImplTest, RequestOrdering)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImplTest>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    std::mutex registerLock;
    ChannelStateWriteRequestExecutorImpl executor(dispatcher);
    executor.start();

    JobVertexID jvid(1, 1);
    executor.registerSubtask(jvid, 0);

    auto targetResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    CheckpointStorageLocationReference locationRef;
    executor.submit(ChannelStateWriteRequest::start(jvid, 0, 42, "Start"));
    executor.submit(ChannelStateWriteRequest::writeInput(jvid, 0, 42, InputChannelInfo{}, {}));
    ASSERT_TRUE(dispatcher->waitFor(3));

    executor.submitPriority(ChannelStateWriteRequest::writeInput(jvid, 0, 42, InputChannelInfo{}, {}));
    executor.submit(ChannelStateWriteRequest::completeInput(jvid, 0, 42));

    ASSERT_TRUE(dispatcher->waitFor(5));

    ASSERT_EQ(dispatcher->processedRequests.size(), 5);
    EXPECT_EQ(dispatcher->processedRequests[0], "Register");
    EXPECT_EQ(dispatcher->processedRequests[1], "Start");
    EXPECT_EQ(dispatcher->processedRequests[2], "WriteInput");
    EXPECT_EQ(dispatcher->processedRequests[3], "WriteInput");
    EXPECT_EQ(dispatcher->processedRequests[4], "CheckpointCompleteInput");

    executor.shutdown();
}

TEST(ChannelStateWriteRequestExecutorImplTest, OnRegisteredCalledOnce)
{
    auto storage = std::make_shared<CheckpointStorageTest>();
    auto serializer = std::make_shared<ChannelStateSerializerImplTest>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    std::mutex registerLock;
    std::mutex callbackMutex;
    std::condition_variable callbackCv;
    int called = 0;

    ChannelStateWriteRequestExecutorImpl executor(dispatcher);
    executor.start();

    executor.registerSubtask(JobVertexID(1, 1), 1);
    {
        std::unique_lock<std::mutex> lk(callbackMutex);
        EXPECT_FALSE(callbackCv.wait_for(lk, 20ms, [&] { return called > 0; }));
    }
    EXPECT_EQ(called, 0);

    executor.registerSubtask(JobVertexID(2, 2), 2); // should fire once
    {
        std::unique_lock<std::mutex> lk(callbackMutex);
        EXPECT_TRUE(callbackCv.wait_for(lk, 100ms, [&] { return called == 1; }));
    }
    EXPECT_THROW(executor.registerSubtask(JobVertexID(3, 3), 3), std::logic_error);

    executor.shutdown();
}
