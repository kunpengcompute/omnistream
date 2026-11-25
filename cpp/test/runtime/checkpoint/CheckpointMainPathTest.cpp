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
#include <set>
#include <memory>
#include <stdexcept>
#include <vector>

#include "runtime/checkpoint/channel/ChannelStatePendingResult.h"
#include "runtime/checkpoint/channel/ChannelStateWriteRequest.h"
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "runtime/checkpoint/channel/ChannelStateWriteRequestDispatcherImpl.h"
#include "runtime/checkpoint/channel/ChannelStateSerializer.h"
#include "runtime/checkpoint/channel/ChannelStateCheckpointWriter.h"
#include "runtime/checkpoint/channel/CheckpointBufferUtils.h"
#include "runtime/buffer/ObjectBufferRecycler.h"
#include "runtime/buffer/ReadOnlySlicedVectorBatchBuffer.h"
#include "runtime/state/CheckpointStorage.h"
#include "runtime/state/filesystem/FsCheckpointStorageAccess.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "streaming/runtime/io/checkpointing/ChannelState.h"
#include "streaming/runtime/io/checkpointing/AbstractAlternatingAlignedBarrierHandlerState.h"
#include "streaming/runtime/io/checkpointing/AlternatingWaitingForFirstBarrier.h"
#include "streaming/runtime/io/checkpointing/AlternatingCollectingBarriers.h"
#include "runtime/io/network/api/CheckpointBarrier.h"
#include "runtime/io/checkpointing/CheckpointBarrierHandler.h"
#include "core/utils/threads/CompletableFutureV2.h"

using namespace omnistream;

// =========================================================================
// Test mocks
// =========================================================================

class MockSerializer : public ChannelStateSerializerImpl {
public:
    int64_t GetHeaderLength() const override
    {
        return 100;
    }
};

class MockStorage : public CheckpointStorage {
public:
    std::shared_ptr<CheckpointStorageAccess> createCheckpointStorage(const JobIDPOD& jobId) override
    {
        auto checkpointDir = std::make_shared<Path>("");
        auto savepointDir = std::make_shared<Path>("");
        return std::make_shared<FsCheckpointStorageAccess>(checkpointDir, savepointDir, jobId, 100, 100);
    }
};

class MockBuffer : public ObjectBuffer {
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
    Buffer* ReadOnlySlice(int, int) override
    {
        return this;
    }
    int GetMaxCapacity() const override
    {
        return 512;
    }
    int GetReaderIndex() const override
    {
        return 0;
    }
    void SetReaderIndex(int) override
    {
    }
    int GetSize() const override
    {
        return 128;
    }
    void SetSize(int) override
    {
    }
    int ReadableObjects() const override
    {
        return 1;
    }
    bool IsCompressed() const override
    {
        return false;
    }
    void SetCompressed(bool) override
    {
    }
    ObjectBufferDataType GetDataType() const override
    {
        return ObjectBufferDataType::DATA_BUFFER;
    }
    void SetDataType(ObjectBufferDataType) override
    {
    }
    int RefCount() const override
    {
        return 1;
    }
    std::string ToDebugString(bool) const override
    {
        return "MockBuffer";
    }
    ObjectSegment* GetObjectSegment() override
    {
        return nullptr;
    }
    int GetBufferType() override
    {
        return 0;
    }
    std::pair<uint8_t*, size_t> GetBytes() override
    {
        return {nullptr, 0};
    }
    bool recycled = false;
};

class TrackingVectorBatchSlice : public ReadOnlySlicedVectorBatchBuffer {
public:
    TrackingVectorBatchSlice(VectorBatchBuffer* parent, int* destroyed)
        : ReadOnlySlicedVectorBatchBuffer(parent, 0, 1),
          destroyed_(destroyed)
    {
    }

    ~TrackingVectorBatchSlice() override
    {
        ++(*destroyed_);
    }

private:
    int* destroyed_;
};

// =========================================================================
// ChannelStatePendingResult tests
// =========================================================================
TEST(ChannelStatePendingResultTest, InitialStateNotDone)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    auto serializer = std::make_shared<MockSerializer>();
    ChannelStatePendingResult pending(0, 1, result, serializer);
    EXPECT_FALSE(pending.IsDone());
    EXPECT_FALSE(pending.IsAllInputsReceived());
    EXPECT_FALSE(pending.IsAllOutputsReceived());
    EXPECT_EQ(pending.GetSubtaskIndex(), 0);
    EXPECT_EQ(pending.GetCheckpointId(), 1);
}

TEST(ChannelStatePendingResultTest, CompleteInputSetsFlag)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    auto serializer = std::make_shared<MockSerializer>();
    ChannelStatePendingResult pending(0, 1, result, serializer);
    pending.CompleteInput();
    EXPECT_TRUE(pending.IsAllInputsReceived());
}

TEST(ChannelStatePendingResultTest, CompleteInputTwiceThrows)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    auto serializer = std::make_shared<MockSerializer>();
    ChannelStatePendingResult pending(0, 1, result, serializer);
    pending.CompleteInput();
    EXPECT_THROW(pending.CompleteInput(), std::logic_error);
}

TEST(ChannelStatePendingResultTest, CompleteOutputSetsFlag)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    auto serializer = std::make_shared<MockSerializer>();
    ChannelStatePendingResult pending(0, 1, result, serializer);
    pending.CompleteOutput();
    EXPECT_TRUE(pending.IsAllOutputsReceived());
}

TEST(ChannelStatePendingResultTest, CompleteOutputTwiceThrows)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    auto serializer = std::make_shared<MockSerializer>();
    ChannelStatePendingResult pending(0, 1, result, serializer);
    pending.CompleteOutput();
    EXPECT_THROW(pending.CompleteOutput(), std::logic_error);
}

TEST(ChannelStatePendingResultTest, FailDoesNotThrow)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    auto serializer = std::make_shared<MockSerializer>();
    ChannelStatePendingResult pending(0, 1, result, serializer);
    EXPECT_NO_THROW(pending.Fail(std::make_exception_ptr(std::runtime_error("test"))));
}

TEST(ChannelStatePendingResultTest, GetInputChannelOffsetsStartsEmpty)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    auto serializer = std::make_shared<MockSerializer>();
    ChannelStatePendingResult pending(0, 1, result, serializer);
    EXPECT_TRUE(pending.GetInputChannelOffsets().empty());
}

TEST(ChannelStatePendingResultTest, GetResultSubpartitionOffsetsStartsEmpty)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    auto serializer = std::make_shared<MockSerializer>();
    ChannelStatePendingResult pending(0, 1, result, serializer);
    EXPECT_TRUE(pending.GetResultSubpartitionOffsets().empty());
}

TEST(ChannelStatePendingResultTest, IsDoneReflectsResultState)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    auto serializer = std::make_shared<MockSerializer>();
    ChannelStatePendingResult pending(0, 1, result, serializer);
    EXPECT_FALSE(pending.IsDone());
}

// =========================================================================
// ChannelStateWriteRequest - CheckpointInProgressRequest tests
// =========================================================================
TEST(CheckpointInProgressRequestTest, ConstructorWithAction)
{
    CheckpointInProgressRequest req(
        "Test", JobVertexID(1, 1), 0, 42, [](std::shared_ptr<ChannelStateCheckpointWriter>&) {});
    EXPECT_EQ(req.getCheckpointId(), 42);
    EXPECT_EQ(req.getSubtaskIndex(), 0);
    EXPECT_EQ(req.getName(), "Test");
}

TEST(CheckpointInProgressRequestTest, ExecuteTransitionsState)
{
    CheckpointInProgressRequest req(
        "Test", JobVertexID(1, 1), 0, 1, [](std::shared_ptr<ChannelStateCheckpointWriter>&) {});
    std::shared_ptr<ChannelStateCheckpointWriter> writer = nullptr;
    EXPECT_NO_THROW(req.execute(writer));
}

TEST(CheckpointInProgressRequestTest, ExecuteTwiceThrows)
{
    CheckpointInProgressRequest req(
        "Test", JobVertexID(1, 1), 0, 1, [](std::shared_ptr<ChannelStateCheckpointWriter>&) {});
    std::shared_ptr<ChannelStateCheckpointWriter> writer = nullptr;
    req.execute(writer);
    EXPECT_THROW(req.execute(writer), std::runtime_error);
}

TEST(CheckpointInProgressRequestTest, CancelCallsDiscardAction)
{
    bool discarded = false;
    CheckpointInProgressRequest req(
        "Test",
        JobVertexID(1, 1),
        0,
        1,
        [](std::shared_ptr<ChannelStateCheckpointWriter>&) {},
        [&](const std::exception_ptr&) { discarded = true; });
    req.cancel(std::make_exception_ptr(std::runtime_error("cancel test")));
    EXPECT_TRUE(discarded);
}

TEST(CheckpointInProgressRequestTest, GetReadyFutureReturnsCompletedWhenNoFutureProvided)
{
    CheckpointInProgressRequest req(
        "Test", JobVertexID(1, 1), 0, 1, [](std::shared_ptr<ChannelStateCheckpointWriter>&) {});
    auto future = req.getReadyFuture();
    EXPECT_TRUE(future->IsDone());
}

TEST(CheckpointInProgressRequestTest, GetReadyFutureReflectsProvidedFuture)
{
    auto ready = std::make_shared<CompletableFutureV2<void>>();
    CheckpointInProgressRequest req(
        "Test",
        JobVertexID(1, 1),
        0,
        1,
        [](std::shared_ptr<ChannelStateCheckpointWriter>&) {},
        [](const std::exception_ptr&) {},
        ready);
    EXPECT_FALSE(req.getReadyFuture()->IsDone());
    ready->Complete();
    EXPECT_TRUE(req.getReadyFuture()->IsDone());
}

// =========================================================================
// ChannelStateWriteRequest - writeInput buffers recycled on cancel
// =========================================================================
TEST(ChannelStateWriteRequestTest, WriteInputDiscardRecyclesBuffers)
{
    auto buf = std::make_shared<MockBuffer>();
    std::vector<Buffer*> buffers = {buf.get()};
    auto req = ChannelStateWriteRequest::writeInput(JobVertexID(1, 1), 0, 1, InputChannelInfo{}, buffers);
    EXPECT_FALSE(buf->recycled);
    req->cancel(std::make_exception_ptr(std::runtime_error("cancel")));
    EXPECT_TRUE(buf->recycled);
}

TEST(ChannelStateWriteRequestTest, WriteOutputDiscardRecyclesBuffers)
{
    auto buf = std::make_shared<MockBuffer>();
    std::vector<Buffer*> buffers = {buf.get()};
    auto req = ChannelStateWriteRequest::writeOutput(JobVertexID(1, 1), 0, 1, ResultSubpartitionInfoPOD{}, buffers);
    EXPECT_FALSE(buf->recycled);
    req->cancel(std::make_exception_ptr(std::runtime_error("cancel")));
    EXPECT_TRUE(buf->recycled);
}

TEST(ChannelStateWriteRequestTest, WriteInputDiscardDeletesReadOnlySlice)
{
    ObjectSegment segment(1);
    auto* parent = new VectorBatchBuffer(&segment, DummyObjectBufferRecycler::getInstance());
    parent->SetSize(1);
    int destroyed = 0;
    std::vector<Buffer*> buffers = {new TrackingVectorBatchSlice(parent, &destroyed)};

    auto req = ChannelStateWriteRequest::writeInput(JobVertexID(1, 1), 0, 1, InputChannelInfo{}, buffers);
    req->cancel(std::make_exception_ptr(std::runtime_error("cancel")));

    EXPECT_EQ(destroyed, 1);
}

TEST(ChannelStateWriteRequestTest, WriteOutputDiscardDeletesReadOnlySlice)
{
    ObjectSegment segment(1);
    auto* parent = new VectorBatchBuffer(&segment, DummyObjectBufferRecycler::getInstance());
    parent->SetSize(1);
    int destroyed = 0;
    std::vector<Buffer*> buffers = {new TrackingVectorBatchSlice(parent, &destroyed)};

    auto req = ChannelStateWriteRequest::writeOutput(JobVertexID(1, 1), 0, 1, ResultSubpartitionInfoPOD{}, buffers);
    req->cancel(std::make_exception_ptr(std::runtime_error("cancel")));

    EXPECT_EQ(destroyed, 1);
}

TEST(ChannelStateWriteRequestTest, CompleteInputHasDoneFuture)
{
    auto req = ChannelStateWriteRequest::completeInput(JobVertexID(1, 1), 0, 1);
    EXPECT_TRUE(req->getReadyFuture()->IsDone());
    EXPECT_EQ(req->getName(), "CheckpointCompleteInput");
}

TEST(ChannelStateWriteRequestTest, CompleteOutputHasDoneFuture)
{
    auto req = ChannelStateWriteRequest::completeOutput(JobVertexID(1, 1), 0, 1);
    EXPECT_TRUE(req->getReadyFuture()->IsDone());
    EXPECT_EQ(req->getName(), "CheckpointCompleteOutput");
}

TEST(ChannelStateWriteRequestTest, RegisterSubtaskCreatesCorrectType)
{
    auto req = ChannelStateWriteRequest::registerSubtask(JobVertexID(1, 1), 0);
    EXPECT_EQ(req->getName(), "Register");
    EXPECT_EQ(req->getSubtaskIndex(), 0);
}

TEST(ChannelStateWriteRequestTest, ReleaseSubtaskCreatesCorrectType)
{
    auto req = ChannelStateWriteRequest::releaseSubtask(JobVertexID(1, 1), 0);
    EXPECT_EQ(req->getName(), "Release");
    EXPECT_EQ(req->getSubtaskIndex(), 0);
}

// =========================================================================
// ChannelStateWriteRequestDispatcherImpl - abort logic
// =========================================================================
TEST(ChannelStateWriteRequestDispatcherImplTest, AbortedCheckpointIdDetection)
{
    auto storage = std::make_shared<MockStorage>();
    auto serializer = std::make_shared<MockSerializer>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    JobVertexID jvid(1, 1);
    auto targetResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();

    dispatcher->dispatch(ChannelStateWriteRequest::registerSubtask(jvid, 1));
    auto startReq = ChannelStateWriteRequest::start(jvid, 1, 5, targetResult, "Start");
    dispatcher->dispatch(startReq);

    auto abortReq =
        ChannelStateWriteRequest::terminate(jvid, 1, 5, std::make_exception_ptr(std::runtime_error("abort")));
    EXPECT_NO_THROW(dispatcher->dispatch(abortReq));
}

TEST(ChannelStateWriteRequestDispatcherImplTest, SubsumedCheckpointIsCancelled)
{
    auto storage = std::make_shared<MockStorage>();
    auto serializer = std::make_shared<MockSerializer>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    JobVertexID jvid(1, 1);
    auto targetResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();

    dispatcher->dispatch(ChannelStateWriteRequest::registerSubtask(jvid, 1));
    auto startReq = ChannelStateWriteRequest::start(jvid, 1, 10, targetResult, "Start");
    dispatcher->dispatch(startReq);

    auto oldStartReq = ChannelStateWriteRequest::start(jvid, 1, 5, targetResult, "Start");
    EXPECT_NO_THROW(dispatcher->dispatch(oldStartReq));
}

TEST(ChannelStateWriteRequestDispatcherImplTest, NoStartBeforeAbortDoesNotThrow)
{
    auto storage = std::make_shared<MockStorage>();
    auto serializer = std::make_shared<MockSerializer>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    JobVertexID jvid(1, 1);
    dispatcher->dispatch(ChannelStateWriteRequest::registerSubtask(jvid, 1));

    auto abortReq =
        ChannelStateWriteRequest::terminate(jvid, 1, 1, std::make_exception_ptr(std::runtime_error("early abort")));
    EXPECT_NO_THROW(dispatcher->dispatch(abortReq));
}

TEST(ChannelStateWriteRequestDispatcherImplTest, ReleaseUnregistersSubtask)
{
    auto storage = std::make_shared<MockStorage>();
    auto serializer = std::make_shared<MockSerializer>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        storage, JobIDPOD(-1, -1), serializer, storage->createCheckpointStorage(JobIDPOD(-1, -1)));

    JobVertexID jvid(1, 1);
    auto registerReq = std::make_shared<SubtaskRegisterRequest>(jvid, 1);
    EXPECT_NO_THROW(dispatcher->dispatch(registerReq));

    auto releaseReq = std::make_shared<SubtaskReleaseRequest>(jvid, 1);
    EXPECT_NO_THROW(dispatcher->dispatch(releaseReq));
}

// =========================================================================
// ChannelState - state machine tests
// =========================================================================

class MockCheckpointableInput : public CheckpointableInput {
public:
    void BlockConsumption(const InputChannelInfo&) override
    {
        blocked = true;
    }
    void ResumeConsumption(const InputChannelInfo&) override
    {
        resumed = true;
    }
    void TimeOutResumeConsumption(const omnistream::InputChannelInfo& channelInfo)
    {
    }
    void notifyDataAvailable()
    {
    }
    void ConvertToPriorityEvent(int, int) override
    {
        prioritized = true;
    }
    std::vector<InputChannelInfo> GetChannelInfos() override
    {
        return {InputChannelInfo(GetInputGateIndex(), 0)};
    }
    void CheckpointStarted(const CheckpointBarrier&) override
    {
    }
    void CheckpointStopped(long) override
    {
    }
    int GetInputGateIndex() override
    {
        return gateIdx;
    }

    int gateIdx = 0;
    bool blocked = false;
    bool resumed = false;
    bool prioritized = false;
};

TEST(ChannelStateTest, BlockChannelCallsBlockOnInput)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    state.BlockChannel(InputChannelInfo(0, 0));
    EXPECT_TRUE(input.blocked);
}

TEST(ChannelStateTest, UnblockAllChannelsResumesBlockedInputs)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    state.BlockChannel(InputChannelInfo(0, 0));
    state.UnblockAllChannels();
    EXPECT_TRUE(input.resumed);
}

TEST(ChannelStateTest, UnblockAllDoesNotResumeAlreadyResumed)
{
    MockCheckpointableInput input0, input1;
    input1.gateIdx = 1;
    std::vector<CheckpointableInput*> inputs = {&input0, &input1};
    ChannelState state(inputs);
    state.BlockChannel(InputChannelInfo(0, 0));
    state.UnblockAllChannels();
    EXPECT_TRUE(input0.resumed);
    EXPECT_FALSE(input1.resumed);
}

TEST(ChannelStateTest, ChannelFinishedRemovesBlockedState)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    state.BlockChannel(InputChannelInfo(0, 0));
    state.ChannelFinished(InputChannelInfo(0, 0));
    EXPECT_NO_THROW(state.EmptyState());
}

TEST(ChannelStateTest, EmptyStateThrowsWhenBlockedChannelsExist)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    state.BlockChannel(InputChannelInfo(0, 0));
    EXPECT_THROW(state.EmptyState(), std::runtime_error);
}

TEST(ChannelStateTest, EmptyStateSucceedsAfterUnblockAll)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    state.BlockChannel(InputChannelInfo(0, 0));
    state.UnblockAllChannels();
    EXPECT_NO_THROW(state.EmptyState());
}

TEST(ChannelStateTest, PrioritizeAllAnnouncementsCallsConvert)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    state.addSeenAnnouncement(InputChannelInfo(0, 0), 42);
    state.PrioritizeAllAnnouncements();
    EXPECT_TRUE(input.prioritized);
}

TEST(ChannelStateTest, AnnouncementSeenAndRemoved)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    state.addSeenAnnouncement(InputChannelInfo(0, 0), 7);
    state.removeSeenAnnouncement(InputChannelInfo(0, 0));
    EXPECT_NO_THROW(state.PrioritizeAllAnnouncements());
}

TEST(ChannelStateTest, GetInputsReturnsCorrect)
{
    MockCheckpointableInput input0, input1;
    input1.gateIdx = 1;
    std::vector<CheckpointableInput*> inputs = {&input0, &input1};
    ChannelState state(inputs);
    auto result = state.getInputs();
    EXPECT_EQ(result.size(), 2u);
}

// =========================================================================
// BarrierHandlerState - transition tests
// =========================================================================

class MockController : public Controller {
public:
    bool AllBarriersReceived() const override
    {
        return allReceived;
    }
    const CheckpointBarrier* GetPendingCheckpointBarrier() const override
    {
        return nullptr;
    }
    void TriggerGlobalCheckpoint(const CheckpointBarrier&) override
    {
        triggered = true;
    }
    void InitInputsCheckpoint(const CheckpointBarrier&) override
    {
        initialized = true;
    }
    bool IsTimedOut(const CheckpointBarrier&) override
    {
        return timedOut;
    }

    bool allReceived = false;
    bool triggered = false;
    bool initialized = false;
    bool timedOut = false;
};

class TestBarrierHandler : public AbstractAlternatingAlignedBarrierHandlerState {
public:
    TestBarrierHandler(ChannelState state) : AbstractAlternatingAlignedBarrierHandlerState(std::move(state))
    {
    }

protected:
    BarrierHandlerState* TransitionAfterBarrierReceived(ChannelState s) override
    {
        return new AlternatingCollectingBarriers(std::move(s));
    }
    BarrierHandlerState* AlignedCheckpointTimeout(Controller*, CheckpointBarrier*) override
    {
        return new AlternatingWaitingForFirstBarrier(state.EmptyState());
    }
};

TEST(BarrierHandlerStateTest, SingleChannelBarrierReceivedTriggersCheckpoint)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    MockController controller;

    std::unique_ptr<TestBarrierHandler> handler(new TestBarrierHandler(std::move(state)));
    controller.allReceived = true;

    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(1, 1000, options);

    std::unique_ptr<BarrierHandlerState> next(
        handler->BarrierReceived(&controller, InputChannelInfo(0, 0), &barrier, true));
    EXPECT_TRUE(controller.initialized);
    EXPECT_TRUE(controller.triggered);
    EXPECT_TRUE(input.resumed);
}

TEST(BarrierHandlerStateTest, BarrierReceivedBlocksChannelWhenMarked)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    MockController controller;

    std::unique_ptr<TestBarrierHandler> handler(new TestBarrierHandler(std::move(state)));
    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(1, 1000, options);

    std::unique_ptr<BarrierHandlerState> next(
        handler->BarrierReceived(&controller, InputChannelInfo(0, 0), &barrier, true));
    EXPECT_TRUE(input.blocked);
}

TEST(BarrierHandlerStateTest, BarrierReceivedDoesNotBlockWhenNotMarked)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    MockController controller;

    std::unique_ptr<TestBarrierHandler> handler(new TestBarrierHandler(std::move(state)));
    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(1, 1000, options);

    std::unique_ptr<BarrierHandlerState> next(
        handler->BarrierReceived(&controller, InputChannelInfo(0, 0), &barrier, false));
    EXPECT_FALSE(input.blocked);
}

TEST(BarrierHandlerStateTest, AnnouncementReceivedRecordsState)
{
    MockCheckpointableInput input;
    std::vector<CheckpointableInput*> inputs = {&input};
    ChannelState state(inputs);
    MockController controller;

    std::unique_ptr<TestBarrierHandler> handler(new TestBarrierHandler(std::move(state)));
    auto next = handler->AnnouncementReceived(&controller, InputChannelInfo(0, 0), 99);
    EXPECT_EQ(next, handler.get());
}

// =========================================================================
// ChannelStateWriteResult tests
// =========================================================================
TEST(ChannelStateWriteResultTest, CreateEmptyIsNotDone)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    EXPECT_FALSE(result->IsDone());
}

TEST(ChannelStateWriteResultTest, IsDoneAfterBothFuturesComplete)
{
    auto inputFuture = std::make_shared<
        CompletableFutureV2<ChannelStateWriter::ChannelStateWriteResult::InputChannelStateHandleVecPtr>>();
    auto outputFuture = std::make_shared<
        CompletableFutureV2<ChannelStateWriter::ChannelStateWriteResult::ResultSubpartitionStateVecPtr>>();
    ChannelStateWriter::ChannelStateWriteResult result(inputFuture, outputFuture);

    EXPECT_FALSE(result.IsDone());
    inputFuture->Complete(std::make_shared<std::vector<std::shared_ptr<InputChannelStateHandle>>>());
    EXPECT_FALSE(result.IsDone());
    outputFuture->Complete(std::make_shared<std::vector<std::shared_ptr<ResultSubpartitionStateHandle>>>());
    EXPECT_TRUE(result.IsDone());
}

TEST(ChannelStateWriteResultTest, FailCancelsBothFutures)
{
    auto inputFuture = std::make_shared<
        CompletableFutureV2<ChannelStateWriter::ChannelStateWriteResult::InputChannelStateHandleVecPtr>>();
    auto outputFuture = std::make_shared<
        CompletableFutureV2<ChannelStateWriter::ChannelStateWriteResult::ResultSubpartitionStateVecPtr>>();
    ChannelStateWriter::ChannelStateWriteResult result(inputFuture, outputFuture);

    result.Fail(std::make_exception_ptr(std::runtime_error("fail")));
    EXPECT_TRUE(result.GetInputChannelStateHandles()->IsCancelled());
    EXPECT_TRUE(result.GetResultSubpartitionStateHandles()->IsCancelled());
}

TEST(ChannelStateWriteResultTest, IsDoneFalseWhenFuturesNotComplete)
{
    auto inputFuture = std::make_shared<
        CompletableFutureV2<ChannelStateWriter::ChannelStateWriteResult::InputChannelStateHandleVecPtr>>();
    auto outputFuture = std::make_shared<
        CompletableFutureV2<ChannelStateWriter::ChannelStateWriteResult::ResultSubpartitionStateVecPtr>>();
    ChannelStateWriter::ChannelStateWriteResult result(inputFuture, outputFuture);

    EXPECT_FALSE(result.IsDone());
}

// =========================================================================
// ChannelStateCheckpointWriter - subtask registration
// =========================================================================
TEST(ChannelStateCheckpointWriterTest, ConstructorRequiresNonEmptySubtasks)
{
    std::set<SubtaskID> empty;
    EXPECT_THROW(
        { ChannelStateCheckpointWriter writer(empty, 1, nullptr, std::make_shared<MockSerializer>(), []() {}); },
        std::invalid_argument);
}

// =========================================================================
// SubtaskID tests
// =========================================================================
TEST(SubtaskIDTest, EqualityAndOrdering)
{
    SubtaskID a(JobVertexID(1, 1), 0);
    SubtaskID b(JobVertexID(1, 1), 0);
    SubtaskID c(JobVertexID(1, 1), 1);
    SubtaskID d(JobVertexID(2, 2), 0);

    EXPECT_EQ(a, b);
    EXPECT_FALSE(a == c);
    EXPECT_FALSE(a == d);
    EXPECT_LT(a, c);
    EXPECT_LT(a, d);
}

TEST(SubtaskIDTest, OfFactoryCreatesCorrectId)
{
    auto sid = SubtaskID::Of(JobVertexID(3, 4), 5);
    EXPECT_EQ(sid.GetJobVertexID(), JobVertexID(3, 4));
    EXPECT_EQ(sid.GetSubtaskIndex(), 5);
}

TEST(SubtaskIDTest, DefaultConstructor)
{
    SubtaskID sid;
    EXPECT_EQ(sid.GetSubtaskIndex(), -1);
    EXPECT_EQ(sid.GetJobVertexID(), JobVertexID(-1, -1));
}

// =========================================================================
// CheckpointAbortRequest tests
// =========================================================================
TEST(CheckpointAbortRequestTest, ConstructorStoresCause)
{
    auto cause = std::make_exception_ptr(std::runtime_error("abort"));
    CheckpointAbortRequest req(JobVertexID(1, 1), 0, 1, cause);
    EXPECT_EQ(req.getName(), "Abort");
    EXPECT_EQ(req.getCheckpointId(), 1);
    EXPECT_EQ(req.getSubtaskIndex(), 0);
}

TEST(CheckpointAbortRequestTest, CancelIsNoOp)
{
    auto cause = std::make_exception_ptr(std::runtime_error("abort"));
    CheckpointAbortRequest req(JobVertexID(1, 1), 0, 1, cause);
    EXPECT_NO_THROW(req.cancel(std::make_exception_ptr(std::runtime_error("cancel"))));
}
