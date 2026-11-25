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

#ifndef OMNISTREAM_CHANNELSTATEWRITER_H
#define OMNISTREAM_CHANNELSTATEWRITER_H
#include <future>
#include <vector>
#include <memory>
#include <exception>
#include <functional>
#include <stdexcept>
#include <iostream>
#include "../CheckpointOptions.h"
#include "runtime/state/ResultSubpartitionStateHandle.h"
#include "runtime/state/InputChannelStateHandle.h"
#include "runtime/partition/ResultSubpartitionInfoPOD.h"
#include "runtime/partition/consumer/InputChannelInfo.h"
#include "core/utils/lang/AutoCloseable.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/buffer/ObjectBuffer.h"
#include "core/utils/threads/CompletableFuture.h"
#include "runtime/state/CheckpointStorageWorkerView.h"

/** Writes channel state during checkpoint/savepoint. */
// @Internal
class ChannelStateWriter {
public:
    /** Channel state write result. */
    class ChannelStateWriteResult {
    public:
        using InputChannelStateHandleVecPtr = std::shared_ptr<std::vector<std::shared_ptr<InputChannelStateHandle>>>;
        using ResultSubpartitionStateVecPtr =
            std::shared_ptr<std::vector<std::shared_ptr<ResultSubpartitionStateHandle>>>;
        using InputChannelStateFuture = std::shared_ptr<CompletableFutureV2<InputChannelStateHandleVecPtr>>;
        using ResultSubpartitionStateFuture = std::shared_ptr<CompletableFutureV2<ResultSubpartitionStateVecPtr>>;

        ChannelStateWriteResult() noexcept
            : inputChannelStateHandles(std::make_shared<CompletableFutureV2<InputChannelStateHandleVecPtr>>()),
              resultSubpartitionStateHandles(std::make_shared<CompletableFutureV2<ResultSubpartitionStateVecPtr>>())
        {
        }

        ChannelStateWriteResult(
            InputChannelStateFuture inputChannelStateHandles,
            ResultSubpartitionStateFuture resultSubpartitionStateHandles) noexcept
            : inputChannelStateHandles(std::move(inputChannelStateHandles)),
              resultSubpartitionStateHandles(std::move(resultSubpartitionStateHandles))
        {
        }

        InputChannelStateFuture GetInputChannelStateHandles()
        {
            return inputChannelStateHandles;
        }

        ResultSubpartitionStateFuture GetResultSubpartitionStateHandles()
        {
            return resultSubpartitionStateHandles;
        }

        static std::shared_ptr<ChannelStateWriteResult> CreateEmpty() noexcept
        {
            auto inputFuture = std::make_shared<CompletableFutureV2<InputChannelStateHandleVecPtr>>();
            auto resultFuture = std::make_shared<CompletableFutureV2<ResultSubpartitionStateVecPtr>>();

            return std::make_shared<ChannelStateWriteResult>(inputFuture, resultFuture);
        }

        void Fail(const std::exception_ptr& cause)
        {
            inputChannelStateHandles->Cancel(cause);
            resultSubpartitionStateHandles->Cancel(cause);
        }

        bool IsDone() const
        {
            return inputChannelStateHandles->IsDone() && resultSubpartitionStateHandles->IsDone();
        }

        bool IsNeedsChannelState()
        {
            return isNeedsChannelState;
        }
        void setIsNeedsChannelState(bool flag)
        {
            isNeedsChannelState = flag;
        }

    private:
        InputChannelStateFuture inputChannelStateHandles;
        ResultSubpartitionStateFuture resultSubpartitionStateHandles;
        bool isNeedsChannelState = false;
    };

    /**
     * Sequence number for the buffers that were saved during the previous execution attempt; then
     * restored; and now are to be saved again (as opposed to the buffers received from the upstream
     * or from the operator).
     */
    static const int sequenceNumberRestored = -1;

    /**
     * Signifies that buffer sequence number is unknown (e.g. if passing sequence numbers is not
     * implemented).
     */
    static const int sequenceNumberUnknown = -2;

    static ChannelStateWriteResult empty;
    /** Initiate write of channel state for the given checkpoint id. */
    virtual void Start(long checkpointId, const CheckpointOptions& checkpointOptions) = 0;
    virtual void open() = 0;
    virtual ~ChannelStateWriter() = default;

    /**
     * Add in-flight buffers from the {@link
     * org.apache.flink.runtime.io.network.partition.consumer.InputChannel InputChannel}. Must be
     * called after {@link #start} (long)} and before {@link #finishInput(long)}. Buffers are
     * recycled after they are written or exception occurs.
     *
     * @param startSeqNum sequence number of the 1st passed buffer. It is intended to use for
     *     incremental snapshots. If no data is passed it is ignored.
     * @param data zero or more <b>data</b> buffers ordered by their sequence numbers
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#sequenceNumberRestored
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#sequenceNumberUnknown
     */
    virtual void AddInputData(
        long checkpointId,
        const omnistream::InputChannelInfo& info,
        int startSeqNum,
        std::vector<omnistream::Buffer*> data) = 0;

    /**
     * Add in-flight buffers from the {@link
     * org.apache.flink.runtime.io.network.partition.ResultSubpartition ResultSubpartition}. Must be
     * called after {@link #start} and before {@link #finishOutput(long)}. Buffers are recycled
     * after they are written or exception occurs.
     *
     * @param startSeqNum sequence number of the 1st passed buffer. It is intended to use for
     *     incremental snapshots. If no data is passed it is ignored.
     * @param data zero or more <b>data</b> buffers ordered by their sequence numbers
     * @throws IllegalArgumentException if one or more passed buffers {@link Buffer#isBuffer() isn't
     *     a buffer}
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#sequenceNumberRestored
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#sequenceNumberUnknown
     */
    virtual void AddOutputData(
        long checkpointId,
        const omnistream::ResultSubpartitionInfoPOD& info,
        int startSeqNum,
        std::vector<omnistream::Buffer*>& data) = 0;

    /**
     * Add in-flight bufferFuture from the {@link
     * org.apache.flink.runtime.io.network.partition.ResultSubpartition ResultSubpartition}. Must be
     * called after {@link #start} and before {@link #finishOutput(long)}. Buffers are recycled
     * after they are written or exception occurs.
     *
     * <p>The method will be called when the unaligned checkpoint is enabled and received an aligned
     * barrier.
     */
    virtual void AddOutputDataFuture(
        long checkpointId,
        const omnistream::ResultSubpartitionInfoPOD& info,
        int startSeqNum,
        std::shared_ptr<CompletableFutureV2<std::vector<omnistream::Buffer*>>> data) = 0;

    /**
     * Finalize write of channel state data for the given checkpoint id. Must be called after {@link
     * #start(long, CheckpointOptions)} and all of the input data of the given checkpoint added.
     * When both {@link #finishInput} and {@link #finishOutput} were called the results can be
     * (eventually) obtained using {@link #getAndRemoveWriteResult}
     */
    virtual void FinishInput(long checkpointId) = 0;

    /**
     * Finalize write of channel state data for the given checkpoint id. Must be called after {@link
     * #start(long, CheckpointOptions)} and all of the output data of the given checkpoint added.
     * When both {@link #finishInput} and {@link #finishOutput} were called the results can be
     * (eventually) obtained using {@link #getAndRemoveWriteResult}
     */
    virtual void FinishOutput(long checkpointId) = 0;

    /**
     * Aborts the checkpoint and fails pending result for this checkpoint.
     *
     * @param cleanup true if {@link #getAndRemoveWriteResult(long)} is not supposed to be called
     *     afterwards.
     */
    virtual void Abort(long checkpointId, const std::exception_ptr& cause, bool cleanup) = 0;

    /**
     * Must be called after {@link #start(long, CheckpointOptions)} once.
     *
     * @throws IllegalArgumentException if the passed checkpointId is not known.
     */
    virtual std::shared_ptr<ChannelStateWriteResult> GetAndRemoveWriteResult(long checkpointId) = 0;
};

/** No-op implementation of {@link ChannelStateWriter}. */
class NoOpChannelStateWriter : public ChannelStateWriter {
public:
    static std::shared_ptr<NoOpChannelStateWriter> noOp;
    void open() override
    {
    }

    void Start(long checkpointId, const CheckpointOptions& checkpointOptions) override
    {
    }

    void AddInputData(
        long checkpointId,
        const omnistream::InputChannelInfo& info,
        int startSeqNum,
        std::vector<omnistream::Buffer*> data) override
    {
        for (omnistream::Buffer* buffer : data) {
            if (buffer == nullptr) {
                continue;
            }
            try {
                buffer->RecycleBuffer();
            } catch (const std::exception& e) {
                INFO_RELEASE(
                    "ERROR: Failed to recycle an input buffer discarded by NoOpChannelStateWriter: " << e.what());
            } catch (...) {
                INFO_RELEASE(
                    "ERROR: Failed to recycle an input buffer discarded by NoOpChannelStateWriter due to an unknown "
                    "exception");
            }
        }
    }

    void AddOutputData(
        long checkpointId,
        const omnistream::ResultSubpartitionInfoPOD& info,
        int startSeqNum,
        std::vector<omnistream::Buffer*>& data) override
    {
    }

    void AddOutputDataFuture(
        long checkpointId,
        const omnistream::ResultSubpartitionInfoPOD& info,
        int startSeqNum,
        std::shared_ptr<CompletableFutureV2<std::vector<omnistream::Buffer*>>> data) override
    {
    }

    void FinishInput(long checkpointId) override
    {
    }

    void FinishOutput(long checkpointId) override
    {
    }

    void Abort(long checkpointId, const std::exception_ptr& cause, bool cleanup) override
    {
    }

    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> GetAndRemoveWriteResult(long checkpointId) override
    {
        return ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    }
};

#endif // OMNISTREAM_CHANNELSTATEWRITER_H
