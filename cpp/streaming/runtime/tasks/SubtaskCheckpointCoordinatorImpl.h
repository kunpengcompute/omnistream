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

#ifndef FLINK_TNEL_SUBTASKCHECKPOINTCOORDINATORIMPL_H
#define FLINK_TNEL_SUBTASKCHECKPOINTCOORDINATORIMPL_H
#include <unordered_map>
#include <set>
#include <chrono>
#include <memory>
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "runtime/checkpoint/channel/ChannelStateWriterImpl.h"
#include "runtime/state/CheckpointStorage.h"
#include "runtime/state/CheckpointStorageWorkerView.h"
#include "mailbox/StreamTaskActionExecutor.h"
#include "streaming/runtime/io/checkpointing/BarrierAlignmentUtil.h"
#include "OperatorChain.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "streaming/runtime/tasks/AsyncCheckpointRunnable.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/io/network/api/CancelCheckpointMarker.h"
#include "runtime/jobgraph/OperatorID.h"
#include "runtime/jobgraph/JobVertexID.h"
#include "core/utils/function/Supplier.h"
#include "core/utils/threads/ExecutorService.h"
#include "runtime/execution/OmniEnvironment.h"
#include "SubtaskCheckpointCoordinator.h"
namespace omnistream::runtime {
class SubtaskCheckpointCoordinatorImpl : public omnistream::SubtaskCheckpointCoordinator {
public:
    SubtaskCheckpointCoordinatorImpl(
        std::shared_ptr<CheckpointStorage> checkpointStorage,
        std::shared_ptr<CheckpointStorageWorkerView> checkpointStorageView,
        std::string taskName,
        std::shared_ptr<omnistream::StreamTaskActionExecutor> actionExecutor,
        std::shared_ptr<omnistream::EnvironmentV2> env,
        bool unalignedCheckpointEnabled,
        bool enableCheckpointAfterTasksFinished,
        std::function<std::shared_ptr<CompletableFutureV2<void>>(std::shared_ptr<ChannelStateWriter>, long)>*
            prepareInputSnapshot,
        BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer)
        : SubtaskCheckpointCoordinatorImpl(
              checkpointStorage,
              checkpointStorageView,
              taskName,
              actionExecutor,
              env,
              unalignedCheckpointEnabled,
              enableCheckpointAfterTasksFinished,
              prepareInputSnapshot,
              defaultMaxRecordAbortedCheckpoints,
              registerTimer) {};

    SubtaskCheckpointCoordinatorImpl(
        std::shared_ptr<omnistream::CheckpointStorage> checkpointStorage,
        std::shared_ptr<CheckpointStorageWorkerView> checkpointStorageView,
        std::string taskName,
        std::shared_ptr<omnistream::StreamTaskActionExecutor> actionExecutor,
        std::shared_ptr<omnistream::EnvironmentV2> env,
        bool unalignedCheckpointEnabled,
        bool enableCheckpointAfterTasksFinished,
        std::function<std::shared_ptr<CompletableFutureV2<void>>(std::shared_ptr<ChannelStateWriter>, long)>*
            prepareInputSnapshot,
        int maxRecordAbortedCheckpoints,
        BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer)
        : SubtaskCheckpointCoordinatorImpl(
              checkpointStorageView,
              taskName,
              actionExecutor,
              env,
              prepareInputSnapshot,
              maxRecordAbortedCheckpoints,
              unalignedCheckpointEnabled
                  ? openChannelStateWriter(taskName, checkpointStorage, checkpointStorageView, env)
                  : NoOpChannelStateWriter::noOp,
              enableCheckpointAfterTasksFinished,
              registerTimer)
    {
        asyncOperationsThreadPool = std::make_shared<ExecutorService>(4);
        this->prepareInputSnapshot = prepareInputSnapshot;
    };

    SubtaskCheckpointCoordinatorImpl(
        std::shared_ptr<CheckpointStorageWorkerView> streamFactoryResolver,
        std::string taskName,
        std::shared_ptr<omnistream::StreamTaskActionExecutor> actionExecutor,
        std::shared_ptr<omnistream::EnvironmentV2> env,
        std::function<std::shared_ptr<CompletableFutureV2<void>>(std::shared_ptr<ChannelStateWriter>, long)>*
            prepareInputSnapshot,
        int maxRecordAbortedCheckpoints,
        std::shared_ptr<ChannelStateWriter> channelStateWriter,
        bool enableCheckpointAfterTasksFinished,
        BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer);

    ~SubtaskCheckpointCoordinatorImpl();

    void checkpointState(
        std::shared_ptr<CheckpointMetaData> metadata,
        std::shared_ptr<CheckpointOptions> options,
        std::shared_ptr<CheckpointMetricsBuilder> metrics,
        omnistream::OperatorChainV2* operatorChain,
        bool isTaskFinished,
        std::shared_ptr<omnistream::Supplier<bool>> isRunning) override;

    void InitInputsCheckpoint(long checkpointId, CheckpointOptions* options) override;

    enum class NotifyCheckpointOperation {
        COMPLETE,
        ABORT,
        SUBSUME
    };

    void Close();

    void Cancel();

    void notifyCheckpointComplete(
        long checkpointId, omnistream::OperatorChainV2* operatorChain, omnistream::Supplier<bool>* isRunning);

    void notifyCheckpointAborted(
        long checkpointId, omnistream::OperatorChainV2* operatorChain, omnistream::Supplier<bool>* isRunning);

    const unordered_map<long, AsyncCheckpointRunnable*>& GetCheckpoints() const;

    void notifyCheckpointSubsumed(
        long checkpointId, omnistream::OperatorChainV2* operatorChain, omnistream::Supplier<bool>* isRunning);

    // Abort a checkpoint due to a cancel-barrier (Flink 1.16.3).
    // This is a best-effort cleanup hook: it clears the storage cache, aborts channel-state writes,
    // records the checkpoint as aborted, and cancels any in-progress alignment timer.
    void AbortCheckpointOnBarrier(long checkpointId, const std::exception_ptr& cause);

    std::shared_ptr<CheckpointStorageWorkerView> getCheckpointStorage();

    std::shared_ptr<ChannelStateWriter> getChannelStateWriter() override;

private:
    class LimitedSizeSet : public std::set<long> {
    public:
        explicit LimitedSizeSet(size_t maxSize) : maxSize(maxSize)
        {
        }

        std::pair<iterator, bool> insert(const long& value)
        {
            if (this->size() >= maxSize) {
                this->erase(this->begin());
            }
            return std::set<long>::insert(value);
        };

    private:
        size_t maxSize;
    };

    class CachingCheckpointStorageWorkerView : public CheckpointStorageWorkerView {
    public:
        CachingCheckpointStorageWorkerView() = default;

        explicit CachingCheckpointStorageWorkerView(std::shared_ptr<CheckpointStorageWorkerView> delegate)
            : delegate(delegate)
        {
        }

        void clearCacheFor(long checkpointId)
        {
            // Async snapshots retain their own shared ownership after cache eviction.
            cache.erase(checkpointId);
        }

        std::shared_ptr<CheckpointStreamFactory> resolveCheckpointStorageLocation(
            int64_t checkpointId, std::shared_ptr<CheckpointStorageLocationReference> reference) override;

        CheckpointStateOutputStream* createTaskOwnedStateStream() override;

        CheckpointStateToolset* createTaskOwnedCheckpointStateToolset() override;

    private:
        std::unordered_map<long, std::shared_ptr<CheckpointStreamFactory>> cache;
        std::shared_ptr<CheckpointStorageWorkerView> delegate;
    };

    std::set<long> createAbortedCheckpointIds(int maxRecordAbortedCheckpoints);

    bool CheckAndClearAbortedStatus(long checkpointId);

    static void logCheckpointProcessingDelay(CheckpointMetaData* metadata);

    void registerAlignmentTimer(
        long checkpointId, omnistream::OperatorChainV2* operatorChain, CheckpointBarrier* checkpointBarrier);

    void CancelAlignmentTimer();

    void PrepareInflightDataSnapshot(long checkpointId);

    bool CancelAsyncCheckpointRunnable(long checkpointId);

    void RegisterAsyncCheckpointRunnable(int64_t checkpointId, AsyncCheckpointRunnable* asyncCheckpointRunnable);

    bool UnregisterAsyncCheckpointRunnable(long checkpointId);

    bool takeSnapshotSync(
        std::unordered_map<OperatorID, OperatorSnapshotFutures*>* operatorSnapshotsInProgress,
        std::shared_ptr<CheckpointMetaData> checkpointMetaData,
        std::shared_ptr<CheckpointMetricsBuilder> checkpointMetrics,
        std::shared_ptr<CheckpointOptions> checkpointOptions,
        omnistream::OperatorChainV2* operatorChain,
        std::shared_ptr<omnistream::Supplier<bool>> isRunning);

    void cleanup(
        std::unordered_map<OperatorID, OperatorSnapshotFutures*>* operatorSnapshotsInProgress,
        std::shared_ptr<CheckpointMetaData> metadata,
        std::shared_ptr<CheckpointMetricsBuilder> metrics,
        std::exception ex);

    void CloseQuietly(AsyncCheckpointRunnable* runnable)
    {
        try {
            if (runnable != nullptr) {
                runnable->Close();
            }
        } catch (const std::exception& e) {
            LogError("Exception while closing runnable: %s", e.what());
        }
    }

    void finishAndReportAsync(
        std::unordered_map<OperatorID, OperatorSnapshotFutures*>* operatorSnapshotsInProgress,
        std::shared_ptr<CheckpointMetaData> metadata,
        std::shared_ptr<CheckpointMetricsBuilder> metrics,
        bool istaskDeployedAsFinished,
        bool isTaskFinished,
        std::shared_ptr<omnistream::Supplier<bool>> isRunning,
        std::shared_ptr<CheckpointOptions> options);

    static std::shared_ptr<ChannelStateWriter> openChannelStateWriter(
        std::string taskName,
        std::shared_ptr<omnistream::CheckpointStorage> checkpointStorage,
        std::shared_ptr<omnistream::CheckpointStorageWorkerView> streamFactoryResolver,
        std::shared_ptr<omnistream::EnvironmentV2> env);

    void notifyCheckpoint(
        long checkpointId,
        omnistream::OperatorChainV2* operatorChain,
        omnistream::Supplier<bool>* isRunning,
        NotifyCheckpointOperation notifyCheckpointOperation);
    std::shared_ptr<CachingCheckpointStorageWorkerView> checkpointStorage;
    std::string taskName;
    void SetisRecoveredFlag(bool flag)
    {
        isRecovered_ = flag;
    }
    bool isRecovered_ = false;
    std::shared_ptr<omnistream::StreamTaskActionExecutor> actionExecutor;
    std::shared_ptr<omnistream::EnvironmentV2> env;
    std::function<std::shared_ptr<CompletableFutureV2<void>>(std::shared_ptr<ChannelStateWriter>, long)>*
        prepareInputSnapshot;
    std::shared_ptr<ChannelStateWriter> channelStateWriter;
    std::set<long> abortedCheckpointIds;
    bool enableCheckpointAfterTasksFinished;
    BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer;
    long lastCheckpointId;

    static const int defaultMaxRecordAbortedCheckpoints = 128;
    static const int checkpointExecutionDelayLogThresholdMs = 30000;
    long alignmentCheckpointId;
    BarrierAlignmentUtil::Cancellable* alignmentTimer = nullptr;
    std::mutex mutexLock;
    std::shared_ptr<ExecutorService> asyncOperationsThreadPool;
    bool closed = false;
    std::unordered_map<long, AsyncCheckpointRunnable*> checkpoints;
};
} // namespace omnistream::runtime
#endif // FLINK_TNEL_SUBTASKCHECKPOINTCOORDINATORIMPL_H
