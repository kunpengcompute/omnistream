/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAMTASK_H
#define OMNISTREAMTASK_H

#include <string>
#include <streaming/runtime/partitioner/V2/StreamPartitionerV2.h>
#include <taskmanager/OmniRuntimeEnvironment.h>
#include <runtime/io/network/api/writer/V2/RecordWriterDelegateV2.h>
#include <streaming/runtime/tasks/OperatorChain.h>
#include <streaming/runtime/partitioner/V2/KeyGroupStreamPartitionerV2.h>
#include <utils/monitormmap/SHMMetric.h>

#include "streaming/runtime/tasks/TimerService.h"
#include "streaming/runtime/tasks/SubtaskCheckpointCoordinator.h"
#include "runtime/jobgraph/tasks/CheckpointableTask.h"
#include "streaming/runtime/tasks/mailbox/MailboxProcessor.h"
#include "streaming/runtime/tasks/mailbox/StreamTaskActionExecutor.h"
#include "streaming/runtime/tasks/mailbox/TaskMailbox.h"
#include "streaming/runtime/io/OmniStreamInputProcessor.h"
#include "io/network/api/StopMode.h"
#include "streaming/runtime/tasks/SubtaskCheckpointCoordinatorImpl.h"
#include "runtime/state/CheckpointStorage.h"
#include "runtime/io/checkpointing/CheckpointBarrierHandler.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "runtime/checkpoint/SavepointType.h"

namespace omnistream {
class OperatorChainV2;
class OmniStreamTask : public std::enable_shared_from_this<OmniStreamTask>, public CheckpointableTask {
public:
    explicit OmniStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env, int taskType = 1);

    OmniStreamTask(
        std::shared_ptr<RuntimeEnvironmentV2>& env,
        std::shared_ptr<StreamTaskActionExecutor> actionExecutor,
        int taskType);
    OmniStreamTask(
        std::shared_ptr<RuntimeEnvironmentV2>& env,
        std::shared_ptr<StreamTaskActionExecutor> actionExecutor,
        TaskMailbox* mailbox,
        int taskType);

    virtual ~OmniStreamTask()
    {
        if (systemTimerService) {
            systemTimerService->shutdownService();
        }
        if (timerService) {
            timerService->shutdownService();
        }
        if (inputProcessor_ != nullptr) {
            delete inputProcessor_;
            inputProcessor_ = nullptr;
        }
    }

    // getter
    virtual const std::string getName() const;

    [[nodiscard]] std::shared_ptr<RuntimeEnvironmentV2> env() const
    {
        return env_;
    }

    virtual void postConstruct();
    // life cycle
    // in the most cases, it does not to be overridden
    void restore();

    void InjectChannelStateWriterIntoChannels();

    // in the most cases, it does not to be overridden
    void restoreInternal();

    virtual void cancel();

    // most case to be overrided by subclass
    virtual void init()
    {
    }

    void restoreGates();

    // life cycle the main task
    virtual void invoke();

    virtual void cleanup();

    void releaseOutputResource();

    shared_ptr<SubtaskCheckpointCoordinator>& GetSubtaskCheckpointCoordinator()
    {
        return subtaskCheckpointCoordinator;
    }

    void TriggerCheckpointOnBarrier(
        std::shared_ptr<CheckpointMetaData> checkpointMetaData,
        std::shared_ptr<CheckpointOptions> checkpointOptions,
        std::shared_ptr<CheckpointMetricsBuilder> checkpointMetrics) override
    {
        LOG(">>>>>>");
        try {
            PerformCheckpoint(checkpointMetaData, std::move(checkpointOptions), checkpointMetrics);
        } catch (const std::exception& e) {
            LOG("Operator " << getName() << " failed while performing checkpoint "
                            << checkpointMetaData->GetCheckpointId() << ": " << e.what());
            throw;
        } catch (...) {
            LOG("Operator " << getName() << " failed while performing checkpoint "
                            << checkpointMetaData->GetCheckpointId() << ": unknown exception");
            throw std::runtime_error("Unknown checkpoint failure");
        }
    };

    OmniStreamInputProcessor* input_processor() const
    {
        return inputProcessor_;
    }

    void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) override;

    bool IsUsingNonBlockingInput();
    // stream task specific
    virtual void processInput(MailboxDefaultAction::Controller* controller);

    void dispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString);

    void setMainOperator(StreamOperator* mainOperator)
    {
        mainOperator_ = mainOperator;
    }

    bool IsRunning()
    {
        return isRunning;
    }

    int getTaskType()
    {
        return taskType;
    }

    bool isCurrentSyncSavepoint(long checkpointId);
    void notifyCheckpointComplete(long checkpointId);
    std::shared_ptr<CompletableFutureV2<void>> notifyCheckpointCompleteAsync(long checkpointid);
    std::shared_ptr<CompletableFutureV2<void>> notifyCheckpointSubsumedAsync(long checkpointid);
    std::shared_ptr<CompletableFutureV2<void>> notifyCheckpointAbortAsync(
        long checkpointid, long latestCompletedCheckpointId);
    void triggerStopWithSavepoint(const std::shared_ptr<CheckpointOptions>& checkpointOptions);
    std::shared_ptr<CompletableFutureV2<bool>> triggerCheckpointAsync(
        std::shared_ptr<CheckpointMetaData> checkpointMetaData, std::shared_ptr<CheckpointOptions> checkpointOptions);
    StreamPartitionerV2<StreamRecord>* createPartitionerFromDesc(StreamPartitionerPOD partitioner);

    datastream::StreamPartitioner<IOReadableWritable>* createPartitionerFromDesc(
        const StreamEdgePOD& edge, bool recover = false);
    ProcessingTimeService* createProcessingTimeService();

protected:
    std::shared_ptr<RuntimeEnvironmentV2> env_;
    std::vector<OperatorConfig> operatorChainConfig_;

    // operator chain
    std::unique_ptr<OperatorChainV2> operatorChain;
    std::shared_ptr<RecordWriterDelegateV2> recordWriter_;
    StreamOperator* mainOperator_ = nullptr;
    OmniStreamInputProcessor* inputProcessor_ = nullptr;

    static ProcessingTimeCallback* deferCallbackToMailBox(
        shared_ptr<MailboxExecutor> mailboxExecutor, ProcessingTimeCallback* callback);

protected:
    /**
     * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another
     * thread) must be executed through this executor to ensure that we don't have concurrent method
     * calls that void consistent checkpoints. The execution will always be performed in the task
     * thread.
     *
     * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with {@link
     * StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor
     * SynchronizedStreamTaskActionExecutor} to provide lock to {@link SourceStreamTask}. */
    std::shared_ptr<StreamTaskActionExecutor> actionExecutor_;
    uint32_t numberOfInnerRecover;
    // mailbox loop
    TaskMailbox*
        mailbox_; // 负责存储相应 task 任务（也就是 mail），它支持多写单读，单线程读取并处理, delete by MailboxProcessor
    std::unique_ptr<MailboxProcessor>
        mailboxProcessor_; // MailBox 的核心处理线程，MailboxDefaultAction 是其默认的 action 实现
    std::shared_ptr<MailboxExecutor> mainMailboxExecutor_; // 它负责向 MailBox 提交 task 任务
    std::shared_ptr<runtime::TimerService> timerService;
    std::shared_ptr<runtime::TimerService> systemTimerService;

    TaskInformationPOD taskConfiguration_;

    // 1 native sql task, 2 native datastream task. 3 future - hybrid java+cpp source task
    int taskType;

    std::string taskName_;

    SHMMetric* shmmetric_processInput{};
    long int processInputCounter{};

    long int numberOfRows{0};

    long syncSavepoint = INT64_MIN;
    long finalCheckpointMinId = INT64_MIN;
    std::shared_ptr<CompletableFutureV2<void>> finalCheckpointCompleted = std::make_shared<CompletableFutureV2<void>>();

    long latestReportCheckpointId = -1;

    long latestAsyncCheckpointStartDelayNanos;

    void EndData(StopMode stopMode);
    virtual void AdvanceToEndOfEventTime() {
        // do nothing
    };
    void SetSynchronousSavepoint(long checkpointId)
    {
        if (syncSavepoint != INT64_MIN && syncSavepoint != checkpointId) {
            INFO_RELEASE(
                "Error SetSynchronousSavepoint at most one stop-with-savepoint checkpoint at a time is allowed");
            throw std::runtime_error(
                "SetSynchronousSavepoint at most one stop-with-savepoint checkpoint at a time is allowed");
        }
        syncSavepoint = checkpointId;
    }
    void DeclineCheckpoint(long checkpointId)
    {
        env_->DeclineCheckpoint(checkpointId);
    }
    virtual std::optional<CheckpointBarrierHandler*> GetCheckpointBarrierHandler()
    {
        LOG("This function should have been override!");
        return std::nullopt;
    }

private:
    // record writer
    std::shared_ptr<RecordWriterDelegateV2> createRecordWriterDelegate(
        TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment);

    std::vector<RecordWriterV2*> createRecordWriters(
        TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment);
    RecordWriterV2* createRecordWriter(
        StreamEdgePOD& edge,
        int outputIndex,
        std::shared_ptr<RuntimeEnvironmentV2> environment,
        std::string taskName,
        long bufferTimeout);

    template <typename K>
    KeySelector<K>* buildKeySelector(std::vector<KeyFieldInfoPOD>& keyFields);

    // mailbox
    void runMailboxLoop();

    /** Flags indicating the finished method of all the operators are called. */
    bool finishedOperators{false};
    // Flags indicating the close method of all the operators are called
    bool closedOperators_{false};

    std::atomic<bool> endOfDataReceived{false};

    // SubtaskCheckpointCoordinator
    std::shared_ptr<omnistream::SubtaskCheckpointCoordinator> subtaskCheckpointCoordinator;
    StateBackend* stateBackend = nullptr;
    std::shared_ptr<CheckpointStorage> checkpointStorage;
    bool isRunning;

    std::shared_ptr<CompletableFutureV2<void>> prepareInputSnapshot(
        std::shared_ptr<ChannelStateWriter> channelStateWriter, long checkpointID);

    std::shared_ptr<CheckpointStorage> createCheckpointStorage(StateBackend* backend);

    bool PerformCheckpoint(
        std::shared_ptr<CheckpointMetaData> checkpointMetaData,
        std::shared_ptr<CheckpointOptions> checkpointOptions,
        std::shared_ptr<CheckpointMetricsBuilder> checkpointMetrics);

    inline bool IsSynchronous(SnapshotType* checkpointType)
    {
        if (checkpointType == nullptr) {
            return false;
        }
        auto* savepointType = dynamic_cast<SavepointType*>(checkpointType);
        return checkpointType->IsSavepoint() && savepointType != nullptr && savepointType->isSynchronous();
    }

    std::shared_ptr<CompletableFutureV2<void>> notifyCheckpointOperation(
        std::function<void()> runnable, const std::string& description);

    inline bool AreCheckpointsWithFinishedTasksEnabled()
    {
        // TTODO: Add ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH and isCheckpointingEnabled to config
        return false;
    }
    bool TriggerCheckpointAsyncInMailbox(
        std::shared_ptr<CheckpointMetaData> checkpointMetaData, std::shared_ptr<CheckpointOptions> checkpointOptions);
    bool triggerUnfinishedChannelsCheckpoint(
        std::shared_ptr<CheckpointMetaData> checkpointMetaData, std::shared_ptr<CheckpointOptions> checkpointOptions);
};

class StreamTaskAction : public MailboxDefaultAction {
public:
    explicit StreamTaskAction(OmniStreamTask* task) : task_(task) {};
    ~StreamTaskAction() override = default;
    void runDefaultAction(Controller* controller) override
    {
        task_->processInput(controller);
    };

private:
    OmniStreamTask* task_;
};

class ResumeWrapper : public Runnable {
public:
    ResumeWrapper(
        std::shared_ptr<MailboxDefaultAction::Suspension> suspendedDefaultAction, std::shared_ptr<PeriodTimer> timer)
        : suspendedDefaultAction(std::move(suspendedDefaultAction)),
          timer(std::move(timer))
    {
        if (timer != nullptr) {
            timer->markStart();
        }
    }

    void run() override
    {
        if (timer != nullptr) {
            timer->markEnd();
        }
        suspendedDefaultAction->resume();
    }

private:
    std::shared_ptr<MailboxDefaultAction::Suspension> suspendedDefaultAction;
    std::shared_ptr<PeriodTimer> timer;
};
} // namespace omnistream

#endif // OMNISTREAMTASK_H
