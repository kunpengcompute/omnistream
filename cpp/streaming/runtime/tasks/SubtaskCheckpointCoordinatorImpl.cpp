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

#include <algorithm>
#include "SubtaskCheckpointCoordinatorImpl.h"
#include "core/include/common.h"
#include "runtime/io/network/api/CancelCheckpointMarker.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/taskmanager/OmniTask.h"
#include "core/include/common.h"

namespace omnistream::runtime {
std::set<long> SubtaskCheckpointCoordinatorImpl::createAbortedCheckpointIds(int maxRecordAbortedCheckpoints)
{
    return LimitedSizeSet(static_cast<size_t>(maxRecordAbortedCheckpoints));
}

bool SubtaskCheckpointCoordinatorImpl::CheckAndClearAbortedStatus(long checkpointId)
{
    if (abortedCheckpointIds.find(checkpointId) != abortedCheckpointIds.end()) {
        abortedCheckpointIds.erase(checkpointId);
        return true;
    }
    return false;
}

void SubtaskCheckpointCoordinatorImpl::logCheckpointProcessingDelay(CheckpointMetaData* metadata)
{
    long delay =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count() -
        metadata->GetReceiveTimestamp();
    if (delay >= checkpointExecutionDelayLogThresholdMs) {
        LOG("Time from receiving all checkpoint barriers/RPC to executing it exceeded threshold: " << delay << "ms");
    }
}

void SubtaskCheckpointCoordinatorImpl::registerAlignmentTimer(
    long checkpointId, omnistream::OperatorChainV2* operatorChain, CheckpointBarrier* checkpointBarrier)
{
    CancelAlignmentTimer();
    if (!checkpointBarrier->GetCheckpointOptions()->IsTimeoutable()) {
        return;
    }
    long timerDelay = BarrierAlignmentUtil::getTimerDelay(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count(),
        *checkpointBarrier);

    alignmentTimer = registerTimer->RegisterTask(
        std::function([this, operatorChain, checkpointId]() {
            try {
                operatorChain->AlignedBarrierTimeout(checkpointId);
            } catch (const std::exception& e) {
                LOG("Exception during alignment timer execution: " + std::string(e.what()));
            }
            if (alignmentTimer) {
                delete alignmentTimer;
            }
            this->alignmentTimer = nullptr;
        }),
        std::chrono::milliseconds(timerDelay));

    alignmentCheckpointId = checkpointId;
}

void SubtaskCheckpointCoordinatorImpl::CancelAlignmentTimer()
{
    if (alignmentTimer) {
        alignmentTimer->Cancel();
        delete alignmentTimer;
        alignmentTimer = nullptr;
    }
}

void SubtaskCheckpointCoordinatorImpl::PrepareInflightDataSnapshot(long checkpointId)
{
    auto future = (*prepareInputSnapshot)(channelStateWriter, checkpointId);
    if (future == nullptr) {
        channelStateWriter->FinishInput(checkpointId);
        return;
    }
    future->ThenRun([this, checkpointId, future]() mutable {
        try {
            future->Get();
            channelStateWriter->FinishInput(checkpointId);
        } catch (...) {
            auto ex = std::current_exception();
            channelStateWriter->Abort(checkpointId, ex, false);
        }
    });
}

bool SubtaskCheckpointCoordinatorImpl::CancelAsyncCheckpointRunnable(long checkpointId)
{
    lock_guard<std::mutex> lock(mutexLock);
    auto it = checkpoints.find(checkpointId);
    if (it != checkpoints.end()) {
        it->second->Close();
        checkpoints.erase(it);
        return true;
    }
    return false;
}

bool SubtaskCheckpointCoordinatorImpl::takeSnapshotSync(
    std::unordered_map<OperatorID, OperatorSnapshotFutures*>* operatorSnapshotsInProgress,
    std::shared_ptr<CheckpointMetaData> checkpointMetaData,
    std::shared_ptr<CheckpointMetricsBuilder> checkpointMetrics,
    std::shared_ptr<CheckpointOptions> checkpointOptions,
    omnistream::OperatorChainV2* operatorChain,
    std::shared_ptr<omnistream::Supplier<bool>> isRunning)
{
    LOG(">>>>>>>>>");
    if (operatorChain->IsClosed()) {
        THROW_RUNTIME_ERROR("OperatorChain and Task should never be closed at this point");
    }

    long checkpointId = checkpointMetaData->GetCheckpointId();
    long started =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
    auto channelStateWriteResult = checkpointOptions->NeedsChannelState()
                                       ? channelStateWriter->GetAndRemoveWriteResult(checkpointId)
                                       : ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();

    std::shared_ptr<CheckpointStreamFactory> storage =
        checkpointStorage->resolveCheckpointStorageLocation(checkpointId, checkpointOptions->GetTargetLocation());

    try {
        operatorChain->SnapshotState(
            operatorSnapshotsInProgress,
            *checkpointMetaData,
            checkpointOptions.get(),
            isRunning,
            channelStateWriteResult,
            storage,
            env->getTaskStateManager()->getOmniTaskBridge());
    } catch (const std::exception& e) {
        LOG("Error: sync snapshot failed, task=" << taskName << ", cp=" << checkpointId << ", error=" << e.what());
        checkpointStorage->clearCacheFor(checkpointId);
        throw;
    } catch (...) {
        LOG("Error: sync snapshot failed, task=" << taskName << ", cp=" << checkpointId << ", error=unknown");
        checkpointStorage->clearCacheFor(checkpointId);
        throw;
    }

    checkpointStorage->clearCacheFor(checkpointId);

    constexpr int nanoToMillis = 1000000;

    long syncDurationMillis =
        (std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
             .count() -
         started) /
        nanoToMillis;
    checkpointMetrics->SetSyncDurationMillis(syncDurationMillis);
    return true;
}

void SubtaskCheckpointCoordinatorImpl::cleanup(
    std::unordered_map<OperatorID, OperatorSnapshotFutures*>* operatorSnapshotsInProgress,
    std::shared_ptr<CheckpointMetaData> metadata,
    std::shared_ptr<CheckpointMetricsBuilder> operatorChain,
    std::exception ex)
{
    if (operatorSnapshotsInProgress == nullptr) {
        return;
    }
    try {
        channelStateWriter->Abort(metadata->GetCheckpointId(), std::make_exception_ptr(ex), true);
    } catch (const std::exception& e) {
        LOG("Could not properly abort channel state writer. " + std::string(e.what()));
    }
    for (auto& entry : *operatorSnapshotsInProgress) {
        OperatorSnapshotFutures* operatorSnapshotResult = entry.second;
        if (operatorSnapshotResult) {
            try {
                operatorSnapshotResult->cancel();
            } catch (const std::exception& e) {
                LOG("Could not poperly cancel an operator snapshot result. " + std::string(e.what()));
            }
            delete operatorSnapshotResult;
            entry.second = nullptr;
        }
    }
    delete operatorSnapshotsInProgress;
}

void SubtaskCheckpointCoordinatorImpl::finishAndReportAsync(
    std::unordered_map<OperatorID, OperatorSnapshotFutures*>* operatorSnapshotsInProgress,
    std::shared_ptr<CheckpointMetaData> metadata,
    std::shared_ptr<CheckpointMetricsBuilder> metrics,
    bool istaskDeployedAsFinished,
    bool isTaskFinished,
    std::shared_ptr<omnistream::Supplier<bool>> isRunning,
    std::shared_ptr<CheckpointOptions> options)
{
    LOG(">>>>>> isTaskDeployedAsFinished " << istaskDeployedAsFinished << " isTaskFinished " << isTaskFinished);
    AsyncCheckpointRunnable* asyncCheckpointRunnable = nullptr;
    try {
        auto unregister = std::make_unique<std::function<void(AsyncCheckpointRunnable*)>>(
            [this](AsyncCheckpointRunnable* asyncCheckpointRunnable) {
                this->UnregisterAsyncCheckpointRunnable(asyncCheckpointRunnable->GetCheckpointId());
            });
        auto asyncExceptionHandler = std::make_unique<std::function<void(std::string, std::exception)>>(
            [](std::string checkpointTaskName, std::exception e) {
                LOG("Async checkpoint exception in task " + checkpointTaskName + ": " + std::string(e.what()));
            });
        asyncCheckpointRunnable = new AsyncCheckpointRunnable(
            operatorSnapshotsInProgress,
            *metadata,
            *metrics,
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count(),
            taskName,
            unregister.get(),
            env,
            asyncExceptionHandler.get(),
            istaskDeployedAsFinished,
            isTaskFinished,
            isRunning);
        unregister.release();
        asyncExceptionHandler.release();
        const auto checkpointId = asyncCheckpointRunnable->GetCheckpointId();
        RegisterAsyncCheckpointRunnable(checkpointId, asyncCheckpointRunnable);
        // thread hold options
        asyncOperationsThreadPool->Execute([this, asyncCheckpointRunnable, checkpointId, options]() {
            try {
                asyncCheckpointRunnable->Run();
            } catch (const std::exception& e) {
                LogError("Exception in async checkpoint: %s", e.what());
                this->UnregisterAsyncCheckpointRunnable(checkpointId);
            } catch (...) {
                LogError("Unknown exception in async checkpoint");
                this->UnregisterAsyncCheckpointRunnable(checkpointId);
            }
            delete asyncCheckpointRunnable;
        });
    } catch (...) {
        if (asyncCheckpointRunnable != nullptr) {
            UnregisterAsyncCheckpointRunnable(asyncCheckpointRunnable->GetCheckpointId());
            delete asyncCheckpointRunnable;
        } else {
            cleanup(
                operatorSnapshotsInProgress,
                metadata,
                metrics,
                std::runtime_error("Failed to create async checkpoint runnable"));
        }
        throw;
    }
    LOG(">>>>> Done");
}

void SubtaskCheckpointCoordinatorImpl::RegisterAsyncCheckpointRunnable(
    int64_t checkpointId, AsyncCheckpointRunnable* asyncCheckpointRunnable)
{
    std::lock_guard<std::mutex> guard(mutexLock);
    if (closed) {
        std::cerr << "Cannot register runnable; coordinator is already closed. Closing runnable." << std::endl;
        CloseQuietly(asyncCheckpointRunnable);
        if (checkpoints.count(checkpointId) == 0) {
            throw std::runtime_error(
                "SubtaskCheckpointCoordinator was closed without releasing "
                "asyncCheckpointRunnable for checkpoint " +
                std::to_string(checkpointId));
        }
    } else if (checkpoints.count(checkpointId) > 0) {
        CloseQuietly(asyncCheckpointRunnable);
        std::stringstream ss;
        ss << "Cannot register runnable; async checkpoint " << checkpointId << " already registered.";
        throw std::runtime_error(ss.str());
    } else {
        LOG(">>>>>> checkpointId " << checkpointId << " AsyncCheckpointRunnable Registered");
        checkpoints[checkpointId] = asyncCheckpointRunnable;
    }
}

std::shared_ptr<ChannelStateWriter> SubtaskCheckpointCoordinatorImpl::openChannelStateWriter(
    std::string taskName,
    std::shared_ptr<omnistream::CheckpointStorage> checkpointStorage,
    std::shared_ptr<omnistream::CheckpointStorageWorkerView> streamFactoryResolver,
    std::shared_ptr<omnistream::EnvironmentV2> env)
{
    // JobIDPOD seems to be similar to JobVertexID. Remove one, then replace the temp JobVertexID with:
    //      env->taskConfiguration().jobConfiguration().getJobId()

    std::shared_ptr<ChannelStateWriterImpl> writer = std::make_shared<omnistream::ChannelStateWriterImpl>(
        omnistream::JobVertexID(0, 0),
        taskName,
        env->taskConfiguration().getIndexOfSubtask(),
        checkpointStorage,
        streamFactoryResolver);
    writer->open();
    return writer;
}

bool SubtaskCheckpointCoordinatorImpl::UnregisterAsyncCheckpointRunnable(long checkpointId)
{
    std::lock_guard<std::mutex> lock(mutexLock);
    return checkpoints.erase(checkpointId) > 0;
}

void SubtaskCheckpointCoordinatorImpl::checkpointState(
    std::shared_ptr<CheckpointMetaData> metadata,
    std::shared_ptr<CheckpointOptions> options,
    std::shared_ptr<CheckpointMetricsBuilder> metrics,
    omnistream::OperatorChainV2* operatorChain,
    bool isTaskFinished,
    std::shared_ptr<omnistream::Supplier<bool>> isRunning)
{
    LOG_DEBUG(">>>>>>> isTaskFinished? " << isTaskFinished);
    if (!metadata || !options || !metrics) {
        INFO_RELEASE("Error CheckpointMetaData or CheckpointOptions or CheckpointMetricsBuilder is null");
        THROW_LOGIC_EXCEPTION("CheckpointMetaData or CheckpointOptions or CheckpointMetricsBuilder is null");
    }

    if (lastCheckpointId >= metadata->GetCheckpointId()) {
        CheckAndClearAbortedStatus(metadata->GetCheckpointId());
        return;
    }

    logCheckpointProcessingDelay(metadata.get());

    lastCheckpointId = metadata->GetCheckpointId();
    if (CheckAndClearAbortedStatus(metadata->GetCheckpointId())) {
        operatorChain->broadcastEvent(
            std::make_shared<omnistream::CancelCheckpointMarker>(metadata->GetCheckpointId()));
        LOG("Checkpoint " + std::to_string(metadata->GetCheckpointId()) +
            " has been notified as aborted, would not trigger any checkpoint.");
        return;
    }

    if (options->GetAlignment() == CheckpointOptions::AlignmentType::FORCED_ALIGNED) {
        CheckpointOptions* unalignedSupportedOptions = options->WithUnalignedSupported();
        if (unalignedSupportedOptions != options.get()) {
            options.reset(unalignedSupportedOptions);
        }
        InitInputsCheckpoint(metadata->GetCheckpointId(), options.get());
    }

    // 预检查：对于 compatible savepoint，提前检查链中所有算子是否支持该格式。
    // 必须在 PrepareSnapshotPreBarrier / broadcastEvent 等有副作用的操作之前执行，
    // 避免预检查失败时已产生不可逆的算子状态变更。
    if (options->GetCheckpointType()->IsSavepoint()) {
        auto savepointType = dynamic_cast<SavepointType*>(options->GetCheckpointType());
        if (savepointType && savepointType->getFormatType() == SavepointFormatType::COMPATIBLE) {
            std::string unsupportedReasons = operatorChain->checkCompatibleSavepointSupport();
            if (!unsupportedReasons.empty()) {
                INFO_RELEASE(
                    "Error:SubtaskCheckpointCoordinatorImpl::checkpointState pre-check FAILED: "
                    "task="
                    << taskName << ", cp=" << metadata->GetCheckpointId() << ", reasons=[" << unsupportedReasons
                    << "]");
                auto* runtimeEnv = dynamic_cast<omnistream::RuntimeEnvironmentV2*>(env.get());
                if (runtimeEnv != nullptr && runtimeEnv->omniTask() != nullptr) {
                    std::runtime_error wrapped("Compatible savepoint is not supported: " + unsupportedReasons);
                    runtimeEnv->omniTask()->declineCheckpoint(
                        metadata->GetCheckpointId(), CheckpointFailureReason::CHECKPOINT_DECLINED, &wrapped);
                } else {
                    INFO_RELEASE(
                        "Error:SubtaskCheckpointCoordinatorImpl::checkpointState pre-check FAILED "
                        "but cannot declineCheckpoint: runtimeEnv or omniTask is null, "
                        "task="
                        << taskName << ", cp=" << metadata->GetCheckpointId());
                }
                return;
            }
        }
    }

    operatorChain->PrepareSnapshotPreBarrier(metadata->GetCheckpointId());

    CheckpointBarrier* checkpointBarrier =
        new CheckpointBarrier(metadata->GetCheckpointId(), metadata->GetTimestamp(), options);

    bool isPriorityEvent = options->IsUnalignedCheckpoint();
    if (isRecovered_) {
        INFO_RELEASE("source " << taskName);
        isPriorityEvent = false;
    }
    operatorChain->broadcastEvent(std::shared_ptr<omnistream::AbstractEvent>(checkpointBarrier), isPriorityEvent);

    if (options->NeedsChannelState()) {
        channelStateWriter->FinishOutput(metadata->GetCheckpointId());
    }

    std::unordered_map<OperatorID, OperatorSnapshotFutures*>* snapshotFutures =
        new std::unordered_map<OperatorID, OperatorSnapshotFutures*>();
    bool snapshotFuturesTransferred = false;
    try {
        if (takeSnapshotSync(snapshotFutures, metadata, metrics, options, operatorChain, isRunning)) {
            LOG_DEBUG("finishAndReportAsync start lastCheckpointId: " << lastCheckpointId);
            snapshotFuturesTransferred = true;
            finishAndReportAsync(
                snapshotFutures,
                metadata,
                metrics,
                operatorChain->IsTaskDeployedAsFinished(),
                isTaskFinished,
                isRunning,
                options);
            LOG_DEBUG("finishAndReportAsync end lastCheckpointId: " << lastCheckpointId);
        } else {
            cleanup(snapshotFutures, metadata, metrics, std::runtime_error("Checkpoint declined"));
            auto* runtimeEnv = dynamic_cast<omnistream::RuntimeEnvironmentV2*>(env.get());
            if (runtimeEnv != nullptr && runtimeEnv->omniTask() != nullptr) {
                std::runtime_error wrapped("Checkpoint declined before async report");
                runtimeEnv->omniTask()->declineCheckpoint(
                    metadata->GetCheckpointId(), CheckpointFailureReason::CHECKPOINT_DECLINED, &wrapped);
            }
        }
    } catch (const std::exception& e) {
        LOG("Error: checkpointState failed, task=" << taskName << ", cp=" << metadata->GetCheckpointId()
                                                   << ", error=" << e.what());
        if (!snapshotFuturesTransferred) {
            cleanup(snapshotFutures, metadata, metrics, e);
        }
        auto* runtimeEnv = dynamic_cast<omnistream::RuntimeEnvironmentV2*>(env.get());
        if (runtimeEnv != nullptr && runtimeEnv->omniTask() != nullptr) {
            std::runtime_error wrapped(std::string("Checkpoint sync failure: ") + e.what());
            runtimeEnv->omniTask()->declineCheckpoint(
                metadata->GetCheckpointId(), CheckpointFailureReason::CHECKPOINT_DECLINED, &wrapped);
        }
        throw;
    } catch (...) {
        LOG("Error: checkpointState failed, task=" << taskName << ", cp=" << metadata->GetCheckpointId()
                                                   << ", error=unknown");
        if (!snapshotFuturesTransferred) {
            cleanup(snapshotFutures, metadata, metrics, std::runtime_error("Unknown checkpoint failure"));
        }
        auto* runtimeEnv = dynamic_cast<omnistream::RuntimeEnvironmentV2*>(env.get());
        if (runtimeEnv != nullptr && runtimeEnv->omniTask() != nullptr) {
            std::runtime_error wrapped("Unknown checkpoint sync failure");
            runtimeEnv->omniTask()->declineCheckpoint(
                metadata->GetCheckpointId(), CheckpointFailureReason::CHECKPOINT_DECLINED, &wrapped);
        }
        throw;
    }
}

SubtaskCheckpointCoordinatorImpl::~SubtaskCheckpointCoordinatorImpl()
{
    if (asyncOperationsThreadPool) {
        asyncOperationsThreadPool.reset();
    }
    if (alignmentTimer) {
        delete alignmentTimer;
    }
    if (registerTimer) {
        delete registerTimer;
    }
    if (prepareInputSnapshot) {
        delete prepareInputSnapshot;
    }
    for (auto pair : checkpoints) {
        if (pair.second) {
            delete pair.second;
        }
    }
    checkpoints.clear();
}

void SubtaskCheckpointCoordinatorImpl::InitInputsCheckpoint(long checkpointId, CheckpointOptions* options)
{
    if (options->IsUnalignedCheckpoint()) {
        channelStateWriter->Start(checkpointId, *options);
        PrepareInflightDataSnapshot(checkpointId);
    } else if (options->IsTimeoutable()) {
        channelStateWriter->Start(checkpointId, *options);
        channelStateWriter->FinishInput(checkpointId);
    }
}

SubtaskCheckpointCoordinatorImpl::SubtaskCheckpointCoordinatorImpl(
    std::shared_ptr<CheckpointStorageWorkerView> checkpointStorage,
    std::string taskName,
    std::shared_ptr<omnistream::StreamTaskActionExecutor> actionExecutor,
    std::shared_ptr<omnistream::EnvironmentV2> env,
    std::function<std::shared_ptr<CompletableFutureV2<void>>(std::shared_ptr<ChannelStateWriter>, long)>*
        prepareInputSnapshot,
    int maxRecordAbortedCheckpoints,
    std::shared_ptr<ChannelStateWriter> channelStateWriter,
    bool enableCheckpointAfterTasksFinished,
    BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer)
    : checkpointStorage(make_shared<CachingCheckpointStorageWorkerView>(checkpointStorage)),
      taskName(taskName),
      actionExecutor(actionExecutor),
      env(env),
      prepareInputSnapshot(prepareInputSnapshot),
      channelStateWriter(channelStateWriter),
      abortedCheckpointIds(createAbortedCheckpointIds(maxRecordAbortedCheckpoints)),
      enableCheckpointAfterTasksFinished(enableCheckpointAfterTasksFinished),
      registerTimer(registerTimer),
      lastCheckpointId(-1)
{
}

std::shared_ptr<CheckpointStreamFactory>
SubtaskCheckpointCoordinatorImpl::CachingCheckpointStorageWorkerView::resolveCheckpointStorageLocation(
    int64_t checkpointId, std::shared_ptr<CheckpointStorageLocationReference> reference)
{
    auto it = cache.find(checkpointId);
    if (it != cache.end()) {
        return it->second;
    }
    try {
        std::shared_ptr<CheckpointStreamFactory> factory =
            delegate->resolveCheckpointStorageLocation(checkpointId, reference);
        cache[checkpointId] = factory;
        return factory;
    } catch (const std::exception& e) {
        INFO_RELEASE("Exception during resolveCheckpointStorageLocation: " + std::string(e.what()));
        throw std::runtime_error(e.what());
    }
}

CheckpointStateOutputStream*
SubtaskCheckpointCoordinatorImpl::CachingCheckpointStorageWorkerView::createTaskOwnedStateStream()
{
    return delegate->createTaskOwnedStateStream();
}

CheckpointStateToolset*
SubtaskCheckpointCoordinatorImpl::CachingCheckpointStorageWorkerView::createTaskOwnedCheckpointStateToolset()
{
    return delegate->createTaskOwnedCheckpointStateToolset();
}

void SubtaskCheckpointCoordinatorImpl::notifyCheckpointComplete(
    long checkpointId, omnistream::OperatorChainV2* operatorChain, omnistream::Supplier<bool>* isRunning)
{
    notifyCheckpoint(checkpointId, operatorChain, isRunning, NotifyCheckpointOperation::COMPLETE);
}

void SubtaskCheckpointCoordinatorImpl::notifyCheckpointAborted(
    long checkpointId, omnistream::OperatorChainV2* operatorChain, omnistream::Supplier<bool>* isRunning)
{
    notifyCheckpoint(checkpointId, operatorChain, isRunning, NotifyCheckpointOperation::ABORT);
}

void SubtaskCheckpointCoordinatorImpl::notifyCheckpointSubsumed(
    long checkpointId, omnistream::OperatorChainV2* operatorChain, omnistream::Supplier<bool>* isRunning)
{
    notifyCheckpoint(checkpointId, operatorChain, isRunning, NotifyCheckpointOperation::SUBSUME);
}

void SubtaskCheckpointCoordinatorImpl::AbortCheckpointOnBarrier(long checkpointId, const std::exception_ptr& cause)
{
    // - update lastCheckpointId
    // - prune aborted ids below lastCheckpointId
    // - record this aborted checkpoint id
    // - clear storage cache
    // - abort channel-state writer and clean up
    // - cancel any in-progress alignment timer for this checkpoint
    lastCheckpointId = std::max(lastCheckpointId, checkpointId);

    for (auto it = abortedCheckpointIds.begin(); it != abortedCheckpointIds.end();) {
        if (*it < lastCheckpointId) {
            it = abortedCheckpointIds.erase(it);
        } else {
            break;
        }
    }
    abortedCheckpointIds.insert(checkpointId);

    if (checkpointStorage) {
        checkpointStorage->clearCacheFor(checkpointId);
    }

    if (channelStateWriter) {
        channelStateWriter->Abort(checkpointId, cause, true);
    }

    try {
        if (env && env->getTaskStateManager()) {
            env->getTaskStateManager()->NotifyCheckpointAbortedV2(checkpointId);
        }
    } catch (...) {
        // Best-effort.
    }

    if (checkpointId == alignmentCheckpointId) {
        CancelAlignmentTimer();
    }
}

void SubtaskCheckpointCoordinatorImpl::notifyCheckpoint(
    long checkpointId,
    omnistream::OperatorChainV2* operatorChain,
    omnistream::Supplier<bool>* isRunning,
    NotifyCheckpointOperation notifyCheckpointOperation)
{
    std::exception_ptr previousException = nullptr;
    try {
        if (isRunning->get()) {
            if (notifyCheckpointOperation == NotifyCheckpointOperation::ABORT) {
                bool canceled = CancelAsyncCheckpointRunnable(checkpointId);
                if (!canceled && checkpointId > lastCheckpointId) {
                    abortedCheckpointIds.insert(checkpointId);
                }

                channelStateWriter->Abort(
                    checkpointId,
                    std::make_exception_ptr(std::runtime_error("checkpoint aborted via notification")),
                    false);
            }

            try {
                switch (notifyCheckpointOperation) {
                    case NotifyCheckpointOperation::ABORT: operatorChain->NotifyCheckpointAborted(checkpointId); break;
                    case NotifyCheckpointOperation::COMPLETE:
                        operatorChain->NotifyCheckpointComplete(checkpointId);
                        break;
                    case NotifyCheckpointOperation::SUBSUME:
                        operatorChain->NotifyCheckpointSubsumed(checkpointId);
                        break;
                }
            } catch (...) {
                previousException = std::current_exception();
            }
        }
    } catch (...) {
        // No catch logic
    }

    try {
        switch (notifyCheckpointOperation) {
            case NotifyCheckpointOperation::ABORT:
                env->getTaskStateManager()->NotifyCheckpointAbortedV2(checkpointId);
                break;
            case NotifyCheckpointOperation::COMPLETE:
                env->getTaskStateManager()->NotifyCheckpointCompleteV2(checkpointId);
                break;
            default: break;
        }
    } catch (...) {
        previousException = std::current_exception();
    }
}

std::shared_ptr<CheckpointStorageWorkerView> SubtaskCheckpointCoordinatorImpl::getCheckpointStorage()
{
    return checkpointStorage;
}

std::shared_ptr<ChannelStateWriter> SubtaskCheckpointCoordinatorImpl::getChannelStateWriter()
{
    return channelStateWriter;
}

void SubtaskCheckpointCoordinatorImpl::Close()
{
    CancelAlignmentTimer();
    Cancel();
}

void SubtaskCheckpointCoordinatorImpl::Cancel()
{
    std::vector<AsyncCheckpointRunnable*> asyncCheckpointRunnables;
    std::unique_lock<std::mutex> lock(mutexLock);
    if (!closed) {
        closed = true;
        for (const auto& pair : checkpoints) {
            asyncCheckpointRunnables.push_back(pair.second);
        }
        checkpoints.clear();
    }

    // Close all runnables quietly
    for (auto runnable : asyncCheckpointRunnables) {
        CloseQuietly(runnable);
    }

    try {
        if (channelStateWriter) {
        }
    } catch (const std::exception& e) {
        LogError("Failed to close channelStateWriter: %s", e.what());
    }
}

const unordered_map<long, AsyncCheckpointRunnable*>& SubtaskCheckpointCoordinatorImpl::GetCheckpoints() const
{
    return checkpoints;
}
} // namespace omnistream::runtime
