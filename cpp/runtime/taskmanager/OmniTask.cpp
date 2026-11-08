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

#include "OmniTask.h"

#include <stdexcept>
#include <thread>
#include <partition/consumer/SingleInputGate.h>
#include <streaming/runtime/tasks/omni/OmniOneInputStreamTask.h>
#include <streaming/runtime/tasks/omni/OmniTwoInputStreamTask.h>
#include <streaming/runtime/tasks/omni/OmniSourceOperatorStreamTask.h>
#include <streaming/runtime/tasks/omni/OmniSourceStreamTask.h>
#include "common.h"
#include "runtime/buffer/ObjectSegment.h"
#include "OmniRuntimeEnvironment.h"
#include "partition/consumer/RemoteInputChannel.h"
#include "partition/ResultPartitionManager.h"
#include "io/network/netty/OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/io/network/OmniShuffleEnvironment.h"
#include "runtime/io/network/netty/GlobalNettyBufferPool.h"
#include "runtime/partition/PartitionNotFoundException.h"
#include "streaming/runtime/tasks/SubtaskCheckpointCoordinatorImpl.h"
#include "core/utils/exception/ScopeGuard.h"

namespace omnistream {
OmniTask::OmniTask(
    JobInformationPOD jobInfo,
    TaskInformationPOD taskInfo,
    TaskDeploymentDescriptorPOD taskDeploymentDescriptor,
    std::shared_ptr<ShuffleEnvironment> shuffleEnvironment,
    std::shared_ptr<OmniTaskBridge> omni_task_bridge,
    std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
    std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
    : OmniTask(
          jobInfo,
          taskInfo,
          taskDeploymentDescriptor,
          shuffleEnvironment,
          nullptr,
          omni_task_bridge,
          taskOperatorEventGatewayBridge,
          remoteDataFetcherBridge)
{
}

OmniTask::OmniTask(
    std::shared_ptr<TaskStateManagerBridge> stateBridge,
    std::shared_ptr<OmniTaskBridge> omni_task_bridge,
    std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
    std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
    : taskStateManagerBridge_(stateBridge),
      omni_task_bridge(omni_task_bridge),
      taskOperatorEventGatewayBridge_(taskOperatorEventGatewayBridge),
      remoteDataFetcherBridge_(remoteDataFetcherBridge)
{
}

OmniTask::OmniTask(
    JobInformationPOD jobInfo,
    TaskInformationPOD taskInfo,
    TaskDeploymentDescriptorPOD tdd,
    std::shared_ptr<ShuffleEnvironment> shuffleEnv,
    std::shared_ptr<TaskStateManagerBridge> taskStateManagerBridge,
    std::shared_ptr<OmniTaskBridge> omni_task_bridge,
    std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
    std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
    : jobInfo_(jobInfo),
      taskInfo_(taskInfo),
      taskDeploymentDescriptor_(tdd),
      shuffleEnv_(shuffleEnv),
      taskStateManagerBridge_(taskStateManagerBridge),
      omni_task_bridge(omni_task_bridge),
      taskOperatorEventGatewayBridge_(taskOperatorEventGatewayBridge),
      remoteDataFetcherBridge_(remoteDataFetcherBridge)
{
    LOG_INFO_IMP(">>>> Task Init");
    LOG_INFO_IMP(">>>> Shuffle Env address" << reinterpret_cast<long>(shuffleEnv_.get()));

    this->taskNameWithSubtask_ = taskInfo.getTaskName() + "#" + std::to_string(taskInfo.getIndexOfSubtask());
    this->executionId_ = ExecutionAttemptIDPOD(); // need to be know where to get it.
    this->allocationId_ = AbstractIDPOD();        //// need to be know where to get it.
    this->taskType = taskInfo.GetTaskType();
    originalNetworkBufferRecycler_ = std::make_shared<OriginalNetworkBufferRecycler>();
    LOG_INFO_IMP("[TaskState][Start]  Task Name : [" << taskNameWithSubtask_ << "]");

    taskPlainInfo_ = TaskPlainInfoPOD(
        taskPlainInfo_.getTaskName(),
        taskNameWithSubtask_,
        allocationId_.toString(),
        taskInfo.getMaxNumberOfSubtasks(),
        taskInfo.getMaxNumberOfSubtasks(),
        taskInfo.getNumberOfSubtasks(),
        0);

    LOG_INFO_IMP(">>>> create the reader and writer structures");
    auto taskNameWithSubtaskAndId = taskNameWithSubtask_ + " (" + executionId_.toString() + ')';

    auto ShuffleIOOwnerContext =
        shuffleEnv->createShuffleIOOwnerContext(taskNameWithSubtaskAndId, executionId_, nullptr);

    auto resultPartitionWriters =
        shuffleEnv_->createResultPartitionWriters(ShuffleIOOwnerContext, tdd.getProducedPartitions(), taskType);

    consumableNotifyingPartitionWriters = resultPartitionWriters;

    LOG_INFO_IMP(">>>> create the InputGates");
    inputGates = shuffleEnv_->createInputGates(ShuffleIOOwnerContext, nullptr, tdd.getInputGates(), taskType);
    taskMetricGroup = std::make_shared<TaskMetricGroup>();
    // setup runtime env
    TaskPlainInfoPOD taskPlainInfo;
    LOG("TaskPlainInfoPOD : " << taskPlainInfo.toString());
    INFO_RELEASE(" setupStramTask: " << taskNameWithSubtask_ << " taskType: " << taskType);

    auto writers = this->consumableNotifyingPartitionWriters;
    std::vector<std::shared_ptr<IndexedInputGate>> input_gates;

    for (auto& gate : this->inputGates) {
        input_gates.push_back(gate);
    }
    runtimeEnv = std::make_shared<RuntimeEnvironmentV2>(
        shuffleEnv_,
        taskInfo_,
        jobInfo_,
        taskPlainInfo,
        executionId_,
        writers,
        input_gates,
        this,
        taskMetricGroup,
        taskStateManagerBridge_,
        taskOperatorEventGatewayBridge_,
        omni_task_bridge,
        taskDeploymentDescriptor_);

    // only for datastream to bind core
    int32_t strategy_id = std::stoi(taskInfo_.getStreamConfigPOD().getOmniConf()["omni.bindcore.strategy"]);
    strategy = static_cast<omnistream::BindCoreStrategy>(strategy_id);
    BindCoreManager::GetInstance()->SetBindStrategy(strategy);
}

OmniTask::~OmniTask()
{
    // Logged on both sides of the member teardown: the "begin" line alone would only prove the
    // destructor was entered, the "end" line proves it ran to completion.
    INFO_RELEASE("~OmniTask begin " << this << " name: " << taskNameWithSubtask_);
    // Running totals for the StreamRecord leak hunt. The last task to tear down carries the
    // whole-job figures; stored-drained is the count nothing can ever free.
    ObjectSegment::reportCounters("~OmniTask");
    consumableNotifyingPartitionWriters.clear();
    inputGates.clear();
    // The remote input channels hold a shared_ptr to this bridge and outlive the task, so its
    // destructor will not run here. Release the JNI global reference on the Java RemoteDataFetcher
    // explicitly; without it that object -- and the input gate and OmniTask reachable from it --
    // can never be collected. Late resumeConsumption calls from a still-draining channel find a
    // null reference and return without doing anything.
    if (remoteDataFetcherBridge_ != nullptr) {
        remoteDataFetcherBridge_->ReleaseJavaRemoteDataFetcher();
    }
    // invokable_.reset();
    // use_count > 1 here means something outside this task still holds the OmniStreamTask, and its
    // record writers / output flusher threads cannot be released.
    INFO_RELEASE(
        "~OmniTask end " << this << " name: " << taskNameWithSubtask_
                         << " invokable_ use_count=" << invokable_.use_count());
}

std::shared_ptr<RuntimeEnvironmentV2> OmniTask::getRuntimeEnv()
{
    return runtimeEnv;
}

void OmniTask::closeRocksDBSharedResources()
{
    if (runtimeEnv) {
        runtimeEnv->setRocksDBSharedResources(nullptr);
    }
}

void OmniTask::cancel()
{
    if (invokable_ != nullptr) {
        invokable_->cancel();
    }
}

uintptr_t OmniTask::setupStreamTask(std::string streamClassName)
{
    try {
        LOG("streamk task class name: " << streamClassName);

        LOG("runtime env is ready");
        LOG("runtime env :" << runtimeEnv->toString());

        if (streamClassName == SOURCE_OPERATOR_STREAM_TASK) {
            LOG("prepare to create SOURCE_STREAM_TASK ");
            this->invokable_ = std::make_shared<OmniSourceOperatorStreamTask>(runtimeEnv, taskType);
            this->invokable_->postConstruct();

            LOG("After to create SOURCE_OPERATOR_STREAM_TASK ");
            auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
            LOG(">>>> rawStreamTask_ : " << rawStreamTask_);
            return rawStreamTask_;
        } else if (streamClassName == SOURCE_STREAM_TASK) {
            LOG("prepare to create SOURCE_STREAM_TASK ");
            this->invokable_ = std::make_shared<OmniSourceStreamTask>(runtimeEnv, std::make_unique<Object>(), taskType);
            this->invokable_->postConstruct();

            LOG("After to create SOURCE_STREAM_TASK ");
            auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
            LOG(">>>> rawStreamTask_ : " << rawStreamTask_);
            return rawStreamTask_;
        } else if (streamClassName == ONEINTPUT_STREAM_TASK) {
            LOG("prepare to create ONEINTPUT_STREAM_TASK ");
            this->invokable_ = std::make_shared<OmniOneInputStreamTask>(runtimeEnv, taskType);
            this->invokable_->postConstruct();

            LOG("After to create ONEINTPUT_STREAM_TASK ");
            auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
            LOG(">>>> rawStreamTask_ : " << rawStreamTask_);
            return rawStreamTask_;
        } else if (streamClassName == TWOINTPUT_STREAM_TASK) {
            LOG("prepare to create ONEINTPUT_STREAM_TASK ");
            this->invokable_ = std::make_shared<OmniTwoInputStreamTask>(runtimeEnv, taskType);
            this->invokable_->postConstruct();

            LOG("After to create ONEINTPUT_STREAM_TASK ");
            auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
            LOG(">>>> rawStreamTask_ : " << rawStreamTask_);
            return rawStreamTask_;
        } else {
            LOG(">>>> rawStreamTask_ : not exist ");
            return 0;
        }
    } catch (...) {
        LOG("Error: failed to create stream task..........................");
        GErrorLog("Error: failed to create stream task..........................");
    }
    return 0;
}

void OmniTask::DoRunRestore(long streamTaskAddress)
{
    INFO_RELEASE(" DoRunRestore starting: " << taskNameWithSubtask_);

    LOG_INFO_IMP("doRun.... ");
    LOG("now oper is :" << taskNameWithSubtask_);
    LOG("setup result partition and inputgate ");

    setupPartitionsAndGates(consumableNotifyingPartitionWriters, inputGates);
    // set metricGroup for ResultPartition
    for (auto& partitionWriter : consumableNotifyingPartitionWriters) {
        std::shared_ptr<AbstractMetricGroup> metricGroup =
            taskMetricGroup->GetTaskIOMetricGroup()->GetChildGroup("VectorBatchBufferPoolMetricGroup");
        partitionWriter->SetMetricGroup(metricGroup);
    }
    try {
        INFO_RELEASE(" OmniTask::DoRunRestore Invokable restore before");
        this->invokable_->restore();
        INFO_RELEASE(" OmniTask::DoRunRestore Invokable restore after");
        flag.store(true);
        INFO_RELEASE("find OmniTask initialized, task name: " << taskNameWithSubtask_);

        // init remote fetcher here because, the channels have been created and restored
        if (remoteDataFetcherBridge_ != nullptr) {
            remoteDataFetcherBridge_->InitCppRemoteInputChannel(this->inputGates);
        }
    } catch (const PartitionNotFoundException& e) {
        THROW_LOGIC_EXCEPTION("PartitionNotFoundException during restore: " << e.what());
    } catch (const std::exception& e) {
        THROW_RUNTIME_ERROR("std::exception during restore: " << e.what());
    } catch (...) {
        THROW_RUNTIME_ERROR("unknown error during restore");
    }
}

void OmniTask::doRun(long streamTaskAddress)
{
    INFO_RELEASE(" OmniTask::doRun welcome to native");
    INFO_RELEASE("doRun starting, taskNameWithSubtask: " << taskNameWithSubtask_);

    LOG_INFO_IMP("doRun.... ");
    LOG("now oper is :" << taskNameWithSubtask_);
    LOG("setup result partition and inputgate ");

    setupPartitionsAndGates(consumableNotifyingPartitionWriters, inputGates);

    try {
        INFO_RELEASE("OmniTask::doRun Invokable restore");
        this->invokable_->restore();

        LOG_INFO_IMP("OmniTask::doRun Invokable Invoke");

        this->invokable_->invoke();
    } catch (const PartitionNotFoundException& e) {
        INFO_RELEASE("Error:PartitionNotFoundException causes the task to stop and will do cleanup");
        throw;
    } catch (const std::exception& e) {
        INFO_RELEASE("Error:std::exception during restore or invoke" << e.what());
        throw;
    } catch (...) {
        INFO_RELEASE("Error:unknown exception during restore or invoke, taskName=");
        throw;
    }

    // ----------------------------------------------------------------
    //  finalization of a successful execution
    // ----------------------------------------------------------------

    // finish the produced partitions. if this fails, we consider the execution failed.
    for (auto& partitionWriter : consumableNotifyingPartitionWriters) {
        if (partitionWriter) {
            LOG_INFO_IMP(partitionWriter->toString() << "Finished");
            partitionWriter->finish();
        } else {
            LOG_INFO_IMP("Error: partitionWriter is null");
        }
    }

    LOG_INFO_IMP("[TaskState][End]  Task Name : [" << taskNameWithSubtask_ << "]");

    INFO_RELEASE(" doRun ending: " << taskNameWithSubtask_);

    LOG_INFO_IMP("Invokable Invoke");
    this->invokable_->cleanup();

    // sleep for a while
    // std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

void OmniTask::DoRunInvoke(long streamTaskAddress)
{
    int count = 0;
    while (!flag.load()) {
        INFO_RELEASE("find OmniTask still uninitialzed, task name : " << taskNameWithSubtask_);
        count++;
        if (count > 5) {
            break;
        }
        sleep(5);
    }

    // RAII clean up logic
    ScopeGuard cleanUpGuard([this]() {
        INFO_RELEASE("OmniTask::DoRunInvoke ending, start to clean up, task name: " << taskNameWithSubtask_);
        if (invokable_ != nullptr) {
            invokable_->cleanup();
        }
        this->ReleaseResources();
        if (originalNetworkBufferRecycler_ != nullptr) {
            originalNetworkBufferRecycler_->stop();
        }
    });

    try {
        INFO_RELEASE("OmniTask::DoRunInvoke welcome to native, task name : " << taskNameWithSubtask_);
        this->invokable_->invoke();
    } catch (const PartitionNotFoundException& e) {
        THROW_LOGIC_EXCEPTION("PartitionNotFoundException during invoke: " << e.what());
    } catch (const std::exception& e) {
        THROW_RUNTIME_ERROR("std::exception during invoke: " << e.what());
    } catch (...) {
        THROW_RUNTIME_ERROR("unknown error during invoke");
    }

    // ----------------------------------------------------------------
    //  finalization of a successful execution
    // ----------------------------------------------------------------

    // finish the produced partitions. if this fails, we consider the execution failed.
    for (auto& partitionWriter : consumableNotifyingPartitionWriters) {
        if (partitionWriter) {
            LOG_INFO_IMP(partitionWriter->toString() << "Finished");
            partitionWriter->finish();
        } else {
            LOG_INFO_IMP("Error: partitionWriter is null");
        }
    }
    INFO_RELEASE("OmniTask::DoRunInvoke ending, task name: " << taskNameWithSubtask_);
}

void OmniTask::setupPartitionsAndGates(
    std::vector<std::shared_ptr<ResultPartitionWriter>>& producedPartitions,
    std::vector<std::shared_ptr<SingleInputGate>>& inputGates)
{
    auto omniShuffleEnv = std::dynamic_pointer_cast<OmniShuffleEnvironment>(this->shuffleEnv_);
    std::shared_ptr<ResultPartitionManager> resultPartitionManager =
        omniShuffleEnv ? omniShuffleEnv->getResultPartitionManager() : nullptr;

    for (auto& producedPartition : producedPartitions) {
        producedPartition->setup();
        if (resultPartitionManager != nullptr) {
            // Bind this task to the partition so the manager can delete the task once every partition
            // it produces has been consumed by all downstream tasks.
            resultPartitionManager->bindOwningTask(producedPartition->getPartitionId(), this);
        }
    }
    LOG("producedPartition after setup");
    for (auto& inputGate : inputGates) {
        inputGate->setup();
    }
    LOG("inputGate after setup");
}

bool OmniTask::NotifyRunFinished()
{
    auto omniShuffleEnv = std::dynamic_pointer_cast<OmniShuffleEnvironment>(this->shuffleEnv_);
    if (omniShuffleEnv == nullptr) {
        return false;
    }
    // Deletes this task when all of its partitions have already been consumed. Nothing may touch the
    // task after this call.
    return omniShuffleEnv->getResultPartitionManager()->onTaskRunFinished(this);
}

void OmniTask::ReleaseResources()
{
    if (this->IsCanceledOrFailed()) {
        FailAllResultPartitions();
    }
    CloseAllResultPartitions();
    CloseAllInputGates();
}

void OmniTask::FailAllResultPartitions()
{
}
bool OmniTask::IsCanceledOrFailed()
{
    return this->executionState == ExecutionState::CREATED || this->executionState == ExecutionState::CANCELED ||
           this->executionState == ExecutionState::FAILED;
}

void OmniTask::CloseAllResultPartitions()
{
    for (auto& ResultPartitionWriter : consumableNotifyingPartitionWriters) {
        try {
            LOG("close the ResultPartitionWriter here at closeAllResultPartitions ");
            ResultPartitionWriter->close();
        } catch (...) {
            INFO_RELEASE("Failed to release result partition for task");
        }
    }
}

void OmniTask::CloseAllInputGates()
{
    if (invokable_ == nullptr || invokable_->IsUsingNonBlockingInput() == false) {
        for (auto& inputGate : inputGates) {
            try {
                inputGate->close();
            } catch (...) {
                LOG("Failed to release input gate for task");
            }
        }
    }
}

void OmniTask::notifyRemoteDataAvailable(
    int inputGateIndex,
    int channelIndex,
    long bufferAddress,
    int bufferLength,
    int readIndex,
    int memorySegmentOffset,
    int sequenceNumber,
    bool isBuffer,
    int bufferType)
{
    LOG("notifyRemoteDataAvailable");
    auto inputGate = inputGates[inputGateIndex];
    auto channel = inputGate->getChannel(channelIndex);
    if (auto remoteChannel = std::dynamic_pointer_cast<RemoteInputChannel>(channel)) {
        if (taskType == 1 && isBuffer) {
            if (readIndex != 0 || memorySegmentOffset != 0) {
                ERROR_RELEASE(
                    "Reject SQL remote data buffer with non-zero payload offset, readerIndex="
                    << readIndex << ", memorySegmentOffset=" << memorySegmentOffset
                    << ", bufferAddress=" << bufferAddress << ", bufferLength=" << bufferLength);
                originalNetworkBufferRecycler_->recycle(bufferAddress);
                return;
            }
            remoteChannel->notifyRemoteDataAvailableForVectorBatch(
                bufferAddress, bufferLength, sequenceNumber, originalNetworkBufferRecycler_);
        } else {
            remoteChannel->notifyRemoteDataAvailableForNetworkBuffer(
                bufferAddress,
                bufferLength,
                readIndex,
                sequenceNumber,
                originalNetworkBufferRecycler_,
                isBuffer,
                bufferType);
        }
    } else {
        LOG("Channel is not a RemoteInputChannel");
    }
}

long OmniTask::createNativeCreditBasedSequenceNumberingViewReader(
    long resultBufferAddress, ResultPartitionIDPOD partitionId, int subPartitionId)
{
    LOG_TRACE(
        "resultBufferAddress:" << resultBufferAddress
                               << " createNativeCreditBasedSequenceNumberingViewReader:" << partitionId.toString()
                               << " taskName = " << taskNameWithSubtask_ << " subPartitionId" << subPartitionId);
    auto omniShuffleEnv = std::dynamic_pointer_cast<OmniShuffleEnvironment>(this->shuffleEnv_);
    LOG_TRACE(" task name " << taskNameWithSubtask_ << " convert to OmniShuffleEnvironment success............");

    if (!omniShuffleEnv) {
        LOG("Failed to cast shuffleEnv_ to OmniShuffleEnvironment");
        return -1;
    }
    std::shared_ptr<ResultPartitionManager> resultPartitionManager = omniShuffleEnv->getResultPartitionManager();
    int numOfRequiredBuffers = consumableNotifyingPartitionWriters.at(0)->getNumberOfSubpartitions() + 1;
    // Create LocalNettyBufferPool from GlobalNettyBufferPool if available
    std::shared_ptr<LocalNettyBufferPool> localNettyBufferPool;
    auto globalNettyBufferPool = omniShuffleEnv->getGlobalNettyBufferPool();
    if (globalNettyBufferPool) {
        localNettyBufferPool = globalNettyBufferPool->createLocalPool(numOfRequiredBuffers);
        std::lock_guard<std::mutex> lock(localNettyBufferPoolsMutex_);
        localNettyBufferPools.push_back(localNettyBufferPool);
    }

    auto* reader = new OmniCreditBasedSequenceNumberingViewReader(
        partitionId, subPartitionId, resultBufferAddress, localNettyBufferPool);
    int retryCount = 0;
    while (true) {
        try {
            reader->requestSubpartitionView(resultPartitionManager, partitionId, subPartitionId);
            break; // Exit loop if successful
        } catch (...) {
            INFO_RELEASE("OmniTask 1 sleep time: " << std::to_string(200));
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        if (++retryCount >= 3) {
            LOG("Failed to request subpartition view after 3 attempts");
            INFO_RELEASE("!!!!!!!!!!! Fail to create OmniCreditBasedSequenceNumberingViewReader after 3 times ");
            delete reader;
            return -1;
        }
    }
    return reinterpret_cast<long>(reader);
}

std::shared_ptr<TaskMetricGroup> OmniTask::getTaskMetricGroup()
{
    return taskMetricGroup;
}

std::shared_ptr<TaskMetricGroup> OmniTask::createTaskMetricGroup()
{
    return taskMetricGroup;
}

void OmniTask::dispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString)
{
    this->invokable_->dispatchOperatorEvent(operatorIdString, eventString);
}
void OmniTask::notifyCheckpointAborted(long checkpointid, long latestCompletedCheckpointId)
{
    this->notifyCheckpoint(checkpointid, latestCompletedCheckpointId, NotifyCheckpointOperation::ABORT);
}
void OmniTask::notifyCheckpointComplete(long checkpointID, long inputState)
{
    if (inputState == 3 && invokable_ != nullptr) {
        try {
            invokable_->notifyCheckpointCompleteAsync(checkpointID);
        } catch (const std::exception& e) {
            throw;
        }
    }
}

void OmniTask::notifyCheckpointSubsumed(long latestCompletedCheckpointId)
{
    notifyCheckpoint(latestCompletedCheckpointId, -1, NotifyCheckpointOperation::SUBSUME);
}

void OmniTask::notifyCheckpoint(
    long checkpointId, long latestCompletedCheckpointId, OmniTask::NotifyCheckpointOperation notifyCheckpointOperation)
{
    if (executionState == ExecutionState::RUNNING && invokable_ != nullptr) {
        try {
            switch (notifyCheckpointOperation) {
                case OmniTask::NotifyCheckpointOperation::ABORT:
                    invokable_->notifyCheckpointAbortAsync(checkpointId, latestCompletedCheckpointId);
                    break;
                case OmniTask::NotifyCheckpointOperation::COMPLETE:
                    invokable_->notifyCheckpointCompleteAsync(checkpointId);
                    break;
                case OmniTask::NotifyCheckpointOperation::SUBSUME:
                    invokable_->notifyCheckpointSubsumedAsync(checkpointId);
                    break;
            }
        } catch (const std::exception& e) {
            switch (notifyCheckpointOperation) {
                case OmniTask::NotifyCheckpointOperation::ABORT: break;
                case OmniTask::NotifyCheckpointOperation::COMPLETE:
                    // TTODO
                    break;
                case OmniTask::NotifyCheckpointOperation::SUBSUME: throw;
            }
        } catch (...) {
            throw;
        }
    } else {
        // TTODO
    }
}

long OmniTask::createOmniLocalChannelReader(
    ResultPartitionIDPOD partitionId, int subPartitionId, long returnDataAddress)
{
    auto omniShuffleEnv = std::dynamic_pointer_cast<OmniShuffleEnvironment>(this->shuffleEnv_);
    LOG_TRACE(" task name " << taskNameWithSubtask_ << " convert to OmniShuffleEnvironment success............");

    if (!omniShuffleEnv) {
        INFO_RELEASE(
            "PartitionRequest[LOCAL] FAILED task=" << taskNameWithSubtask_ << " partitionId=" << partitionId.toString()
                                                   << " subPartitionId=" << subPartitionId
                                                   << " reason=shuffleEnv_cast_failed");
        return -1;
    }
    std::shared_ptr<ResultPartitionManager> resultPartitionManager = omniShuffleEnv->getResultPartitionManager();

    auto reader =
        std::make_unique<OmniLocalChannelReader>(partitionId, subPartitionId, returnDataAddress, taskNameWithSubtask_);
    auto readerAddr = reinterpret_cast<long>(reader.get());

    int retryCount = 0;
    while (true) {
        try {
            reader->requestSubpartitionView(resultPartitionManager, partitionId, subPartitionId);
            break; // Exit loop if successful
        } catch (...) {
            INFO_RELEASE("OmniTask 2 sleep time: " << std::to_string(200));
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        if (++retryCount >= 3) {
            LOG("Failed to request subpartition view after 3 attempts");
            INFO_RELEASE("!!!!!!!!!!! Fail to create OmniLocalChannelReader after 3 times ");
            reader.reset();
            return -1;
        }
    }
    omniLocalInputChannelReaders.push_back(std::move(reader));
    return readerAddr;
}

long OmniTask::changeLocalInputChannelToOriginal(ResultPartitionIDPOD partitionId)
{
    IntermediateResultPartitionIDPOD irp = partitionId.getPartitionId();
    for (std::shared_ptr<SingleInputGate> singleInputGate : this->inputGates) {
        std::unordered_map<IntermediateResultPartitionIDPOD, std::shared_ptr<InputChannel>>& inputChannelMap =
            singleInputGate->getInputChannels();
        auto it = inputChannelMap.find(irp);
        if (it != inputChannelMap.end()) {
            // Partition exists in the map
            std::shared_ptr<InputChannel> channel = it->second;
            std::shared_ptr<InputChannel> inputChannel = channel;
            LOG("changeLocalInputChannelToOriginal invoke!");
            if (auto local2 = std::dynamic_pointer_cast<OmniLocalInputChannel>(inputChannel)) {
                LOG("changeLocalInputChannelToOriginal instance of OmniLocalInputChannel!");
            } else if (auto local1 = std::dynamic_pointer_cast<LocalInputChannel>(inputChannel)) {
                LOG("changeLocalInputChannelToOriginal instance of LocalInputChannel!");
            } else if (auto remote1 = std::dynamic_pointer_cast<RemoteInputChannel>(inputChannel)) {
                LOG("changeLocalInputChannelToOriginal instance of RemoteInputChannel!");
            } else {
                LOG("changeLocalInputChannelToOriginal unKnown channel type!");
            }

            // create omniLocalInputChannel
            auto omniShuffleEnv = std::dynamic_pointer_cast<OmniShuffleEnvironment>(this->shuffleEnv_);
            std::shared_ptr<SingleInputGateFactory> singleInputGateFactory =
                omniShuffleEnv->getSingleInputGateFactory();
            shared_ptr<OmniLocalInputChannel> originalInputChannel = singleInputGateFactory->createOriginalInputChannel(
                singleInputGate, channel->getChannelIndex(), partitionId);
            inputChannelMap[irp] = originalInputChannel;
            singleInputGate->changeLocalInputChannelToOriginal(channel->getChannelIndex(), originalInputChannel);
            return reinterpret_cast<long>(originalInputChannel.get());
        }
    }
    return -1;
}

void OmniTask::notifyChannelToOmni(const ResultPartitionIDPOD& partitionId)
{
    const IntermediateResultPartitionIDPOD& irp = partitionId.getPartitionId();
    for (const std::shared_ptr<SingleInputGate>& singleInputGate : this->inputGates) {
        std::unordered_map<IntermediateResultPartitionIDPOD, std::shared_ptr<InputChannel>>& inputChannelMap =
            singleInputGate->getInputChannels();
        auto it = inputChannelMap.find(irp);
        if (it == inputChannelMap.end()) {
            continue;
        }
        auto channel = it->second;
        if (auto inputChannel = std::dynamic_pointer_cast<RecoveredInputChannel>(channel)) {
            inputChannel->SetIsOmniChannel(true);
            INFO_RELEASE(
                "Successfully notify native local input channel to omni. " << channel->getChannelInfo().toString());
        }
        return;
    }
}

int OmniTask::GetTaskType()
{
    return taskType;
}

long OmniTask::GetRecycleBufferAddress()
{
    if (originalNetworkBufferRecycler_) {
        return originalNetworkBufferRecycler_->getRecycleBufferAddress();
    } else {
        return -1;
    }
}
ExecutionState OmniTask::getExecutionState()
{
    return this->executionState;
}

void OmniTask::triggerCheckpointBarrier(
    long checkpointid, long checkpointtimestamp, std::shared_ptr<CheckpointOptions> checkpointOptions)
{
    OmniStreamTask* checkpointableTask = this->invokable_.get();
    auto checkpointMetaData = std::make_shared<CheckpointMetaData>(
        checkpointid, checkpointtimestamp, std::chrono::system_clock::now().time_since_epoch().count());
    /* TODO 规避：默认设置为 RUNNING 状态, 后续根据实际情况调整 */
    executionState = ExecutionState::RUNNING;
    if (executionState == ExecutionState::RUNNING) {
        if (checkpointableTask == nullptr) {
            throw std::runtime_error("invokable is not checkpointable");
        }
        try {
            checkpointableTask->triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
            // TTODO
        } catch (const OmniException& ex) {
            INFO_RELEASE(
                "Error:triggerCheckpointBarrier caught OmniException for cp " << checkpointid << ": " << ex.what());
            std::runtime_error wrapped(std::string("OmniException: ") + ex.what());
            this->declineCheckpoint(checkpointid, CheckpointFailureReason::CHECKPOINT_DECLINED_TASK_CLOSING, &wrapped);
        } catch (const std::exception& t) {
            INFO_RELEASE(
                "Error:triggerCheckpointBarrier caught std::exception for cp " << checkpointid << ": " << t.what());
            std::runtime_error wrapped(std::string("std::exception: ") + t.what());
            this->declineCheckpoint(checkpointid, CheckpointFailureReason::CHECKPOINT_DECLINED, &wrapped);
        }
    } else {
        this->declineCheckpoint(checkpointid, CheckpointFailureReason::CHECKPOINT_DECLINED_TASK_NOT_READY);
    }
}

void OmniTask::declineCheckpoint(long checkpointid, CheckpointFailureReason failureReason)
{
    std::string checkpointIDStr = to_string(checkpointid);
    std::string failureReasonStr = toString(failureReason);
    std::string exceptionStr = "nullptr";
    this->declineCheckpoint(checkpointid, failureReason, nullptr);
}
void OmniTask::declineCheckpoint(long checkpointid, CheckpointFailureReason failureReason, std::exception* e)
{
    std::string checkpointIDStr = to_string(checkpointid);
    std::string failureReasonStr = toString(failureReason);
    if (e == nullptr) {
        std::string exceptionStr = "nullptr";
        omni_task_bridge->declineCheckpoint(checkpointIDStr, failureReasonStr, exceptionStr);
    } else {
        std::string exceptionStr = e->what();
        omni_task_bridge->declineCheckpoint(checkpointIDStr, failureReasonStr, exceptionStr);
    }
}

std::shared_ptr<RemoteDataFetcherBridge> OmniTask::GetRemoteDataFetcherBridge()
{
    return remoteDataFetcherBridge_;
}

void OmniTask::SetTaskLocalNettyBufferMetricGroup(
    std::shared_ptr<TaskLocalNettyBufferMetricGroup> taskLocalNettyBufferMetricGroup_)
{
    this->taskLocalNettyBufferMetricGroup = taskLocalNettyBufferMetricGroup_;
}

void OmniTask::SetVectorBatchBufferPoolMetricGroup(
    std::shared_ptr<VectorBatchBufferPoolMetricGroup> vectorBatchBufferPoolMetricGroup_)
{
    this->vectorBatchBufferPoolMetricGroup = vectorBatchBufferPoolMetricGroup_;
}

SizeGauge::SizeSupplier OmniTask::CreateLocalNettyBufferMetricSupplier(const std::string& metricName)
{
    auto sumMetric = [this](const std::function<int(const std::shared_ptr<LocalNettyBufferPool>&)>& getter) {
        int total = 0;
        std::lock_guard<std::mutex> lock(localNettyBufferPoolsMutex_);
        for (const auto& localNettyBufferPool : localNettyBufferPools) {
            if (localNettyBufferPool) {
                total += getter(localNettyBufferPool);
            }
        }
        return total;
    };

    if (metricName == "requiredBuffers") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getNumberOfRequiredBuffers(); });
        };
    }
    if (metricName == "maxBuffers") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getMaxNumberOfBuffers(); });
        };
    }
    if (metricName == "currentPoolSize") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getCurrentPoolSize(); });
        };
    }
    if (metricName == "availableBuffers") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getNumberOfAvailableBuffers(); });
        };
    }
    if (metricName == "requestedBuffers") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getNumberOfRequestedBuffers(); });
        };
    }
    if (metricName == "requestRegularBufferCount") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getRequestRegularBufferCount(); });
        };
    }
    if (metricName == "requestBigBufferCount") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getRequestBigBufferCount(); });
        };
    }
    if (metricName == "recycleRegularBufferCount") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getRecycleRegularBufferCount(); });
        };
    }
    if (metricName == "recycleBigBufferCount") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getRecycleBigBufferCount(); });
        };
    }
    if (metricName == "bigTotalMemorySize") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getBigTotalMemorySize(); });
        };
    }
    if (metricName == "availableBigMemorySize") {
        return [sumMetric]() {
            return sumMetric(
                [](const std::shared_ptr<LocalNettyBufferPool>& pool) { return pool->getAvailableBigMemorySize(); });
        };
    }

    throw std::runtime_error("Unknown LocalNettyBufferPool metric: " + metricName);
}
} // namespace omnistream
