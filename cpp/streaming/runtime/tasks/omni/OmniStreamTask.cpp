/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#include "OmniStreamTask.h"
#include <iostream>
#include <chrono>
#include <thread>

#include <streaming/runtime/partitioner/V2/StreamPartitionerV2.h>
#include "runtime/io/network/api/writer/V2/RecordWriterDelegateV2.h"
#include <runtime/io/network/api/writer/V2/SingleRecordWriterV2.h>
#include <streaming/runtime/partitioner/ForwardPartitioner.h>
#include <streaming/runtime/partitioner/KeyGroupStreamPartitioner.h>
#include <streaming/runtime/partitioner/RebalancePartitioner.h>
#include <streaming/runtime/partitioner/RescalePartitioner.h>
#include "common.h"
#include "../RegularOperatorChain.h"
#include <streaming/runtime/partitioner/V2/SimplePartitioner.h>
#include <runtime/io/network/api/writer/V2/RecordWriterBuilderV2.h>
#include <streaming/runtime/partitioner/V2/ForwardPartitionerV2.h>
#include <streaming/runtime/partitioner/V2/RebalancePartitionerV2.h>
#include <streaming/runtime/partitioner/V2/KeyGroupStreamPartitionerV2.h>
#include <streaming/runtime/partitioner/V2/RescalePartitionerV2.h>
#include <streaming/runtime/partitioner/V2/GlobalPartitionerV2.h>
#include <utils/monitormmap/MetricManager.h>

#include "runtime/checkpoint/SavepointType.h"
#include "streaming/runtime/tasks/mailbox/TaskMailboxImpl.h"
#include "core/api/common/TaskInfoImpl.h"
#include "runtime/io/network/api/writer/V2/MultipleRecordWritersV2.h"
#include "runtime/io/network/api/writer/V2/NonRecordWriterV2.h"
#include "streaming/runtime/tasks/OperatorChain.h"
#include "runtime/state/RocksDBStateBackend.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/LocalRecoveryDirectoryProviderImpl.h"
#ifdef WITH_OMNISTATESTORE
#include "runtime/state/ockdb/EmbeddedOckStateBackend.h"
#endif
#include "runtime/taskmanager/OmniTask.h"
#include "partition/BufferWritingResultPartition.h"
#include "streaming/runtime/tasks/ProcessingTimeServiceImpl.h"

namespace omnistream {

OmniStreamTask::OmniStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env, int taskType)
    : OmniStreamTask(env, StreamTaskActionExecutor::IMMEDIATE, taskType) {};

OmniStreamTask::OmniStreamTask(
    std::shared_ptr<RuntimeEnvironmentV2>& env, std::shared_ptr<StreamTaskActionExecutor> actionExecutor, int taskType)
    : OmniStreamTask(env, actionExecutor, new TaskMailboxImpl(std::this_thread::get_id()), taskType)
{
}

OmniStreamTask::OmniStreamTask(
    std::shared_ptr<RuntimeEnvironmentV2>& env,
    std::shared_ptr<StreamTaskActionExecutor> actionExecutor,
    TaskMailbox* mailbox,
    int taskType)
    : env_(env),
      actionExecutor_(actionExecutor),
      mailbox_(mailbox),
      taskConfiguration_(env->taskConfiguration()),
      taskType(taskType)
{
    LOG("begin>>>>");
}

void OmniStreamTask::postConstruct()
{
    LOG_DEBUG("postConstruct begin");

    // needs to be shared_ptr
    auto streamTaskAction = std::make_unique<StreamTaskAction>(this);
    mailboxProcessor_ = std::make_unique<MailboxProcessor>(std::move(streamTaskAction), mailbox_, actionExecutor_);
    mainMailboxExecutor_ = mailboxProcessor_->getMainMailboxExecutor();
    LOG("mailboxProcessor_  init setup>>>>");
    taskConfiguration_ = env_->taskConfiguration();
    auto checkpointExecutionConfig = taskConfiguration_.getExecutionCheckpointConfig();
    taskName_ = taskConfiguration_.getTaskName();
    LOG("begin>>>>" << taskName_);
    recordWriter_ = createRecordWriterDelegate(taskConfiguration_, env_);

    // The internal TimerService used to define the current processing time (default = System.currentTimeMillis()),
    // and register timers for tasks to be executed in the future.
    timerService = std::make_shared<SystemProcessingTimeService>();
    // In contrast to timerService, we should not register any user timers here. It should be used only for system level
    // timers.
    systemTimerService = std::make_shared<SystemProcessingTimeService>();

    // SubtaskCheckpointCoordinatorImpl initialization
    if (taskConfiguration_.getStateBackend() == "HashMapStateBackend") {
        stateBackend = new HashMapStateBackend();
    } else if (taskConfiguration_.getStateBackend() == "EmbeddedOckStateBackend") {
#ifdef WITH_OMNISTATESTORE
        stateBackend = new EmbeddedOckStateBackend(taskConfiguration_);
#else
        ERROR_RELEASE("EmbeddedOckStateBackend was requested, but OmniStream was built without WITH_OMNISTATESTORE");
        THROW_RUNTIME_ERROR(
            "EmbeddedOckStateBackend was requested, but OmniStream was built without WITH_OMNISTATESTORE");
#endif
    } else {
        stateBackend = new EmbeddedRocksDBStateBackend(taskConfiguration_);
    }
    checkpointStorage = createCheckpointStorage(stateBackend);
    std::shared_ptr<CheckpointStorageAccess> checkpointStorageAccess =
        checkpointStorage->createCheckpointStorage(taskConfiguration_.getTmpWorkingDirectory());

    subtaskCheckpointCoordinator = std::make_shared<runtime::SubtaskCheckpointCoordinatorImpl>(
        checkpointStorage,
        checkpointStorageAccess,
        env_->taskConfiguration().getTaskName(),
        actionExecutor_,
        // getAsyncOperatorsThreadPool(),
        env_,
        checkpointExecutionConfig.getUnalignedCheckpointsEnabled(),
        checkpointExecutionConfig.getCheckpointAfterTasksFinishEnabled(),
        new std::function<std::shared_ptr<CompletableFutureV2<void>>(std::shared_ptr<ChannelStateWriter>, long)>(
            [this](std::shared_ptr<ChannelStateWriter> writer, long checkpointId) {
                return this->prepareInputSnapshot(writer, checkpointId);
            }),
        runtime::BarrierAlignmentUtil::template createRegisterTimerCallback<std::function<void()>>(
            nullptr, systemTimerService.get())

    );
    InjectChannelStateWriterIntoChannels();
    // Reconstruct LocalRecoveryConfig
    std::shared_ptr<LocalRecoveryDirectoryProviderImpl> dirProvider;
    numberOfInnerRecover = env_->getInputGates().size();
    if (taskConfiguration_.getCheckpointConfig().getLocalRecovery()) {
        std::string localRecoveryProviderStr = taskConfiguration_.getLocalRecoveryConfig();
        nlohmann::json localJson = nlohmann::json::parse(localRecoveryProviderStr);
        std::vector<std::filesystem::path> allocationBaseDirs;
        for (auto path : localJson["allocationBaseDirs"].get<std::vector<std::string>>()) {
            allocationBaseDirs.push_back(std::filesystem::path(path));
        }
        JobIDPOD jobId = TaskStateSnapshotDeserializer::HexStringToOperatorId<JobIDPOD>(localJson["jobID"]);
        JobVertexID jobVertexId =
            TaskStateSnapshotDeserializer::HexStringToOperatorId<JobVertexID>(localJson["jobVertexID"]);
        dirProvider = std::make_shared<LocalRecoveryDirectoryProviderImpl>(
            allocationBaseDirs, jobId, jobVertexId, std::stoi(localJson["subtaskIndex"].get<std::string>()));
        dirProvider->SetJobIdHexStr(localJson["jobID"]);
        dirProvider->SetVertexIdHexStr(localJson["jobVertexID"]);
    }
    auto localRecoveryConfig = std::make_shared<LocalRecoveryConfig>(dirProvider);
    env_->setLocalRecoveryConfig(localRecoveryConfig);
    LOG("postConstruct end >>>> mailboxProcessor_ " << mailboxProcessor_->toString());
}

void OmniStreamTask::restore()
{
    LOG(">>>>>>>>");
    restoreInternal();
}

bool OmniStreamTask::IsUsingNonBlockingInput()
{
    return true;
}

void OmniStreamTask::InjectChannelStateWriterIntoChannels()
{
    auto channelStateWriter = subtaskCheckpointCoordinator->getChannelStateWriter();
    for (auto gate : env_->GetAllInputGates()) {
        gate->SetChannelStateWriter(channelStateWriter);
    }
    for (auto writer : env_->writers()) {
        if (auto writerChild = std::dynamic_pointer_cast<BufferWritingResultPartition>(writer)) {
            writerChild->SetChannelStateWriter(channelStateWriter);
        }
    }
}

void OmniStreamTask::restoreInternal()
{
    LOG("Initializing {}." << getName());

    this->operatorChain =
        std::make_unique<RegularOperatorChain>(std::weak_ptr<OmniStreamTask>(shared_from_this()), this->recordWriter_);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(stateBackend, env_.get());
    this->operatorChain->initializeStateAndOpenOperators(initializer, taskConfiguration_);
    this->mainOperator_ = operatorChain->getMainOperator();

    // task specific initialization
    init();

    /*
            *   // we need to make sure that any triggers scheduled in open() cannot be
                    // executed before all operators are opened
                    CompletableFuture<Void> allGatesRecoveredFuture = actionExecutor.call(this::restoreGates);
     */
    // for now, skip the actionExecutor, direct call restroGates
    // and inside restoregate does not use mailbox loop.
    // we assume the new inputgate is alreay restore.
    restoreGates();
    isRunning = true;
}

/**
*   private CompletableFuture<Void> restoreGates() throws Exception {
        SequentialChannelStateReader reader =
                getEnvironment().getTaskStateManager().getSequentialChannelStateReader();
        reader.readOutputData(
                getEnvironment().getAllWriters(), !configuration.isGraphContainingLoops());

        operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());

        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();
        channelIOExecutor.execute(
                () -> {
                    try {
                        reader.readInputData(inputGates);
                    } catch (Exception e) {
                        asyncExceptionHandler.handleAsyncException(
                                "Unable to read channel state", e);
                    }
                });

        List<CompletableFuture<?>> recoveredFutures = new ArrayList<>(inputGates.length);
        for (InputGate inputGate : inputGates) {
            recoveredFutures.add(inputGate.getStateConsumedFuture());

            inputGate
                    .getStateConsumedFuture()
                    .thenRun(
                            () ->
                                    mainMailboxExecutor.execute(
                                            inputGate::RequestPartitions,
                                            "Input gate request partitions"));
        }

        return CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0]))
                .thenRun(mailboxProcessor::suspend);
    }
 *
 */

// simple requirePartionView for each local chaneel
void OmniStreamTask::restoreGates()
{
    try {
        INFO_RELEASE("restoreGates begin " << taskName_);
        auto indexedInputGates = env_->GetAllInputGates();
        auto reader = env_->getTaskStateManager()->getSequentialChannelStateReader();
        INFO_RELEASE("restoreGates before readOutputData, task=" << taskName_);
        reader->readOutputData(env_->getAllWriters(), true);
        INFO_RELEASE("restoreGates after readOutputData, task=" << taskName_);
        if (indexedInputGates.empty()) {
            INFO_RELEASE("SQL scenarios, do not need to restore the channel.");
            return;
        }

        std::vector<std::shared_ptr<InputGate>> inputGateVec;
        inputGateVec.reserve(indexedInputGates.size());
        for (const auto& inputGate : indexedInputGates) {
            inputGateVec.emplace_back(inputGate);
        }

        reader->readInputData(inputGateVec);

        INFO_RELEASE("restoreGates before recovery mailbox loop");
        mailboxProcessor_->runMailboxLoop();
        INFO_RELEASE("restoreGates after recovery mailbox loop");
        bool allRecovered;
        do {
            allRecovered = true;
            for (size_t gateIndex = 0; gateIndex < inputGateVec.size(); ++gateIndex) {
                const auto recoveredFlags = inputGateVec[gateIndex]->getStateConsumedFuture1();
                const bool gateRecovered =
                    std::all_of(recoveredFlags.begin(), recoveredFlags.end(), [](bool recovered) { return recovered; });
                if (!gateRecovered) {
                    allRecovered = false;
                    INFO_RELEASE("restoreGates: recovered channels are not fully consumed, gateIndex=" << gateIndex);
                }
            }
        } while (!allRecovered);

        for (size_t gateIndex = 0; gateIndex < inputGateVec.size(); ++gateIndex) {
            INFO_RELEASE("restoreGates requestPartitions after all gates recovered, gateIndex=" << gateIndex);
            inputGateVec[gateIndex]->RequestPartitions();
        }

        INFO_RELEASE("restoreGates complete!");
    } catch (...) {
        INFO_RELEASE("Error: restoreGates failed, unable to read channel state");
        throw std::runtime_error("restoreGates failed, unable to read channel state");
    }
}

void OmniStreamTask::invoke()
{
    LOG("Invoking {}." << getName());

    // let the task do its work
    LOG("Invoking {}." << getName() << "run mailbox loop");
    runMailboxLoop();

    if (syncSavepoint != INT64_MIN) {
        INFO_RELEASE("OmniStreamTask::invoke waiting for checkpoint resolution for task " << getName());
        while (!finalCheckpointCompleted->IsDone()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (finalCheckpointCompleted->IsDone()) {
            INFO_RELEASE("OmniStreamTask::invoke checkpoint resolved for task " << getName());
        } else {
            INFO_RELEASE("OmniStreamTask::invoke timed out waiting for checkpoint resolution for task " << getName());
        }
    }
}

void OmniStreamTask::cancel()
{
    mailboxProcessor_->suspend();
}

void OmniStreamTask::cleanup()
{
    INFO_RELEASE("Start cleaning up OmniStreamTask");
    // clean up operator chain and record writer
    releaseOutputResource();

    if (operatorChain != nullptr && !closedOperators_) {
        closedOperators_ = true;
        operatorChain->CloseAllOperators();
    }

    if (inputProcessor_ != nullptr) {
        inputProcessor_->close();
    }
    INFO_RELEASE("Finish cleaning up OmniStreamTask");
}

void OmniStreamTask::releaseOutputResource()
{
    LOG_INFO_IMP(" recordWriter_  " << reinterpret_cast<uintptr_t>(recordWriter_.get()));
    if (recordWriter_) {
        recordWriter_->close();
    }
}

void OmniStreamTask::EndData(StopMode mode)
{
    INFO_RELEASE("TaskName:" << taskName_ << " EndData with  Mode:" << static_cast<int>(mode));
    if (mode == StopMode::DRAIN) {
        AdvanceToEndOfEventTime();
    }
    if (operatorChain != nullptr) {
        operatorChain->finishOperators(actionExecutor_.get());
        this->finishedOperators = true;
    }

    for (auto partitionWriter : env_->writers()) {
        partitionWriter->NotifyEndOfData(mode);
    }

    this->endOfDataReceived = true;
}

void OmniStreamTask::processInput(MailboxDefaultAction::Controller* controller)
{
    auto status = inputProcessor_->processInput();
    switch (status) {
        case DataInputStatus::MORE_AVAILABLE: return;
        case DataInputStatus::NOTHING_AVAILABLE: break;
        case DataInputStatus::INNER_RECOVER:
            INFO_RELEASE("processInput INNER_RECOVER, stop recovery mail box :" << numberOfInnerRecover);
            if (--numberOfInnerRecover == 0) {
                mailboxProcessor_->suspend();
            }
            GetSubtaskCheckpointCoordinator()->SetisRecoveredFlag(true);
            break;
        case DataInputStatus::END_OF_RECOVERY: GetSubtaskCheckpointCoordinator()->SetisRecoveredFlag(false); return;
        case DataInputStatus::END_OF_DATA: EndData(StopMode::DRAIN); return;
        case DataInputStatus::NOT_PROCESSED: return;
        case DataInputStatus::STOPPED: EndData(StopMode::NO_DRAIN); return;
        case DataInputStatus::END_OF_INPUT:
            controller->suspendDefaultAction();
            mailboxProcessor_->suspend();
            return;
    }
    std::shared_ptr<CompletableFuture> resumeFuture;
    if (!inputProcessor_->isAvailable()) {
        resumeFuture = inputProcessor_->GetAvailableFuture();
        LOG("inputProcessor is not available, wait for it");
    } else {
        return;
    }

    // todo: the timer is not implemented yet, to measure the idle time
    resumeFuture->thenRun(std::make_shared<ResumeWrapper>(controller->suspendDefaultAction(nullptr), nullptr));
}

const std::string OmniStreamTask::getName() const
{
    return taskName_;
}

std::shared_ptr<RecordWriterDelegateV2> OmniStreamTask::createRecordWriterDelegate(
    TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment)
{
    std::vector<RecordWriterV2*> recordWriters = createRecordWriters(taskConfiguration_, environment);
    LOG("TaskInformation " << taskConfig.toString() << "and recorderWriter size " << recordWriters.size());
    if (recordWriters.size() == 1) {
        return std::make_shared<SingleRecordWriterV2>(recordWriters[0]);
    } else if (recordWriters.empty()) {
        return std::make_shared<NonRecordWriterV2>();
    } else {
        return std::make_shared<MultipleRecordWritersV2>(recordWriters);
    }
}

std::vector<RecordWriterV2*> OmniStreamTask::createRecordWriters(
    TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment)
{
    auto recordWriters = std::vector<RecordWriterV2*>();
    auto outEdgesInOrder = taskConfig.getStreamConfigPOD().getOutEdgesInOrder();
    for (size_t i = 0; i < outEdgesInOrder.size(); i++) {
        auto outEdge = outEdgesInOrder[i];
        recordWriters.push_back(createRecordWriter(outEdge, i, environment, taskName_, outEdge.getBufferTimeout()));
    }
    return recordWriters;
}

RecordWriterV2* OmniStreamTask::createRecordWriter(
    StreamEdgePOD& edge,
    int outputIndex,
    std::shared_ptr<RuntimeEnvironmentV2> environment,
    std::string taskName,
    long bufferTimeout)
{
    // output partitioner

    auto partitioner = edge.getPartitioner();

    // Sql Task
    if (taskType == 1) {
        auto outputPartitioner = createPartitionerFromDesc(partitioner);
        LOG("output partitioner " << "output index: " << outputIndex << " task name: " << taskName_);
        // workaround for now
        if (outputPartitioner == nullptr) {
            THROW_RUNTIME_ERROR("outputPartitioner is null"); // the below can not work
        }

        auto bufferPartitionWriter = env_->writers()[outputIndex];
        // we initialize the partitioner here with the number of key groups (aka max. parallelism)
        if (typeid(*outputPartitioner) == typeid(KeyGroupStreamPartitionerV2<StreamRecord, BinaryRowData*>)) {
            int numKeyGroups = bufferPartitionWriter->getNumTargetKeyGroups();
            if (0 < numKeyGroups) {
                reinterpret_cast<KeyGroupStreamPartitionerV2<StreamRecord, BinaryRowData*>*>(outputPartitioner)
                    ->configure(numKeyGroups);
            }
        }

        auto writer = RecordWriterBuilderV2()
                          .withChannelSelector(outputPartitioner)
                          .withWriter(bufferPartitionWriter)
                          .withTimeout(bufferTimeout)
                          .withTaskName(taskName)
                          .withJobType(taskType)
                          .build();

        writer->postConstruct();
        return writer;
    } else if (taskType == 2) {
        // Datastream Task
        auto outputPartitioner = createPartitionerFromDesc(edge);

        LOG("output partitioner " << "output index: " << outputIndex << " task name: " << taskName_);
        // workaround for now
        if (outputPartitioner == nullptr) {
            THROW_RUNTIME_ERROR("outputPartitioner is null"); // the below can not work
        }

        auto bufferPartitionWriter = env_->writers()[outputIndex];
        auto writer = RecordWriterBuilderV2()
                          .withChannelSelector(outputPartitioner)
                          .withWriter(bufferPartitionWriter)
                          .withTimeout(bufferTimeout)
                          .withTaskName(taskName)
                          .withJobType(taskType)
                          .build();

        writer->postConstruct();
        return writer;
    } else {
        THROW_RUNTIME_ERROR("unknown taskType " + taskType); // the below can not work
    }
}

// record write deleage

StreamPartitionerV2<StreamRecord>* OmniStreamTask::createPartitionerFromDesc(StreamPartitionerPOD partitioner)
{
    if (partitioner.getPartitionerName() == StreamPartitionerPOD::FORWARD) {
        return new ForwardPartitionerV2<StreamRecord>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::REBALANCE) {
        return new RebalancePartitionerV2<StreamRecord>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::RESCALE) {
        return new RescalePartitionerV2<StreamRecord>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::GLOBAL) {
        return new GlobalPartitionerV2<StreamRecord>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::HASH) {
        auto hashFields = partitioner.getHashFields();
        auto keySelector = buildKeySelector<BinaryRowData*>(hashFields);
        int32_t maxParallelism = env_->taskConfiguration().getMaxNumberOfSubtasks();
        return new KeyGroupStreamPartitionerV2<StreamRecord, BinaryRowData*>(keySelector, maxParallelism);
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::NONE) {
        return new SimplePartitioner<StreamRecord>();
    } else {
        throw std::invalid_argument("Invalid partitioner!");
    }
}

datastream::StreamPartitioner<IOReadableWritable>* OmniStreamTask::createPartitionerFromDesc(
    const StreamEdgePOD& edge, bool recover)
{
    const auto& partitioner = edge.getPartitioner();

    if (partitioner.getPartitionerName() == StreamPartitionerPOD::FORWARD) {
        return new datastream::ForwardPartitioner<IOReadableWritable>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::REBALANCE) {
        return new datastream::RebalancePartitioner<IOReadableWritable>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::RESCALE) {
        return new datastream::RescalePartitioner<IOReadableWritable>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::HASH) {
        int targetId = edge.getTargetId();
        int sourceId = edge.getSourceId();
        auto taskInfo = env_->taskConfiguration();
        std::unordered_map<int, StreamConfigPOD> configMap = taskInfo.getChainedConfigMap();
        auto description = configMap[sourceId].getOperatorDescription().getDescription();
        if (recover) {
            // 可能是非对齐input buffer恢复在fiter时使用
            description = configMap[targetId].getOperatorDescription().getDescription();
        }
        nlohmann::json config = nlohmann::json::parse(description);
        int32_t maxParallelism = env_->taskConfiguration().getMaxNumberOfSubtasks();
        return new datastream::KeyGroupStreamPartitioner<IOReadableWritable, Object>(
            config, targetId, maxParallelism, recover);
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::NONE) {
        throw std::invalid_argument("Invalid partitioner!");
    } else {
        throw std::invalid_argument("Invalid partitioner!");
    }
}

template <typename K>
KeySelector<K>* OmniStreamTask::buildKeySelector(std::vector<KeyFieldInfoPOD>& keyFields)
{
    std::vector<int> keyCols;
    std::vector<int> keyTypes;
    for (auto& keyField : keyFields) {
        keyCols.emplace_back(keyField.getFieldIndex());
        if (keyField.getFieldTypeName() == "BIGINT") {
            keyTypes.emplace_back(omniruntime::type::OMNI_LONG);
        } else if (keyField.getFieldTypeName() == "VARCHAR") {
            keyTypes.emplace_back(omniruntime::type::OMNI_VARCHAR);
        } else if (keyField.getFieldTypeName() == "INTEGER") {
            keyTypes.emplace_back(omniruntime::type::OMNI_INT);
        } else if (keyField.getFieldTypeName() == "OMNI_TIME_WITHOUT_TIME_ZONE") {
            keyTypes.emplace_back(omniruntime::type::OMNI_TIME_WITHOUT_TIME_ZONE);
        } else {
            THROW_LOGIC_EXCEPTION("Not supported field type: " << keyField.getFieldTypeName());
        }
    }
    return new KeySelector<BinaryRowData*>(keyTypes, keyCols);
}

// mailbox loop
void OmniStreamTask::runMailboxLoop()
{
    LOG("runMailboxLoop  >>>>>");
    mailboxProcessor_->runMailboxLoop();
}

void OmniStreamTask::dispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString)
{
    auto mailboxRunnable = std::make_shared<VoidFunctionRunnable>([this, operatorIdString, eventString]() {
        try {
            operatorChain->DispatchOperatorEvent(operatorIdString, eventString);
        } catch (const std::exception& e) {
            isRunning = false;
            LOG("Error: mailbox dispatch failed, operatorId=" << operatorIdString << ", eventBytes="
                                                              << eventString.size() << ", error=" << e.what());
            mailboxProcessor_->suspend();
        } catch (...) {
            isRunning = false;
            LOG("Error: mailbox dispatch failed, operatorId=" << operatorIdString << ", eventBytes="
                                                              << eventString.size() << ", error=unknown");
            mailboxProcessor_->suspend();
        }
    });
    try {
        mainMailboxExecutor_->execute(mailboxRunnable, "dispatchOperatorEvent");
    } catch (const std::exception& e) {
        LOG("Error: operator event dropped, mailbox not running, operatorId=" << operatorIdString
                                                                              << ", error=" << e.what());
    }
}

std::shared_ptr<CheckpointStorage> OmniStreamTask::createCheckpointStorage(StateBackend* backend)
{
    return nullptr;
}

std::shared_ptr<CompletableFutureV2<void>> OmniStreamTask::prepareInputSnapshot(
    std::shared_ptr<ChannelStateWriter> channelStateWriter, long checkpointID)
{
    if (inputProcessor_ == nullptr) {
        // Source tasks have no input channels. Return an already-completed future so the
        // channel-state writer can immediately finish the input side of an unaligned checkpoint.
        auto completed = std::make_shared<CompletableFutureV2<void>>();
        completed->Complete();
        return completed;
    }
    return inputProcessor_->PrepareSnapshot(channelStateWriter, checkpointID);
}

bool OmniStreamTask::PerformCheckpoint(
    std::shared_ptr<CheckpointMetaData> checkpointMetaData,
    std::shared_ptr<CheckpointOptions> checkpointOptions,
    std::shared_ptr<CheckpointMetricsBuilder> checkpointMetrics)
{
    try {
        SnapshotType* checkpointType = checkpointOptions->GetCheckpointType();
        LOG("Starting checkpoint {} {} on task {}" << checkpointMetaData->GetCheckpointId()
                                                   << checkpointType->ToString() << getName());

        VoidFunctionRunnable checkpointRunnable(
            [this, checkpointMetaData, checkpointOptions, checkpointMetrics, checkpointType]() {
                if (IsSynchronous(checkpointType)) {
                    SetSynchronousSavepoint(checkpointMetaData->GetCheckpointId());
                }

                if (AreCheckpointsWithFinishedTasksEnabled() && endOfDataReceived &&
                    this->finalCheckpointMinId == INT64_MIN) {
                    this->finalCheckpointMinId = checkpointMetaData->GetCheckpointId();
                }
                std::shared_ptr<LambdaSupplier<bool>> isRunningLoad = std::make_shared<LambdaSupplier<bool>>([this]() {
                    auto ret = std::make_shared<bool>(this->IsRunning());
                    return ret;
                });

                subtaskCheckpointCoordinator->checkpointState(
                    checkpointMetaData,
                    checkpointOptions,
                    checkpointMetrics,
                    operatorChain.get(),
                    finishedOperators,
                    isRunningLoad);
            });
        if (isRunning) {
            actionExecutor_->run(&checkpointRunnable);
            return true;
        } else {
            VoidFunctionRunnable broadcastRunnable([this, checkpointMetaData]() {
                // we cannot perform our checkpoint - let the downstream operators know that
                // they
                // should not wait for any input from this operator

                // we cannot broadcast the cancellation markers on the 'operator chain',
                // because it may not
                // yet be created
                auto message = std::make_shared<CancelCheckpointMarker>(checkpointMetaData->GetCheckpointId());
                recordWriter_->broadcastEvent(message);
            });
            actionExecutor_->run(&broadcastRunnable);
            return false;
        }
    } catch (const std::exception& e) {
        throw; // Re-throw the exception
    }
}

void OmniStreamTask::abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
{
    LOG("Aborting checkpoint via cancel-barrier " << checkpointId << " for task " << getName());

    // clear checkpoint storage cache and abort channel-state writes.
    try {
        std::shared_ptr<runtime::SubtaskCheckpointCoordinatorImpl> subtaskCheckpointCoordinatorImpl =
            std::dynamic_pointer_cast<runtime::SubtaskCheckpointCoordinatorImpl>(subtaskCheckpointCoordinator);
        if (subtaskCheckpointCoordinatorImpl) {
            subtaskCheckpointCoordinatorImpl->AbortCheckpointOnBarrier(checkpointId, std::make_exception_ptr(cause));
        }
    } catch (...) {
        //
    }

    // Notify the JobManager that we decline this checkpoint.
    try {
        if (env_ && env_->omniTask()) {
            env_->omniTask()->declineCheckpoint(checkpointId, cause.GetCheckpointFailureReason());
        }
    } catch (...) {
        //
    }

    // Notify operators and broadcast a CancelCheckpointMarker downstream.
    VoidFunctionRunnable abortRunnable(
        [this, checkpointId]() {
            try {
                if (operatorChain != nullptr) {
                    operatorChain->NotifyCheckpointAborted(checkpointId);
                    operatorChain->broadcastEvent(std::make_shared<omnistream::CancelCheckpointMarker>(checkpointId));
                } else if (recordWriter_ != nullptr) {
                    auto msg = std::make_shared<omnistream::CancelCheckpointMarker>(checkpointId);
                    recordWriter_->broadcastEvent(msg);
                }
            } catch (...) {
                //
            }
        },
        "AbortCheckpointOnBarrier");

    if (actionExecutor_) {
        actionExecutor_->run(&abortRunnable);
    } else {
        abortRunnable.Run();
    }
}

std::shared_ptr<CompletableFutureV2<void>> OmniStreamTask::notifyCheckpointCompleteAsync(long checkpointId)
{
    if (isCurrentSyncSavepoint(checkpointId)) {
        INFO_RELEASE(
            "OmniStreamTask::notifyCheckpointCompleteAsync checkpointId " << checkpointId << " completed for task "
                                                                          << getName());
        finalCheckpointCompleted->Complete();
    }

    std::string description = "checkpoint ";
    description += std::to_string(checkpointId);
    description += " complete";
    return notifyCheckpointOperation([this, checkpointId]() { notifyCheckpointComplete(checkpointId); }, description);
}

std::shared_ptr<CompletableFutureV2<void>> OmniStreamTask::notifyCheckpointSubsumedAsync(long checkpointId)
{
    std::string description = "checkpoint ";
    description += std::to_string(checkpointId);
    description += " subsumed";
    return notifyCheckpointOperation(
        [this, checkpointId]() {
            std::shared_ptr<runtime::SubtaskCheckpointCoordinatorImpl> subtaskCheckpointCoordinatorImpl =
                std::dynamic_pointer_cast<runtime::SubtaskCheckpointCoordinatorImpl>(subtaskCheckpointCoordinator);
            bool running = this->IsRunning();

            std::shared_ptr<omnistream::Supplier<bool>> isRunningSupplier =
                std::make_shared<omnistream::LambdaSupplier<bool>>(
                    [running]() { return std::make_shared<bool>(running); });

            subtaskCheckpointCoordinatorImpl->notifyCheckpointSubsumed(
                checkpointId, operatorChain.get(), isRunningSupplier.get());
        },
        description);
}

std::shared_ptr<CompletableFutureV2<void>> OmniStreamTask::notifyCheckpointAbortAsync(
    long checkpointId, long latestCompletedCheckpointId)
{
    if (isCurrentSyncSavepoint(checkpointId)) {
        INFO_RELEASE(
            "OmniStreamTask::notifyCheckpointAbortAsync checkpointId " << checkpointId << " aborted for task "
                                                                       << getName());
        finalCheckpointCompleted->Complete();
    }

    std::string description = "checkpoint ";
    description += std::to_string(checkpointId);
    description += " aborted";

    return notifyCheckpointOperation(
        [=]() {
            if (latestCompletedCheckpointId > 0) {
                notifyCheckpointComplete(latestCompletedCheckpointId);
            }
            if (isCurrentSyncSavepoint(checkpointId)) {
                throw std::runtime_error("Stop-with-savepoint failed");
            }
            std::shared_ptr<runtime::SubtaskCheckpointCoordinatorImpl> subtaskCheckpointCoordinatorImpl =
                std::dynamic_pointer_cast<runtime::SubtaskCheckpointCoordinatorImpl>(subtaskCheckpointCoordinator);
            bool running = this->IsRunning();

            std::shared_ptr<omnistream::Supplier<bool>> isRunningSupplier =
                std::make_shared<omnistream::LambdaSupplier<bool>>(
                    [running]() { return std::make_shared<bool>(running); });

            subtaskCheckpointCoordinatorImpl->notifyCheckpointAborted(
                checkpointId, operatorChain.get(), isRunningSupplier.get());
        },
        description);
}

// TTODO: TO BE COMPELETED
std::shared_ptr<CompletableFutureV2<void>> OmniStreamTask::notifyCheckpointOperation(
    std::function<void()> runnable, const std::string& description)
{
    std::shared_ptr<CompletableFutureV2<void>> result;
    auto task = std::make_unique<VoidFunctionRunnable>([runnable, result]() mutable {
        try {
            runnable();
        } catch (const std::exception& ex) {
            throw;
        }
    });
    mailboxProcessor_->getMailboxExecutor(TaskMailbox::MAX_PRIORITY)->execute(std::move(task), description);
    return result;
}

void OmniStreamTask::notifyCheckpointComplete(long checkpointId)
{
    LOG("start notify Checkpoint Complete.");
    if (checkpointId <= latestReportCheckpointId) {
        return;
    }
    latestReportCheckpointId = checkpointId;
    std::shared_ptr<runtime::SubtaskCheckpointCoordinatorImpl> subtaskCheckpointCoordinatorImpl =
        std::dynamic_pointer_cast<runtime::SubtaskCheckpointCoordinatorImpl>(subtaskCheckpointCoordinator);
    bool running = this->IsRunning();

    std::shared_ptr<omnistream::Supplier<bool>> isRunningSupplier =
        std::make_shared<omnistream::LambdaSupplier<bool>>([running]() { return std::make_shared<bool>(running); });

    subtaskCheckpointCoordinatorImpl->notifyCheckpointComplete(
        checkpointId, operatorChain.get(), isRunningSupplier.get());

    if (isRunning) {
        if (isCurrentSyncSavepoint(checkpointId)) {
            finalCheckpointCompleted->Complete();
        } else if (!syncSavepoint && finalCheckpointMinId != INT64_MIN && checkpointId >= finalCheckpointMinId) {
            finalCheckpointCompleted->Complete();
        }
    }
}

bool OmniStreamTask::isCurrentSyncSavepoint(long checkpointId)
{
    return syncSavepoint != INT64_MIN && syncSavepoint == checkpointId;
}

void OmniStreamTask::triggerStopWithSavepoint(const std::shared_ptr<CheckpointOptions>& checkpointOptions)
{
    if (!checkpointOptions->GetCheckpointType()) {
        INFO_RELEASE("Error triggerStopWithSavepoint checkpointType is null");
        throw std::runtime_error("triggerStopWithSavepoint checkpointType is null");
    }
    if (!checkpointOptions->GetCheckpointType()->IsSavepoint()) {
        INFO_RELEASE("Warn triggerStopWithSavepoint checkpointType is not Savepoint");
        return;
    }
    SavepointType* savepointType = dynamic_cast<SavepointType*>(checkpointOptions->GetCheckpointType());
    if (!savepointType) {
        INFO_RELEASE("Error triggerStopWithSavepoint savepointType CheckpointType can not cast to SavepointType.");
        throw std::runtime_error(
            "triggerStopWithSavepoint savepointType CheckpointType can not cast to SavepointType.");
    }
    if (!mainOperator_) {
        INFO_RELEASE("Warn triggerStopWithSavepoint mainOperator is null.");
        return;
    }
    mainOperator_->stop(savepointType->shouldDrain() ? StopMode::DRAIN : StopMode::NO_DRAIN);
}
std::shared_ptr<CompletableFutureV2<bool>> OmniStreamTask::triggerCheckpointAsync(
    std::shared_ptr<CheckpointMetaData> checkpointMetaData, std::shared_ptr<CheckpointOptions> checkpointOptions)
{
    auto result = std::make_shared<CompletableFutureV2<bool>>();
    auto mailboxRunnable =
        std::make_shared<VoidFunctionRunnable>([this, result, checkpointMetaData, checkpointOptions]() {
            try {
                if (IsSynchronous(checkpointOptions->GetCheckpointType())) {
                    triggerStopWithSavepoint(checkpointOptions);
                }
                auto inputGates = env_->GetAllInputGates();
                bool noUnfinishedInputGates =
                    std::all_of(inputGates.begin(), inputGates.end(), [](const std::shared_ptr<InputGate>& gate) {
                        return gate->IsFinished();
                    });
                if (noUnfinishedInputGates) {
                    result->Complete(TriggerCheckpointAsyncInMailbox(checkpointMetaData, checkpointOptions));
                } else {
                    result->Complete(triggerUnfinishedChannelsCheckpoint(checkpointMetaData, checkpointOptions));
                }
            } catch (const std::exception& ex) {
                // Report the failure both via the Future result but also to the mailbox
                // TTODO: result->set_exception(std::current_exception());
                throw;
            }
        });

    mainMailboxExecutor_->execute(mailboxRunnable, "triggerCheckpointInMailbox");
    return result;
}

bool OmniStreamTask::TriggerCheckpointAsyncInMailbox(
    std::shared_ptr<CheckpointMetaData> checkpointMetaData, std::shared_ptr<CheckpointOptions> checkpointOptions)
{
    // TTODO: FlinkSecurityManager::monitorUserSystemExitForCurrentThread();
    try {
        auto currentTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();

        latestAsyncCheckpointStartDelayNanos =
            1000000L * std::max(0L, currentTime - checkpointMetaData->GetTimestamp());

        // No alignment if we inject a checkpoint
        auto checkpointMetrics = std::make_shared<CheckpointMetricsBuilder>();
        checkpointMetrics->SetAlignmentDurationNanos(0L);
        checkpointMetrics->SetBytesProcessedDuringAlignment(0L);
        checkpointMetrics->SetCheckpointStartDelayNanos(latestAsyncCheckpointStartDelayNanos);

        subtaskCheckpointCoordinator->InitInputsCheckpoint(
            checkpointMetaData->GetCheckpointId(), checkpointOptions.get());

        bool success = PerformCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
        if (!success) {
            DeclineCheckpoint(checkpointMetaData->GetCheckpointId());
        }
        return success;
    } catch (...) {
        // propagate exceptions only if the task is still in "running" state
        if (isRunning) {
            LOG("Could not perform checkpoint " << checkpointMetaData->GetCheckpointId()
                                                << " for operator " + getName() + ".");
            throw;
        } else {
            LOG("Could not perform checkpoint {} for operator {} while the invokable was not in state running."
                << checkpointMetaData->GetCheckpointId() << getName());
            return false;
        }
    }
    // TTODO: FlinkSecurityManager::unmonitorUserSystemExitForCurrentThread();
}

bool OmniStreamTask::triggerUnfinishedChannelsCheckpoint(
    std::shared_ptr<CheckpointMetaData> checkpointMetaData, std::shared_ptr<CheckpointOptions> checkpointOptions)
{
    std::optional<CheckpointBarrierHandler*> checkpointBarrierHandler = GetCheckpointBarrierHandler();
    // First make sure the Handler is not empty
    if (checkpointBarrierHandler) {
        CheckpointBarrier barrier(
            checkpointMetaData->GetCheckpointId(), checkpointMetaData->GetTimestamp(), checkpointOptions);

        for (const auto& inputGate : env_->GetAllInputGates()) {
            if (inputGate->IsFinished()) {
                continue;
            }
            for (const auto& channelInfo : inputGate->getUnfinishedChannels()) {
                checkpointBarrierHandler.value()->ProcessBarrier(barrier, channelInfo, true);
            }
        }
        return true;
    }
    return false;
}

// Create processingTimeService which can defer the execution of op.onProcessingTime to mainThread(Mailbox)
ProcessingTimeService* OmniStreamTask::createProcessingTimeService()
{
    ProcessingTimeServiceImpl::ProcessingTimeCallbackWrapper processingTimeCallbackWrapper =
        [this](ProcessingTimeCallback* callback) {
            return deferCallbackToMailBox(this->mainMailboxExecutor_, callback);
        };
    return new ProcessingTimeServiceImpl(this->timerService, std::move(processingTimeCallbackWrapper));
}

ProcessingTimeCallback* OmniStreamTask::deferCallbackToMailBox(
    std::shared_ptr<MailboxExecutor> mailboxExecutor, ProcessingTimeCallback* callback)
{
    class DeferredCallback : public ProcessingTimeCallback {
    public:
        DeferredCallback(std::shared_ptr<MailboxExecutor> mailboxExecutor, ProcessingTimeCallback* callback)
            : mailboxExecutor_(mailboxExecutor),
              callback_(callback)
        {
        }

        void OnProcessingTime(int64_t timestamp) override
        {
            auto mailboxRunnable = std::make_shared<VoidFunctionRunnable>([this, timestamp]() {
                try {
                    this->callback_->OnProcessingTime(timestamp);
                } catch (...) {
                    // todo: realize handleAsyncException
                    THROW_RUNTIME_ERROR("Caught exception while processing timer");
                }
            });

            mailboxExecutor_->execute(mailboxRunnable, "Timer callback");
        }

    private:
        std::shared_ptr<MailboxExecutor> mailboxExecutor_;
        ProcessingTimeCallback* callback_;
    };
    return new DeferredCallback(mailboxExecutor, callback);
}
} // namespace omnistream
