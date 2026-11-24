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

#ifndef OPERATORCHAIN_V2_H
#define OPERATORCHAIN_V2_H
#include <executiongraph/StreamConfigPOD.h>
#include <executiongraph/StreamEdgePOD.h>
#include <executiongraph/operatorchain/OperatorChainPOD.h>
#include <streaming/runtime/io/RecordWriterOutputV2.h>
#include <streaming/runtime/io/RecordWriterOutput.h>
#include <streaming/api/operators/StreamOperatorFactory.h>
#include <streaming/api/operators/StreamOperator.h>
#include "StreamOperatorWrapper.h"
#include <typeinfo/TypeInformation.h>
#include "runtime/io/network/api/writer/V2/RecordWriterDelegateV2.h"
#include "runtime/io/network/api/writer/RecordWriterDelegate.h"

#include "CopyingBroadcastingOutputCollector.h"
#include "taskmanager/OmniRuntimeEnvironment.h"

#include "mailbox/StreamTaskActionExecutor.h"
#include "runtime/operators/TableOperatorConstants.h"
#include "runtime/operators/coordination/OperatorEventHandler.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"
#include "runtime/checkpoint/CheckpointMetaData.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "runtime/event/AbstractEvent.h"
#include "runtime/jobgraph/OperatorID.h"
#include "core/utils/function/Supplier.h"
#include "runtime/operators/coordination/AcknowledgeCheckpointEvent.h"
#include "runtime/operators/coordination/OperatorEventDispatcher.h"
namespace omnistream {
// forward declaration
class OmniStreamTask;

class OperatorChainV2 {
public:
    OperatorChainV2(
        std::weak_ptr<OmniStreamTask> containingTask, std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate);

    OperatorChainV2(
        std::shared_ptr<RuntimeEnvironmentV2> env,
        std::shared_ptr<omnistream::datastream::RecordWriterDelegate> recordWriterDelegate)
    {
        TaskInformationPOD taskConfiguration = env->taskConfiguration();

        auto configuration = taskConfiguration.getStreamConfigPOD();
        auto outputsInOrder = configuration.getOutEdgesInOrder();
        auto chainedConfigs = taskConfiguration.getChainedConfigMap();

        std::unordered_map<int, datastream::RecordWriterOutput*> recordWriterOutputs;
        createDataStreamChainOutputs(outputsInOrder, chainedConfigs, recordWriterDelegate, recordWriterOutputs);

        std::vector<StreamOperatorWrapper*> allOperatorWrappers;
        this->mainOperatorOutput =
            createDataStreamOutputCollector(configuration, chainedConfigs, recordWriterOutputs, allOperatorWrappers);

        auto opDesc = configuration.getOperatorDescription();
        auto chainedOperator = StreamOperatorFactory::createOperatorAndCollector(opDesc, mainOperatorOutput, nullptr);
        auto operatorWrapper = new StreamOperatorWrapper(chainedOperator, false);
        this->mainOperatorWrapper = operatorWrapper;
        allOperatorWrappers.emplace_back(operatorWrapper);
        if (allOperatorWrappers.size() != 0) {
            this->tailOperatorWrapper = allOperatorWrappers[0];
        } else {
            this->tailOperatorWrapper = mainOperatorWrapper;
        }
        linkOperatorWrappers(allOperatorWrappers);

        operatorDependenciesDeal();

        operatorEventDispatcher = new OperatorEventDispatcherImpl(env->getOperatorCoordinatorEventGateway());
    }

    OperatorChainV2(
        std::shared_ptr<RuntimeEnvironmentV2> env, std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate)
    {
        TaskInformationPOD taskConfiguration = env->taskConfiguration();

        auto configuration = taskConfiguration.getStreamConfigPOD();
        auto outputsInOrder = configuration.getOutEdgesInOrder();
        auto chainedConfigs = taskConfiguration.getChainedConfigMap();

        std::unordered_map<int, RecordWriterOutputV2*> recordWriterOutputs;
        createChainOutputs(outputsInOrder, chainedConfigs, recordWriterDelegate, recordWriterOutputs);

        std::vector<StreamOperatorWrapper*> allOperatorWrappers;
        this->mainOperatorOutput =
            createOutputCollector(nullptr, configuration, chainedConfigs, recordWriterOutputs, allOperatorWrappers);

        auto opDesc = configuration.getOperatorDescription();
        auto chainedOperator = StreamOperatorFactory::createOperatorAndCollector(opDesc, mainOperatorOutput, nullptr);

        registerHandler(opDesc, chainedOperator);

        auto operatorWrapper = new StreamOperatorWrapper(chainedOperator, false);
        this->mainOperatorWrapper = operatorWrapper;
        allOperatorWrappers.emplace_back(operatorWrapper);
        if (!allOperatorWrappers.empty()) {
            this->tailOperatorWrapper = allOperatorWrappers[0];
        } else {
            this->tailOperatorWrapper = mainOperatorWrapper;
        }
        linkOperatorWrappers(allOperatorWrappers);
    }

    ~OperatorChainV2()
    {
        delete mainOperatorWrapper;
        mainOperatorWrapper = nullptr;
        tailOperatorWrapper = nullptr;
        // Outputs the chain allocated itself. RecordWriterOutputs are deliberately absent: they
        // are owned via the recordWriterOutputs map, so deleting them here would double free.
        for (auto* output : ownedOutputs_) {
            delete output;
        }
        ownedOutputs_.clear();
    }

    void finishOperators(StreamTaskActionExecutor* actionExecutor);

    StreamOperator* createMainOperatorAndCollector(OperatorChainPOD& opChainConfig, RecordWriterOutputV2* chainOutput);

    void initializeStateAndOpenOperators(
        StreamTaskStateInitializerImpl* initializer, const TaskInformationPOD& taskConfiguration_);

    [[nodiscard]] StreamOperator* getMainOperator() const
    {
        return mainOperatorWrapper->getStreamOperator();
    }

    [[nodiscard]] WatermarkGaugeExposingOutput* GetMainOperatorOutput() const
    {
        return mainOperatorOutput;
    }

    bool IsClosed() const
    {
        return isClosed_;
    }

    virtual void CloseAllOperators()
    {
        isClosed_ = true;
    }

    void AlignedBarrierTimeout(long checkpointId)
    {
        // TTODO
    }

    void broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent)
    {
        LOG_DEBUG("broadcastEvent");
        for (RecordWriterOutputV2* streamOutput : streamOutputs) {
            streamOutput->broadcastEvent(event, isPriorityEvent);
        }
    };

    void broadcastEvent(std::shared_ptr<AbstractEvent> event)
    {
        broadcastEvent(event, false);
    };

    const std::vector<RecordWriterOutputV2*>& getStreamOutputs() const
    {
        return streamOutputs;
    }

    void DispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString);

    // Snapshot related functions
    void PrepareSnapshotPreBarrier(long checkpointId);

    void NotifyCheckpointComplete(long checkpointId);
    void NotifyCheckpointAborted(long checkpointId);
    void NotifyCheckpointSubsumed(long checkpointId);
    void SnapshotState(
        std::unordered_map<OperatorID, OperatorSnapshotFutures*>* operatorSnapshotsInProgress,
        CheckpointMetaData& checkpointMetaData,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<Supplier<bool>> isRunning,
        std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> channelStateWriteResult,
        std::shared_ptr<CheckpointStreamFactory> storage,
        const std::shared_ptr<OmniTaskBridge>& bridge);
    bool IsTaskDeployedAsFinished()
    {
        return false;
    }

    // 预检查：遍历链中所有算子，返回不支持 compatible savepoint 的原因列表（空串表示全部支持）。
    std::string checkCompatibleSavepointSupport();

protected:
    OperatorChainV2()
        : isClosed_(false),
          mainOperatorOutput(nullptr),
          mainOperatorWrapper(nullptr),
          tailOperatorWrapper(nullptr),
          operatorEventDispatcher(nullptr)
    {
    }

    bool isClosed_ = false;
    // WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput;
    WatermarkGaugeExposingOutput* mainOperatorOutput;
    // Every output this chain constructs with new, freed in the destructor.
    std::vector<WatermarkGaugeExposingOutput*> ownedOutputs_;

    std::unordered_map<std::string, OperatorEventHandler*> handlers;

    // ownership is mainOperatorWrapper own its next, wrapper also own its op, op own its output
    StreamOperatorWrapper* mainOperatorWrapper;

    std::vector<RecordWriterOutputV2*> streamOutputs;

    // weak ref,
    StreamOperatorWrapper* tailOperatorWrapper;

    OperatorEventDispatcherImpl* operatorEventDispatcher;

    void SnapshotChannelStates(
        StreamOperator* op,
        std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> channelStateWriteResult,
        OperatorSnapshotFutures* snapshotInProgress);

    void SendAcknowledgeCheckpointEvent(long checkpointId);

    ReadIterator getAllOperators(bool reverse)
    {
        return reverse ? ReadIterator(tailOperatorWrapper, true) : ReadIterator(mainOperatorWrapper, false);
    }

    void operatorDependenciesDeal();

private:
    // future the following function should be private and the logic will be refactory
    void createChainOutputs(
        std::vector<StreamEdgePOD>& outputsInOrder,
        std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
        std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate,
        std::unordered_map<int, RecordWriterOutputV2*>& recordWriterOutputs);
    RecordWriterOutputV2* createStreamOutput(
        RecordWriterV2* recordWriter, TypeInformation& typeInformation, const NonChainedOutputPOD& streamOutput);

    void createDataStreamChainOutputs(
        std::vector<StreamEdgePOD>& outputsInOrder,
        std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
        std::shared_ptr<datastream::RecordWriterDelegate> recordWriterDelegate,
        std::unordered_map<int, datastream::RecordWriterOutput*>& recordWriterOutputs);
    datastream::RecordWriterOutput* createDataStreamStreamOutput(
        datastream::RecordWriter* recordWriter, TypeInformation& typeInformation);

    TypeInformation* getChainOutputType(OperatorPOD operatorPod);
    TypeInformation* getDataStreamStateKeyType(OperatorPOD operatorPod);
    TypeInformation* getDataStreamChainOutputType(OperatorPOD operatorPod);
    static WatermarkGaugeExposingOutput* wrapOperatorIntoOutput(StreamOperator* op, omnistream::OperatorPOD& opConfig);
    WatermarkGaugeExposingOutput* createOperatorChain(
        const std::shared_ptr<OmniStreamTask>& streamTask,
        StreamConfigPOD* operatorConfig,
        std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
        std::unordered_map<int, RecordWriterOutputV2*>& recordWriterOutputs,
        std::vector<StreamOperatorWrapper*>& allOperatorWrappers);

    WatermarkGaugeExposingOutput* createOutputCollector(
        const std::shared_ptr<OmniStreamTask>& streamTask,
        StreamConfigPOD& operatorConfig,
        std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
        std::unordered_map<int, RecordWriterOutputV2*>& recordWriterOutputs,
        std::vector<StreamOperatorWrapper*>& allOperatorWrappers);
    static void linkOperatorWrappers(std::vector<StreamOperatorWrapper*>& allOperatorWrappers);

    WatermarkGaugeExposingOutput* createDataStreamOutputCollector(
        StreamConfigPOD& operatorConfig,
        std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
        std::unordered_map<int, datastream::RecordWriterOutput*>& recordWriterOutputs,
        std::vector<StreamOperatorWrapper*>& allOperatorWrappers);
    WatermarkGaugeExposingOutput* createDataStreamOperatorChain(
        StreamConfigPOD& operatorConfig,
        std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
        std::unordered_map<int, datastream::RecordWriterOutput*>& recordWriterOutputs,
        std::vector<StreamOperatorWrapper*>& allOperatorWrappers);

    void registerHandler(OperatorPOD& opDesc, StreamOperator* streamOperator)
    {
        auto id = opDesc.getId();
        if (id == OPERATOR_NAME_SOURCE_OPERATOR) {
            if (auto* handler = dynamic_cast<OperatorEventHandler*>(streamOperator)) {
                handlers.emplace(opDesc.getOperatorId(), handler);
            } else {
                LOG("Unsupported source operator of csv");
            }
        }
    }

    OperatorSnapshotFutures* BuildOperatorSnapshotFutures(
        CheckpointMetaData checkpointMetaData,
        CheckpointOptions* checkpointOptions,
        StreamOperator* op,
        std::shared_ptr<Supplier<bool>> isRunning,
        std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> channelStateWriteResult,
        std::shared_ptr<CheckpointStreamFactory> storage,
        const std::shared_ptr<OmniTaskBridge>& bridge);

    OperatorSnapshotFutures* CheckpointStreamOperator(
        StreamOperator* op,
        CheckpointMetaData checkpointMetaData,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<CheckpointStreamFactory> storageLocation,
        std::shared_ptr<Supplier<bool>> isRunning,
        const std::shared_ptr<OmniTaskBridge>& bridge);
    // std::vector<OperatorConfig> & opChainConfig_;
    // std::shared_ptr<RecordWriterDelegate> recordWriterDelegate_;
};
}; // namespace omnistream

#endif // OPERATORCHAIN_H
