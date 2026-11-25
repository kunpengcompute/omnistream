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

#ifndef OMNIFLINK_COMMITTEROPERATOR_H
#define OMNIFLINK_COMMITTEROPERATOR_H

#include <memory>
#include <vector>
#include <optional>
#include <functional>
#include <limits>

#include "committables/CommittableCollectorSerializer.h"
#include "streaming/api/operators/sink/committables/CommittableMessage.h"
#include "streaming/api/operators/sink/committables/CheckpointCommittableManager.h"
#include "streaming/api/operators/sink/committables/CommittableCollector.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "core/api/common/state/ListState.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/io/SimpleVersionedSerializer.h"
#include "core/typeutils/BytePrimitiveArraySerializer.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "streaming/runtime/tasks/ProcessingTimeCallback.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "streaming/api/watermark/Watermark.h"
#include "runtime/state/OperatorStateBackend.h"
#include "runtime/state/StateInitializationContextImpl.h"
#include "runtime/state/StateSnapshotContextSynchronousImpl.h"
#include "streaming/api/operators/util/SimpleVersionedListState.h"
#include "connector/kafka/sink/KafkaCommittable.h"
#include "connector/kafka/sink/KafkaCommittableSerializer.h"

static std::string committerRawStatesName = "streaming_committer_raw_states";

template <typename CommT = KafkaCommittable>
class CommitterOperator : public OneInputStreamOperator,
                          public AbstractStreamOperator<void*>,
                          public ProcessingTimeCallback {
public:
    static constexpr long RETRY_DELAY = 1000;

    CommitterOperator(ProcessingTimeService* processingTimeService, bool isBatchMode, bool isCheckpointingEnabled)
        : isBatchMode(isBatchMode),
          isCheckpointingEnabled(isCheckpointingEnabled)
    {
        setProcessingTimeService(processingTimeService);
        isDataStream = false;
        endOfInput = false;
        emitDownstream = false;
        committableCollectorState_ = nullptr;
        committer = nullptr;
        kafkaSink_ = nullptr;
    }

    CommitterOperator(bool emitDownstream, bool isBatchMode, bool isCheckpointingEnabled)
        : emitDownstream(emitDownstream),
          isBatchMode(isBatchMode),
          isCheckpointingEnabled(isCheckpointingEnabled)
    {
        isDataStream = false;
        endOfInput = false;
        committableCollectorState_ = nullptr;
        isCheckpointingEnabled = false;
        emitDownstream = false;
    }

    explicit CommitterOperator(bool isBatch) : isDataStream(!isBatch)
    {
        endOfInput = false;
        isBatchMode = false;
        isCheckpointingEnabled = false;
        emitDownstream = false;
        committableCollectorState_ = nullptr;
    }

    void setup(std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        AbstractStreamOperator<void*>::setup(task);
        if (task != nullptr && task->env() != nullptr) {
            int subtaskId = task->env()->taskConfiguration().getIndexOfSubtask();
            int numberOfSubtasks = task->env()->taskConfiguration().getNumberOfSubtasks();
            committableCollector = std::make_shared<CommittableCollector<CommT>>(subtaskId, numberOfSubtasks);
        }
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        INFO_RELEASE(
            "savepoint: CommitterOperator initializeState with initializer, operatorID: "
            << OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<void*>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<void*>::initializeState(initializer, keySerializer);
    }

    void initializeState(StateInitializationContextImpl* context) override
    {
        AbstractStreamOperator<void*>::initializeState(context);

        auto* stateBackend = static_cast<DefaultOperatorStateBackend*>(context->getOperatorStateBackend());
        if (stateBackend != nullptr && committableSerializer != nullptr) {
            auto rawState = stateBackend->getListState<std::vector<uint8_t>>(&STREAMING_COMMITTER_RAW_STATES_DESC);
            auto committableCollectorSerializer = std::make_shared<CommittableCollectorSerializer<CommT>>(
                committableSerializer,
                getRuntimeContext()->getIndexOfThisSubtask(),
                getRuntimeContext()->getNumberOfParallelSubtasks());
            committableCollectorState_ = std::make_shared<SimpleVersionedListState<CommittableCollector<CommT>>>(
                rawState, committableCollectorSerializer);
        }

        if (context->isRestored()) {
            if (committableCollectorState_ != nullptr) {
                auto restoredCollectors = committableCollectorState_->get();
                if (restoredCollectors != nullptr) {
                    for (auto& cc : *restoredCollectors) {
                        committableCollector->Merge(cc);
                    }
                    delete restoredCollectors;
                }
            }
            auto restoredCheckpointId = context->getRestoredCheckpointId();
            if (restoredCheckpointId.has_value()) {
                lastCompletedCheckpointId = static_cast<long>(restoredCheckpointId.value());
            }
            commitAndEmitCheckpoints();
        }
    }

    void snapshotState(StateSnapshotContextSynchronousImpl* context) override
    {
        AbstractStreamOperator<void*>::snapshotState(context);

        if (committableCollectorState_ != nullptr) {
            committableCollectorState_->update(std::vector<CommittableCollector<CommT>>{committableCollector->Copy()});
        }
    }

    void EndInput()
    {
        endOfInput = true;
        if (!isCheckpointingEnabled || isBatchMode) {
            notifyCheckpointComplete(std::numeric_limits<long>::max());
        }
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        AbstractStreamOperator<void*>::notifyCheckpointComplete(checkpointId);

        if (endOfInput) {
            lastCompletedCheckpointId = std::numeric_limits<long>::max();
        } else {
            lastCompletedCheckpointId = std::max(lastCompletedCheckpointId, checkpointId);
        }
        commitAndEmitCheckpoints();
    }

    void notifyCheckpointAborted(long checkpointId) override
    {
        AbstractStreamOperator<void*>::notifyCheckpointAborted(checkpointId);
    }

    void processElement(StreamRecord& element)
    {
        auto message = reinterpret_cast<CommittableMessage<CommT>*>(element.getValue());
        committableCollector->AddMessage(*message);

        auto checkpointId = message->GetCheckpointId();
        if (checkpointId.has_value() && checkpointId.value() <= lastCompletedCheckpointId) {
            commitAndEmitCheckpoints();
        }
    }

    void processElement(StreamRecord* record) override
    {
        if (record == nullptr) {
            return;
        }
        auto message = reinterpret_cast<CommittableMessage<CommT>*>(record->getValue());
        committableCollector->AddMessage(*message);

        auto checkpointId = message->GetCheckpointId();
        if (checkpointId.has_value() && checkpointId.value() <= lastCompletedCheckpointId) {
            commitAndEmitCheckpoints();
        }
    }

    void processBatch(StreamRecord* record) override
    {
    }

    const char* getName() override
    {
        return "CommitterOperator";
    }

    void ProcessWatermark(Watermark* watermark) override
    {
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
    }

    void close() override
    {
        if (committer != nullptr) {
            committer->Close();
        }
        AbstractStreamOperator<void*>::close();
    }

    void OnProcessingTime(int64_t timestamp) override
    {
        commitAndEmitCheckpoints();
    }

    bool canBeStreamOperator() override
    {
        return isDataStream;
    }

    std::string getTypeName() override
    {
        std::string typeName = "CommitterOperator";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

    void initFromKafkaSink(KafkaSink* kafkaSink)
    {
        kafkaSink_ = kafkaSink;

        auto kafkaCommittableSerializer =
            std::shared_ptr<KafkaCommittableSerializer>(kafkaSink_->getCommittableSerializer());
        committableSerializer = std::dynamic_pointer_cast<SimpleVersionedSerializer<CommT>>(kafkaCommittableSerializer);

        committer = std::shared_ptr<Committer<CommT>>(kafkaSink_->CreateCommitter());

        emitDownstream = false;
    }

private:
    void commitAndEmitCheckpoints()
    {
        bool committed = false;
        do {
            auto managers = committableCollector->getChkComUp(lastCompletedCheckpointId);
            for (const auto& manager : managers) {
                bool fullyReceived = !endOfInput && manager->GetCheckpointId() == lastCompletedCheckpointId;
                commitAndEmit(manager, fullyReceived);
                committed = true;
            }

            if (!committableCollector->IsFinished() && endOfInput) {
                break;
            }
        } while (!committableCollector->IsFinished() && endOfInput);

        if (!committableCollector->IsFinished()) {
            retryWithDelay();
        }
    }

    void commitAndEmit(std::shared_ptr<CheckpointCommittableManager<CommT>> committableManager, bool fullyReceived)
    {
        auto committables = committableManager->commit(fullyReceived, *committer);

        if (emitDownstream && !committables.empty()) {
            if (output != nullptr) {
                auto summary = committableManager->GetSummary();
                output->collect(&summary);
                for (const auto& committable : committables) {
                    output->collect(const_cast<CommittableWithLineage<CommT>*>(&committable));
                }
            }
        }
    }

    void retryWithDelay()
    {
        ProcessingTimeService* service = getProcessingTimeService();
        long scheduledTime = service->getCurrentProcessingTime() + RETRY_DELAY;
        service->registerTimer(scheduledTime, this);
    }

    bool isDataStream;
    std::shared_ptr<SimpleVersionedSerializer<CommT>> committableSerializer;
    std::shared_ptr<Committer<CommT>> committer;
    bool emitDownstream;
    bool isBatchMode;
    bool isCheckpointingEnabled;
    std::shared_ptr<CommittableCollector<CommT>> committableCollector;
    long lastCompletedCheckpointId = -1;
    bool endOfInput;

    KafkaSink* kafkaSink_;

    inline static ListStateDescriptor<std::vector<uint8_t>> STREAMING_COMMITTER_RAW_STATES_DESC{
        committerRawStatesName, new BytePrimitiveArraySerializer(nullptr)};

    std::shared_ptr<SimpleVersionedListState<CommittableCollector<CommT>>> committableCollectorState_;
};

#endif
