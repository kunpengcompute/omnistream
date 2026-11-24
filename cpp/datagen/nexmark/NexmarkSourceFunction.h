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

#ifndef OMNISTREAM_NEXMARKSOURCEFUNCTION_H
#define OMNISTREAM_NEXMARKSOURCEFUNCTION_H

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <list>
#include <memory>
#include <mutex>
#include <vector>
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/typeinfo/TypeInformation.h"
#include "core/typeutils/LongSerializer.h"
#include "functions/SourceFunction.h"
#include "functions/AbstractRichFunction.h"
#include "GeneratorConfig.h"
#include "generator/NexmarkGenerator.h"
#include "../source/EventDeserializer.h"
#include "table/runtime/operators/source/InputFormatSourceFunction.h"
#include "functions/Configuration.h"
#include "streaming/api/checkpoint/CheckpointedFunction.h"
#include "core/api/common/state/ListState.h"
#include "runtime/state/DefaultOperatorStateBackend.h"

template <typename K>
class NexmarkSourceFunction : public SourceFunction<K>, public AbstractRichFunction, public CheckpointedFunction {
    // Configuration for generator to use when reading synthetic events. May be split.
    GeneratorConfig config;

    EventDeserializer* deserializer;

    TypeInformation* resultType;

    // Transient generator pointer.
    std::unique_ptr<NexmarkGenerator> generator;

    // Number of events contained in batches successfully emitted downstream. Checkpointing this
    // committed position (rather than the generator position) makes a partially built batch replayable.
    long numCommittedEvents;

    // Flag to make the source cancelable.
    std::atomic_bool isRunning;
    std::mutex cancelMutex;
    std::condition_variable cancelCondition;

    // Transient checkpointed state.
    std::shared_ptr<ListState<long>> checkpointedState;

public:
    NexmarkSourceFunction(const GeneratorConfig& config, EventDeserializer* deserializer, TypeInformation* resultType)
        : config(config),
          deserializer(deserializer),
          resultType(resultType),
          generator(nullptr),
          numCommittedEvents(0),
          isRunning(true)
    {
    }

    // StreamOperatorFactory hands both pointers over with `new` and keeps no reference to them,
    // so the source function owns them. Without this, every job submission leaked one
    // deserializer (and the batch it was still filling) and one TypeInformation per source subtask.
    ~NexmarkSourceFunction() override
    {
        delete deserializer;
        delete resultType;
    }

    // Overriding open method.
    void open(const Configuration& parameters) override
    {
        AbstractRichFunction::open(parameters);
        // initializeState() creates the generator when restoring. Do not overwrite its restored offset.
        if (this->generator == nullptr) {
            this->generator = std::make_unique<NexmarkGenerator>(getSubGeneratorConfig());
        }
    }

    // Private method to get sub-generator config.
    GeneratorConfig getSubGeneratorConfig()
    {
        int parallelism = this->getRuntimeContext()->getNumberOfParallelSubtasks();
        int taskId = this->getRuntimeContext()->getIndexOfThisSubtask();
        std::vector<GeneratorConfig> splits = config.split(parallelism);
        return splits.at(taskId);
    }

    void initializeState()
    {
        // checkpoint stuff
    }

    // Overriding run method.
    void run(SourceContext* ctx) override
    {
        while (isRunning.load() && generator->hasNext()) {
            long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
            NexmarkGenerator::NextEvent nextEvent = generator->nextEvent();
            if (nextEvent.wallclockTimestamp > now) {
                const auto waitDuration = std::chrono::milliseconds(nextEvent.wallclockTimestamp - now);
                std::unique_lock<std::mutex> lock(cancelMutex);
                cancelCondition.wait_for(lock, waitDuration, [this]() { return !isRunning.load(); });
            }
            if (!isRunning.load()) {
                break;
            }

            auto next = deserializer->deserialize(std::move(nextEvent.event));
            {
                std::lock_guard<std::recursive_mutex> checkpointLock(ctx->getCheckpointLock()->mutex);
                if (!isRunning.load()) {
                    break;
                }
                // Only do output when a batch is prepared
                if (next) {
                    // Advance the recoverable position only after the complete batch was accepted.
                    // A collect failure must fail the task; continuing would silently lose the batch.
                    ctx->collect(next->getValue());
                    numCommittedEvents = generator->getEventsCountSoFar();
                }
            }
        }
        // A partial batch is emitted only after the generator is exhausted. Cancel leaves it uncommitted
        // so checkpoint recovery can generate those events again.
        if (isRunning.load() && !generator->hasNext()) {
            auto tail = deserializer->finish();
            if (tail != nullptr) {
                std::lock_guard<std::recursive_mutex> checkpointLock(ctx->getCheckpointLock()->mutex);
                if (isRunning.load()) {
                    ctx->collect(tail->getValue());
                    numCommittedEvents = generator->getEventsCountSoFar();
                } else {
                    delete static_cast<omnistream::VectorBatch*>(tail->getValue());
                    tail->setValue(nullptr);
                }
            }
        }
    }

    // Overriding cancel method.
    void cancel() override
    {
        {
            std::lock_guard<std::mutex> lock(cancelMutex);
            isRunning.store(false);
        }
        cancelCondition.notify_all();
    }

    // Overriding close method.
    void close() override
    {
        AbstractRichFunction::close();
    }

    // Overriding getProducedType method.
    TypeInformation* getProducedType()
    {
        return resultType;
    }

    void snapshotState(StateSnapshotContextSynchronousImpl* context) override
    {
        this->checkpointedState->clear();
        this->checkpointedState->add(numCommittedEvents);
    }

    void initializeState(StateInitializationContextImpl* context) override
    {
        std::string stateName = "elements-count-state";
        // The backend copies the descriptor's name and keeps its serializer, but never the descriptor
        // itself, so it can live on the stack. ~StateDescriptor does not touch the serializer.
        ListStateDescriptor<long> descriptor(stateName, new LongSerializer());
        auto* stateBackend = static_cast<DefaultOperatorStateBackend*>(context->getOperatorStateBackend());
        this->checkpointedState = stateBackend->template getListState<long>(&descriptor);

        if (context->isRestored()) {
            std::vector<long> retrievedStates;
            for (auto const& entry : *this->checkpointedState->get()) {
                retrievedStates.push_back(entry);
            }
            if (retrievedStates.size() != 1) {
                throw std::runtime_error("NexmarkSourceFunction retrieve invalid state.");
            }
            const long restoredOffset = retrievedStates[0];
            const GeneratorConfig subGeneratorConfig = getSubGeneratorConfig();
            if (restoredOffset < 0 || restoredOffset > subGeneratorConfig.maxEvents) {
                throw std::runtime_error(
                    "NexmarkSourceFunction restored offset is outside the sub-generator range: " +
                    std::to_string(restoredOffset));
            }
            numCommittedEvents = restoredOffset;
            INFO_RELEASE("NexmarkSourceFunction::initializeState, restoredOffset: " << restoredOffset);
            // Make the first restored event immediately eligible while retaining the configured rate
            // for subsequent events. Passing -1 here would delay the first event by the elapsed event
            // time represented by restoredOffset.
            const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
            const int64_t restoredEventTime =
                subGeneratorConfig.timestampForEvent(subGeneratorConfig.nextEventNumber(restoredOffset));
            const int64_t restoredWallclockBaseTime = now - (restoredEventTime - subGeneratorConfig.baseTime);
            this->generator =
                std::make_unique<NexmarkGenerator>(subGeneratorConfig, restoredOffset, restoredWallclockBaseTime);
        }
    }
};

#endif // OMNISTREAM_NEXMARKSOURCEFUNCTION_H
