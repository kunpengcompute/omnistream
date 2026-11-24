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

#ifndef OMNIFLINK_KAFKA_SINK_WRITER_STATE_H
#define OMNIFLINK_KAFKA_SINK_WRITER_STATE_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>

#include "connector/kafka/sink/KafkaSink.h"
#include "connector/kafka/sink/KafkaWriter.h"
#include "connector/kafka/sink/KafkaWriterState.h"
#include "connector/kafka/sink/KafkaWriterStateSerializer.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/api/common/state/ListState.h"
#include "core/typeutils/BytePrimitiveArraySerializer.h"
#include "streaming/api/operators/util/SimpleVersionedListState.h"
#include "streaming/api/operators/sink/InitContextImpl.h"
#include "runtime/state/StateInitializationContextImpl.h"

class SinkWriterStateHandler {
public:
    virtual ~SinkWriterStateHandler() = default;

    template <typename K1>
    KafkaWriter* createWriter(InitContextImpl<K1>* initContext, StateInitializationContextImpl* context)
    {
        return nullptr;
    }
    void snapshotState(long checkpointId)
    {
    }
};

class KafkaSinkWriterStateHandler : public SinkWriterStateHandler {
private:
    static std::string writerRawStatesName;
    static ListStateDescriptor<std::vector<uint8_t>> WRITER_RAW_STATES_DESC;

    std::shared_ptr<KafkaWriterStateSerializer> writerStateSerializer;
    std::vector<std::string> previousSinkStateNames;
    KafkaSink* sink;

    std::vector<ListState<KafkaWriterState>*> previousSinkStates;
    std::shared_ptr<SimpleVersionedListState<KafkaWriterState>> writerState;
    KafkaWriter* kafkaWriter;

public:
    explicit KafkaSinkWriterStateHandler(KafkaSink* sink) : sink(sink), kafkaWriter(nullptr)
    {
        if (!sink) {
            throw std::invalid_argument("sink cannot be null");
        }
        writerStateSerializer = std::make_shared<KafkaWriterStateSerializer>();
    }

    virtual ~KafkaSinkWriterStateHandler() override
    {
        for (auto state : previousSinkStates) {
            delete state;
        }
        delete kafkaWriter;
    }

    template <typename K1>
    KafkaWriter* createWriter(InitContextImpl<K1>* initContext, StateInitializationContextImpl* context)
    {
        INFO_RELEASE("savepoint: KafkaSinkWriterStateHandler createWriter");
        auto* operatorStateBackend = static_cast<DefaultOperatorStateBackend*>(context->getOperatorStateBackend());
        auto rawState = operatorStateBackend->getListState(&WRITER_RAW_STATES_DESC);

        writerState = std::make_shared<SimpleVersionedListState<KafkaWriterState>>(rawState, writerStateSerializer);

        if (context->isRestored()) {
            auto states = writerState->get();
            std::vector<KafkaWriterState> statesList;
            if (states) {
                statesList = *states;
            }
            INFO_RELEASE("savepoint: KafkaSinkWriterStateHandler createWriter, statesList size: " << statesList.size());
            kafkaWriter = sink->RestoreWriter(initContext, statesList);
        } else {
            INFO_RELEASE("savepoint: KafkaSinkWriterStateHandler createWriter, create new KafkaWriter");
            kafkaWriter = sink->CreateWriter(initContext);
        }
        return kafkaWriter;
    }

    void snapshotState(long checkpointId)
    {
        if (!kafkaWriter) {
            throw std::runtime_error("kafkaWriter has not been created yet");
        }

        std::vector<KafkaWriterState> currentState = kafkaWriter->snapshotState(checkpointId);
        writerState->update(currentState);

        for (auto state : previousSinkStates) {
            state->clear();
        }
    }
};

inline std::string KafkaSinkWriterStateHandler::writerRawStatesName = "writer_raw_states";

inline ListStateDescriptor<std::vector<uint8_t>> KafkaSinkWriterStateHandler::WRITER_RAW_STATES_DESC(
    KafkaSinkWriterStateHandler::writerRawStatesName, new BytePrimitiveArraySerializer(nullptr));

#endif // OMNIFLINK_KAFKA_SINK_WRITER_STATE_H
