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

#ifndef OMNIFLINK_SINKWRITEROPERATOR_H
#define OMNIFLINK_SINKWRITEROPERATOR_H

#include <vector>
#include <memory>
#include <optional>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <limits>
#include "core/include/common.h"
#include "connector/kafka/sink/KafkaSink.h"
#include "connector/kafka/sink/KafkaWriter.h"
#include "connector/kafka/sink/KafkaCommittable.h"
#include "connector/kafka/sink/KafkaCommittableSerializer.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/sink/KafkaSinkWriterStateHandler.h"
#include "streaming/api/operators/sink/InitContextImpl.h"

class SinkWriterOperator : public OneInputStreamOperator, public AbstractStreamOperator<void*> {
public:
    static ListStateDescriptor<std::vector<uint8_t>> STREAMING_COMMITTER_RAW_STATES_DESC;

    SinkWriterOperator(KafkaSink* kafkaSink, const nlohmann::json& config);

    ~SinkWriterOperator()
    {
        close();
    }

    void initializeState(StateInitializationContextImpl* context) override;

    void snapshotState(StateSnapshotContextSynchronousImpl* context) override;

    void open() override;

    void close() override;

    RowData* getOutputEntireRow(omnistream::VectorBatch* batch, int rowId);

    void processBatch(StreamRecord* record) override;

    void processElement(StreamRecord* record) override;

    void EndInput();

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override;

    void notifyCheckpointComplete(long checkpointId) override;

    void notifyCheckpointAborted(long checkpointId) override;

    void ProcessWatermark(Watermark* watermark) override;

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override;

    bool canBeStreamOperator() override
    {
        return isDataStream;
    }

    std::string getTypeName() override;

    KafkaSink* getKafkaSink()
    {
        return kafkaSink;
    }

private:
    template <typename K>
    InitContextImpl<K>* createInitContext(std::optional<uint64_t> restoredCheckpointId);
    template <typename CommT>
    void emitCommittables(std::int64_t checkpointId);
    template <typename CommT>
    void emit(
        int indexOfThisSubtask,
        int numberOfParallelSubtasks,
        std::int64_t checkpointId,
        const std::vector<CommT>& committables);
    KafkaCommittableSerializer* committableSerializer{};
    bool emitDownstream{};
    std::int64_t currentWatermark{};
    KafkaSink* kafkaSink = nullptr;
    KafkaWriter* sinkWriter = nullptr;
    bool endOfInput = false;
    nlohmann::json description;
    std::vector<std::string> inputTypes;

    ProcessingTimeServiceImpl* processingTimeService = nullptr;
    KafkaSinkWriterStateHandler* writerStateHandler = nullptr;
    std::vector<KafkaCommittable> legacyCommittables;
    bool isDataStream;
    bool closed_ = false;
    int32_t subtaskIndex;
};
#endif // OMNIFLINK_SINKWRITEROPERATOR_H
