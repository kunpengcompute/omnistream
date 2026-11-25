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

#ifndef FLINK_TNEL_KAFKARECORDEMITTER_H
#define FLINK_TNEL_KAFKARECORDEMITTER_H

#include <vector>
#include <string_view>
#include "../split/KafkaPartitionSplitState.h"
#include "functions/Collector.h"
#include "RecordEmitter.h"
#include "connector/kafka/source/reader/deserializer/KafkaRecordDeserializationSchema.h"
#include "table/data/vectorbatch/VectorBatch.h"

class KafkaRecordEmitter : public RecordEmitter<RdKafka::Message, KafkaPartitionSplitState> {
public:
    ~KafkaRecordEmitter() override;

    explicit KafkaRecordEmitter(KafkaRecordDeserializationSchema* deserializationSchema);

    void emitRecord(
        RdKafka::Message* consumerRecord, SourceOutput* output, KafkaPartitionSplitState* splitState) override;

    void emitBatchRecord(
        const std::vector<RdKafka::Message*>& messageVec,
        SourceOutput* output,
        KafkaPartitionSplitState* splitState) override;

private:
    class SourceOutputWrapper : public Collector {
    public:
        void collect(void* record) override
        {
            curSourceOutput->Collect(record, curTimestamp);
        }

        void close() override
        {
        }

        void setSourceOutput(SourceOutput* sourceOutput)
        {
            curSourceOutput = sourceOutput;
        }

        void setTimestamp(long timestamp)
        {
            this->curTimestamp = timestamp;
        }

    private:
        SourceOutput* curSourceOutput;
        long curTimestamp;
    };

    KafkaRecordDeserializationSchema* deserializationSchema;
    SourceOutputWrapper* sourceOutputWrapper;
};

#endif // FLINK_TNEL_KAFKARECORDEMITTER_H
