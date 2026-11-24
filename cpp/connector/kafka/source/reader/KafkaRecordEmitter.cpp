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

#include "KafkaRecordEmitter.h"
#include <stdexcept>
#include "datagen/meituan/OriginalRecord.h"

KafkaRecordEmitter::KafkaRecordEmitter(KafkaRecordDeserializationSchema* deserializationSchema)
    : deserializationSchema(deserializationSchema),
      sourceOutputWrapper(new SourceOutputWrapper())
{
}

KafkaRecordEmitter::~KafkaRecordEmitter()
{
    delete deserializationSchema;
    delete sourceOutputWrapper;
}

void KafkaRecordEmitter::emitRecord(
    RdKafka::Message* consumerRecord, SourceOutput* output, KafkaPartitionSplitState* splitState)
{
    try {
        sourceOutputWrapper->setSourceOutput(output);
        sourceOutputWrapper->setTimestamp(consumerRecord->timestamp().timestamp);
        deserializationSchema->deserialize(consumerRecord, sourceOutputWrapper);
        splitState->setCurrentOffset(consumerRecord->offset() + 1);
    } catch (const std::exception& e) {
        std::string error = e.what();
        if (error.find("Interrupted while waiting for buffer") != std::string::npos ||
            error.find("Buffer pool request was cancelled") != std::string::npos ||
            error.find("Partition is released") != std::string::npos) {
            throw;
        }
        INFO_RELEASE("Error: Failed to deserialize consumer record due to: " << e.what());
        throw std::runtime_error("Failed to deserialize consumer record due to: " + std::string(e.what()));
    }
}

void KafkaRecordEmitter::emitBatchRecord(
    const std::vector<RdKafka::Message*>& messageVec, SourceOutput* output, KafkaPartitionSplitState* splitState)
{
    try {
        sourceOutputWrapper->setSourceOutput(output);
        sourceOutputWrapper->setTimestamp(messageVec.back()->timestamp().timestamp);
        deserializationSchema->deserialize(messageVec, sourceOutputWrapper);
        splitState->setCurrentOffset(messageVec.back()->offset() + 1);
    } catch (const std::exception& e) {
        std::string error = e.what();
        if (error.find("Interrupted while waiting for buffer") != std::string::npos ||
            error.find("Buffer pool request was cancelled") != std::string::npos ||
            error.find("Partition is released") != std::string::npos) {
            throw;
        }
        INFO_RELEASE("Error: Failed to deserialize consumer batch record due to: " << e.what());
        throw std::runtime_error("Failed to deserialize consumer batch record due to: " + std::string(e.what()));
    }
}
