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

#ifndef OMNISTREAM_KAFKAVALUEONLYDESERIALIZATIONSCHEMAWRAPPER_H
#define OMNISTREAM_KAFKAVALUEONLYDESERIALIZATIONSCHEMAWRAPPER_H

#include "KafkaRecordDeserializationSchema.h"

class KafkaValueOnlyDeserializationSchemaWrapper : public KafkaRecordDeserializationSchema {
public:
    explicit KafkaValueOnlyDeserializationSchemaWrapper(DeserializationSchema* deserializationSchema)
        : deserializationSchema(deserializationSchema)
    {
    }

    ~KafkaValueOnlyDeserializationSchemaWrapper() override
    {
        delete deserializationSchema;
    }

    void open() override
    {
        deserializationSchema->Open();
    }

    void deserialize(RdKafka::Message* record, Collector* out) override
    {
        deserializationSchema->deserialize(static_cast<const uint8_t*>(record->payload()), record->len(), out);
    }

    void deserialize(std::vector<RdKafka::Message*> recordVec, Collector* out) override
    {
        int size = static_cast<int>(recordVec.size());
        prepareForVecData(size);
        for (auto record : recordVec) {
            valueVec.push_back(static_cast<const uint8_t*>(record->payload()));
            lengthVec.push_back(record->len());
            timeVec.push_back(record->timestamp().timestamp);
        }
        deserializationSchema->deserialize(valueVec, lengthVec, timeVec, out);
    }

    void prepareForVecData(int size)
    {
        valueVec.clear();
        lengthVec.clear();
        timeVec.clear();
        valueVec.reserve(size);
        lengthVec.reserve(size);
        timeVec.reserve(size);
    }

private:
    DeserializationSchema* deserializationSchema;
    std::vector<const uint8_t*> valueVec;
    std::vector<size_t> lengthVec;
    std::vector<int64_t> timeVec;
};

#endif // OMNISTREAM_KAFKAVALUEONLYDESERIALIZATIONSCHEMAWRAPPER_H
