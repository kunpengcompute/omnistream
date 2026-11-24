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

#ifndef FLINK_TNEL_KAFKARECORDDESERIALIZATIONSCHEMA_H
#define FLINK_TNEL_KAFKARECORDDESERIALIZATIONSCHEMA_H

#include <memory>
#include <vector>
#include <map>
#include <stdexcept>
#include <iostream>
#include "connector/kafka/source/reader/RdKafkaConsumer.h"
#include "DynamicKafkaDeserializationSchema.h"
#include "core/api/common/serialization/DeserializationSchema.h"

class KafkaRecordDeserializationSchema {
public:
    virtual ~KafkaRecordDeserializationSchema() = default;

    virtual void open() = 0;

    //    virtual void deserialize(std::vector<RdKafka::Message*> recordVec, Collector* out) = 0;
    virtual void deserialize(RdKafka::Message* recordVec, Collector* out) = 0;

    virtual void deserialize(std::vector<RdKafka::Message*> recordVec, Collector* out) = 0;

    static KafkaRecordDeserializationSchema* valueOnly(DeserializationSchema* valueDeserializationSchema);
};

#endif // FLINK_TNEL_KAFKARECORDDESERIALIZATIONSCHEMA_H
