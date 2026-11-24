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

#ifndef OMNIFLINK_KAFKACOMMITTABLE_H
#define OMNIFLINK_KAFKACOMMITTABLE_H

#include <memory>
#include <string>
#include <optional>
#include <functional>
#include <librdkafka/rdkafkacpp.h>
#include "FlinkKafkaInternalProducer.h"
#include "Recyclable.h"

class KafkaCommittable {
public:
    KafkaCommittable(
        long producerId,
        short epoch,
        const std::string& transactionalId,
        std::shared_ptr<Recyclable<FlinkKafkaInternalProducer>> producer);

    static KafkaCommittable of(
        FlinkKafkaInternalProducer* producer, std::function<void(FlinkKafkaInternalProducer*)> recycler);

    long GetProducerId() const;
    short GetEpoch() const;
    const std::string& GetTransactionalId() const;
    std::optional<std::shared_ptr<Recyclable<FlinkKafkaInternalProducer>>> GetProducer() const;

    std::string toString() const;

    bool operator==(const KafkaCommittable& that) const;
    bool operator!=(const KafkaCommittable& that) const;

private:
    long producerId;
    short epoch;
    std::string transactionalId;
    std::shared_ptr<Recyclable<FlinkKafkaInternalProducer>> producer;
};

#endif // OMNIFLINK_KAFKACOMMITTABLE_H
