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

#ifndef OMNIFLINK_KAFKACOMMITTER_H
#define OMNIFLINK_KAFKACOMMITTER_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include "FlinkKafkaInternalProducer.h"
#include "KafkaCommittable.h"
#include "Recyclable.h"
#include "streaming/api/operators/sink/committables/Committer.h"

class KafkaCommitter : public Committer<KafkaCommittable> {
public:
    explicit KafkaCommitter(const RdKafka::Conf* kafkaProducerConfig);
    void Commit(std::vector<std::shared_ptr<CommitRequest<KafkaCommittable>>>& requests);
    void Close();

private:
    RdKafka::Conf* kafkaProducerConfig;
    std::shared_ptr<FlinkKafkaInternalProducer> recoveryProducer;
    std::shared_ptr<FlinkKafkaInternalProducer> GetRecoveryProducer(KafkaCommittable committable);
};

#endif // OMNIFLINK_KAFKACOMMITTER_H
