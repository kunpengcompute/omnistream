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

#include "KafkaCommitter.h"

KafkaCommitter::KafkaCommitter(const RdKafka::Conf* kafkaProducerConfig)
    : kafkaProducerConfig(const_cast<RdKafka::Conf*>(kafkaProducerConfig)),
      recoveryProducer(nullptr)
{
}

void KafkaCommitter::Commit(std::vector<std::shared_ptr<CommitRequest<KafkaCommittable>>>& requests)
{
    for (auto& request : requests) {
        const KafkaCommittable& committable = request->GetCommittable();
        const std::string& transactionalId = committable.GetTransactionalId();
        auto recyclable = committable.GetProducer().has_value() ? committable.GetProducer().value() : nullptr;
        FlinkKafkaInternalProducer* producer =
            recyclable ? recyclable->GetObject() : GetRecoveryProducer(committable).get();
        try {
            producer->CommitTransaction();
            producer->Flush();
            if (recyclable) {
                recyclable->Close();
            }
        } catch (const std::exception& e) {
            if (recyclable) {
                recyclable->Close();
            }
            request->signalFailedWithUnknownReason(e);
        }
    }
}

void KafkaCommitter::Close()
{
    if (recoveryProducer) {
        recoveryProducer->Close();
    }
}

std::shared_ptr<FlinkKafkaInternalProducer> KafkaCommitter::GetRecoveryProducer(KafkaCommittable committable)
{
    if (recoveryProducer == nullptr) {
        recoveryProducer =
            std::make_shared<FlinkKafkaInternalProducer>(kafkaProducerConfig, committable.GetTransactionalId());
    } else {
        recoveryProducer->setTransactionId(committable.GetTransactionalId());
    }
    recoveryProducer->resumeTransaction(committable.GetProducerId(), committable.GetEpoch());
    return recoveryProducer;
}
