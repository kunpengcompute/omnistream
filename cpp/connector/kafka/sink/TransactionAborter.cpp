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

#include <stdexcept>
#include <utility>
#include "TransactionalIdFactory.h"
#include "TransactionAborter.h"

TransactionAborter::TransactionAborter(
    std::function<std::shared_ptr<FlinkKafkaInternalProducer>(const std::string&)> producerFactory,
    std::function<void(std::shared_ptr<FlinkKafkaInternalProducer>)> closeAction)
    : producerFactory_(std::move(producerFactory)),
      closeAction_(std::move(closeAction))
{
    parallelism = 1;
    subtaskId = 0;
}

void TransactionAborter::abortLingeringTransactions(
    const std::vector<std::string>& prefixesToAbort, long startCheckpointId)
{
    for (const std::string& prefix : prefixesToAbort) {
        abortTransactionsWithPrefix(prefix, startCheckpointId);
    }
}

void TransactionAborter::abortTransactionsWithPrefix(const std::string& prefix, long startCheckpointId)
{
}

int TransactionAborter::abortTransactionOfSubtask(const std::string& prefix, long startCheckpointId, int subId)
{
    int numTransactionAborted = 0;
    for (long checkpointId = startCheckpointId;; checkpointId++, numTransactionAborted++) {
        std::string transactionalId = TransactionalIdFactory::buildTransactionalId(prefix, subId, checkpointId);
        if (producer == nullptr) {
            producer = producerFactory_(transactionalId);
        } else {
            producer->initTransactionId(transactionalId);
        }
        producer->Flush();
        if (producer->getEpoch() == 0) {
            break;
        }
    }
    return numTransactionAborted;
}

void TransactionAborter::Close()
{
    if (producer != nullptr) {
        closeAction_(producer);
    }
}
