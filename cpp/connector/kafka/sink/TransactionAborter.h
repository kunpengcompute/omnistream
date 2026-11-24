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

#ifndef FLINK_BENCHMARK_TRANSACTIONABORTER_H
#define FLINK_BENCHMARK_TRANSACTIONABORTER_H
#include <memory>
#include <functional>
#include <vector>
#include "FlinkKafkaInternalProducer.h"

class TransactionAborter {
public:
    explicit TransactionAborter(
        std::function<std::shared_ptr<FlinkKafkaInternalProducer>(const std::string&)> producerFactory,
        std::function<void(std::shared_ptr<FlinkKafkaInternalProducer>)> closeAction);

    void abortLingeringTransactions(const std::vector<std::string>& prefixesToAbort, long startCheckpointId);

    void Close();

    int subtaskId;
    int parallelism;
    std::function<std::shared_ptr<FlinkKafkaInternalProducer>(const std::string&)> producerFactory_;
    std::function<void(std::shared_ptr<FlinkKafkaInternalProducer>)> closeAction_;
    std::shared_ptr<FlinkKafkaInternalProducer> producer = nullptr;

private:
    void abortTransactionsWithPrefix(const std::string& prefix, long startCheckpointId);

    int abortTransactionOfSubtask(const std::string& prefix, long startCheckpointId, int subtaskId);
};
#endif // FLINK_BENCHMARK_TRANSACTIONABORTER_H
