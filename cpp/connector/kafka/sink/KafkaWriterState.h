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

#ifndef FLINK_BENCHMARK_KAFKAWRITERSTATE_H
#define FLINK_BENCHMARK_KAFKAWRITERSTATE_H

#include <string>
#include <memory>

class KafkaWriterState {
public:
    explicit KafkaWriterState(const std::string& transactionalIdPrefix);

    std::string getTransactionalIdPrefix() const;

    bool operator==(const KafkaWriterState& that) const;

    bool operator!=(const KafkaWriterState& that) const;

    size_t hashCode() const;

    std::string toString() const;

private:
    std::string transactionalIdPrefix;
};
#endif // FLINK_BENCHMARK_KAFKAWRITERSTATE_H
