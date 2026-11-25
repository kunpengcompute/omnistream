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

#ifndef FLINK_BENCHMARK_TRANSACTIONALIDFACTORY_H
#define FLINK_BENCHMARK_TRANSACTIONALIDFACTORY_H

#include <string>

class TransactionalIdFactory {
public:
    static std::string buildTransactionalId(
        const std::string& transactionalIdPrefix, int subtaskId, long checkpointOffset);

private:
    static const std::string TRANSACTIONAL_ID_DELIMITER;
    static const int randomStrLen = 6;
};

#endif // FLINK_BENCHMARK_TRANSACTIONALIDFACTORY_H
