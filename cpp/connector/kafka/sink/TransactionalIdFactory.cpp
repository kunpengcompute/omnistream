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

#include <random>
#include <string>
#include "ctime"
#include "TransactionalIdFactory.h"

const std::string TransactionalIdFactory::TRANSACTIONAL_ID_DELIMITER = "-";

std::string generateRandomString(size_t length)
{
    const std::string chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<size_t> distribution(0, chars.size() - 1);

    std::string randomString;
    for (size_t i = 0; i < length; ++i) {
        randomString += chars[distribution(generator)];
    }

    return randomString;
}

std::string TransactionalIdFactory::buildTransactionalId(
    const std::string& transactionalIdPrefix, int subtaskId, long checkpointOffset)
{
    return transactionalIdPrefix + TRANSACTIONAL_ID_DELIMITER + std::to_string(subtaskId) + TRANSACTIONAL_ID_DELIMITER +
           std::to_string(checkpointOffset) + generateRandomString(randomStrLen);
}
