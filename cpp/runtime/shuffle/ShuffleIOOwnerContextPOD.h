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

#ifndef SHUFFLEIOOWNERCONTEXTPOD_H
#define SHUFFLEIOOWNERCONTEXTPOD_H

#include <string>
#include <memory>
#include <sstream>
#include <executiongraph/descriptor/ExecutionAttemptIDPOD.h>

namespace omnistream {
class ShuffleIOOwnerContextPOD {
public:
    ShuffleIOOwnerContextPOD();
    ShuffleIOOwnerContextPOD(const std::string& ownerName, const ExecutionAttemptIDPOD& executionAttemptID);
    ShuffleIOOwnerContextPOD(const ShuffleIOOwnerContextPOD& other);
    ShuffleIOOwnerContextPOD& operator=(const ShuffleIOOwnerContextPOD& other);
    ~ShuffleIOOwnerContextPOD();

    std::string getOwnerName() const;
    void setOwnerName(const std::string& ownerName);
    ExecutionAttemptIDPOD getExecutionAttemptID() const;
    void setExecutionAttemptID(const ExecutionAttemptIDPOD& executionAttemptID);

    bool operator==(const ShuffleIOOwnerContextPOD& other) const
    {
        return ownerName == other.getOwnerName() && executionAttemptID == other.getExecutionAttemptID();
    }

    std::string toString() const;

private:
    std::string ownerName;
    ExecutionAttemptIDPOD executionAttemptID;
};
} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::ShuffleIOOwnerContextPOD> {
    std::size_t operator()(const omnistream::ShuffleIOOwnerContextPOD& obj) const
    {
        size_t h1 = std::hash<std::string>{}(obj.getOwnerName());
        size_t h2 = hash_value(obj.getExecutionAttemptID());
        return h1 ^ (h2 << 1);
    }
};
} // namespace std

#endif
