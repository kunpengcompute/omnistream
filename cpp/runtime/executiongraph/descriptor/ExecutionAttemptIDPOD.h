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

#ifndef OMNISTREAM_EXECUTIONATTEMPTIDPOD_H
#define OMNISTREAM_EXECUTIONATTEMPTIDPOD_H

#include <string>
#include <nlohmann/json.hpp>
#include "../common/AbstractIDPOD.h" // Assuming this header defines AbstractIDPOD
#include "ExecutionGraphIDPOD.h"
#include "runtime/scheduler/strategy/ExecutionVertexIDPOD.h"
#include <stdexcept>

namespace omnistream {

class ExecutionAttemptIDPOD {
public:
    static constexpr int BYTE_BUF_LEN = ExecutionGraphIDPOD::SIZE + ExecutionVertexIDPOD::SIZE + sizeof(int32_t);

    // Default constructor
    ExecutionAttemptIDPOD() = default;

    ExecutionAttemptIDPOD(
        const ExecutionGraphIDPOD& executionGraphId, const ExecutionVertexIDPOD& executionVertexId, int attemptNumber)
        : executionGraphId(executionGraphId),
          executionVertexId(executionVertexId),
          attemptNumber(attemptNumber)
    {
        if (attemptNumber < 0) {
            throw std::invalid_argument("attemptNumber must be >= 0");
        }
    }

    ExecutionVertexIDPOD getExecutionVertexIDPOD()
    {
        return executionVertexId;
    }

    JobVertexID getJobVertexId() const
    {
        return executionVertexId.getJobVertexId();
    }

    int32_t getSubtaskIndex() const
    {
        return executionVertexId.getSubtaskIndex();
    }

    int32_t getAttempNumber()
    {
        return attemptNumber;
    }

    static int getByteBufLength()
    {
        return BYTE_BUF_LEN;
    }

    friend bool operator==(const ExecutionAttemptIDPOD& lhs, const ExecutionAttemptIDPOD& rhs)
    {
        return lhs.executionGraphId == rhs.executionGraphId && lhs.executionVertexId == rhs.executionVertexId &&
               lhs.attemptNumber == rhs.attemptNumber;
    }

    friend bool operator!=(const ExecutionAttemptIDPOD& lhs, const ExecutionAttemptIDPOD& rhs)
    {
        return !(lhs == rhs);
    }

    friend std::size_t hash_value(const ExecutionAttemptIDPOD& obj)
    {
        std::size_t seed = 0x2A9B61C8;
        seed ^= (seed << 6) + (seed >> 2) + 0x21D57D88 + hash_value(obj.executionGraphId);
        seed ^= (seed << 6) + (seed >> 2) + 0x13579BDF + hash_value(obj.executionVertexId);
        seed ^= (seed << 6) + (seed >> 2) + 0x2468ACE0 + static_cast<std::size_t>(obj.attemptNumber);
        return seed;
    }
    // toString method
    std::string toString() const
    {
        return executionGraphId.toString() + "_" + executionVertexId.toString() + "_" + std::to_string(attemptNumber);
    }

    static ExecutionAttemptIDPOD randomId()
    {
        ExecutionGraphIDPOD graphId;
        ExecutionVertexIDPOD vertexId{JobVertexID(0, 0), 0};

        return ExecutionAttemptIDPOD(graphId, vertexId, 0);
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ExecutionAttemptIDPOD, executionGraphId, executionVertexId, attemptNumber)
private:
    ExecutionGraphIDPOD executionGraphId;
    ExecutionVertexIDPOD executionVertexId;
    int32_t attemptNumber;
};

} // namespace omnistream
namespace std {
template <>
struct hash<omnistream::ExecutionAttemptIDPOD> {
    std::size_t operator()(const omnistream::ExecutionAttemptIDPOD& obj) const
    {
        return hash_value(obj);
    }
};
} // namespace std

#endif // OMNISTREAM_EXECUTIONATTEMPTIDPOD_H
