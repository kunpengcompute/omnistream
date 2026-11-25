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

#ifndef OMNISTREAM_EXECUTIONVERTEX_H
#define OMNISTREAM_EXECUTIONVERTEX_H

#include <cstdint>
#include "jobgraph/JobVertexID.h"

namespace omnistream {

class ExecutionVertexIDPOD {
public:
    /**
     * The size of the ID in byte. It is the sum of one JobVertexID type(jobVertexId) and one int
     * type(subtaskIndex).
     */
    static constexpr int SIZE = JobVertexID::SIZE + sizeof(int32_t);

    ExecutionVertexIDPOD() : jobVertexId(), subtaskIndex(0)
    {
    }

    ExecutionVertexIDPOD(const JobVertexID& jobVertexId, int32_t subtaskIndex)
        : jobVertexId(jobVertexId),
          subtaskIndex(subtaskIndex)
    {
        if (subtaskIndex < 0) {
            throw std::invalid_argument("subtaskIndex must be greater than or equal to 0");
        }
    }

    JobVertexID getJobVertexId() const
    {
        return jobVertexId;
    }

    int32_t getSubtaskIndex() const
    {
        return subtaskIndex;
    }

    bool operator==(const ExecutionVertexIDPOD& other) const
    {
        return subtaskIndex == other.subtaskIndex && jobVertexId == other.jobVertexId;
    }

    bool operator!=(const ExecutionVertexIDPOD& other) const
    {
        return !(*this == other);
    }

    friend std::size_t hash_value(const ExecutionVertexIDPOD& obj)
    {
        std::size_t seed = 0x358D0F80;
        seed ^= (seed << 6) + (seed >> 2) + 0x105088E5 + hash_value(static_cast<const AbstractIDPOD&>(obj.jobVertexId));
        seed ^= (seed << 6) + (seed >> 2) + 0x291D5EEC + static_cast<std::size_t>(obj.subtaskIndex);
        return seed;
    }

    std::string toString() const
    {
        return jobVertexId.toString() + std::to_string(subtaskIndex);
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ExecutionVertexIDPOD, jobVertexId, subtaskIndex)

private:
    JobVertexID jobVertexId;
    int32_t subtaskIndex;
};

} // namespace omnistream

#endif // OMNISTREAM_EXECUTIONVERTEX_H
