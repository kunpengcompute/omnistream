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

#ifndef RESULTPARTITIONIDPOD_H
#define RESULTPARTITIONIDPOD_H

#include <string>
#include <nlohmann/json.hpp>
#include "IntermediateResultPartitionIDPOD.h" // Include header for IntermediateResultPartitionIDPOD
#include "ExecutionAttemptIDPOD.h"            // Include header for ExecutionAttemptIDPOD

namespace omnistream {

struct ResultPartitionIDPOD {
public:
    // Default constructor
    ResultPartitionIDPOD() = default;
    ~ResultPartitionIDPOD() = default;
    // Full argument constructor
    ResultPartitionIDPOD(const IntermediateResultPartitionIDPOD& partitionId, const ExecutionAttemptIDPOD& producerId)
        : partitionId(partitionId),
          producerId(producerId)
    {
    }

    friend bool operator==(const ResultPartitionIDPOD& lhs, const ResultPartitionIDPOD& rhs)
    {
        return lhs.partitionId == rhs.partitionId && lhs.producerId == rhs.producerId;
    }

    friend bool operator!=(const ResultPartitionIDPOD& lhs, const ResultPartitionIDPOD& rhs)
    {
        return !(lhs == rhs);
    }

    friend std::size_t hash_value(const ResultPartitionIDPOD& obj)
    {
        std::size_t seed = 0x133AAB57;
        seed ^= (seed << 6) + (seed >> 2) + 0x3010BC7E + hash_value(obj.partitionId);
        seed ^= (seed << 6) + (seed >> 2) + 0x2244B1BF + hash_value(obj.producerId);
        return seed;
    }

    // Getters
    const IntermediateResultPartitionIDPOD& getPartitionId() const
    {
        return partitionId;
    }
    const ExecutionAttemptIDPOD& getProducerId() const
    {
        return producerId;
    }

    // Setters
    void setPartitionId(const IntermediateResultPartitionIDPOD& partitionId_)
    {
        this->partitionId = partitionId_;
    }
    void setProducerId(const ExecutionAttemptIDPOD& producerId_)
    {
        this->producerId = producerId_;
    }

    // toString method (example implementation)
    std::string toString() const
    {
        return "ResultPartitionIDPOD { "
               "partitionId: " +
               partitionId.toString() +
               ", "
               "producerId: " +
               producerId.toString() + " }";
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ResultPartitionIDPOD, partitionId, producerId)
private:
    IntermediateResultPartitionIDPOD partitionId;
    ExecutionAttemptIDPOD producerId;
};

} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::ResultPartitionIDPOD> {
    std::size_t operator()(const omnistream::ResultPartitionIDPOD& obj) const
    {
        return hash_value(obj);
    }
};
} // namespace std

#endif // RESULT_PARTITION_ID_POD_H
