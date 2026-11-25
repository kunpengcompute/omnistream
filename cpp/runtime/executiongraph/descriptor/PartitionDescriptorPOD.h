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

#ifndef PARTITIONDESCRIPTORPOD_H
#define PARTITIONDESCRIPTORPOD_H

#include <string>
#include <iostream>
#include "../common/AbstractIDPOD.h"
#include "IntermediateResultPartitionIDPOD.h"
#include "nlohmann/json.hpp"

namespace omnistream {

using json = nlohmann::json;
class PartitionDescriptorPOD {
public:
    // Default constructor
    PartitionDescriptorPOD()
        : totalNumberOfPartitions(0),
          partitionType(0),
          numberOfSubpartitions(0),
          connectionIndex(0)
    {
    }

    // Full argument constructor
    PartitionDescriptorPOD(
        const AbstractIDPOD& resultId,
        int totalNumberOfPartitions,
        const IntermediateResultPartitionIDPOD& partitionId,
        int partitionType,
        int numberOfSubpartitions,
        int connectionIndex)
        : resultId(resultId),
          totalNumberOfPartitions(totalNumberOfPartitions),
          partitionId(partitionId),
          partitionType(partitionType),
          numberOfSubpartitions(numberOfSubpartitions),
          connectionIndex(connectionIndex)
    {
    }

    // Copy constructor
    PartitionDescriptorPOD& operator=(const PartitionDescriptorPOD&) = delete;
    PartitionDescriptorPOD(const PartitionDescriptorPOD& other) = default;

    // Getters
    const AbstractIDPOD& getResultId() const
    {
        return resultId;
    }
    int getTotalNumberOfPartitions() const
    {
        return totalNumberOfPartitions;
    }
    const IntermediateResultPartitionIDPOD& getPartitionId() const
    {
        return partitionId;
    }
    int getPartitionType() const
    {
        return partitionType;
    }
    int getNumberOfSubpartitions() const
    {
        return numberOfSubpartitions;
    }
    int getConnectionIndex() const
    {
        return connectionIndex;
    }

    // Setters
    void setResultId(const AbstractIDPOD& resultId_)
    {
        this->resultId = resultId_;
    }
    void setTotalNumberOfPartitions(int totalNumberOfPartitions_)
    {
        this->totalNumberOfPartitions = totalNumberOfPartitions_;
    }
    void setPartitionId(const IntermediateResultPartitionIDPOD& partitionId_)
    {
        this->partitionId = partitionId_;
    }
    void setPartitionType(int partitionType_)
    {
        this->partitionType = partitionType_;
    }
    void setNumberOfSubpartitions(int numberOfSubpartitions_)
    {
        this->numberOfSubpartitions = numberOfSubpartitions_;
    }
    void setConnectionIndex(int connectionIndex_)
    {
        this->connectionIndex = connectionIndex_;
    }

    bool operator==(const PartitionDescriptorPOD& other) const
    {
        return resultId == other.getResultId() && totalNumberOfPartitions == other.getTotalNumberOfPartitions() &&
               partitionId == other.getPartitionId() && partitionType == other.getPartitionType() &&
               numberOfSubpartitions == other.getNumberOfSubpartitions() &&
               connectionIndex == other.getConnectionIndex();
    }
    // toString method
    std::string toString() const
    {
        return "Result ID: " + resultId.toString() + ", Total Partitions: " + std::to_string(totalNumberOfPartitions) +
               ", Partition ID: " + partitionId.toString() + ", Partition Type: " + std::to_string(partitionType) +
               ", Subpartitions: " + std::to_string(numberOfSubpartitions) +
               ", Connection Index: " + std::to_string(connectionIndex);
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        PartitionDescriptorPOD,
        resultId,
        totalNumberOfPartitions,
        partitionId,
        partitionType,
        numberOfSubpartitions,
        connectionIndex)
private:
    AbstractIDPOD resultId;
    int totalNumberOfPartitions;
    IntermediateResultPartitionIDPOD partitionId;
    int partitionType;
    int numberOfSubpartitions;
    int connectionIndex;
};
} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::PartitionDescriptorPOD> {
    size_t operator()(const omnistream::PartitionDescriptorPOD& PartitionDescriptorPOD) const
    {
        std::size_t h1 = std::hash<int>()(PartitionDescriptorPOD.getPartitionType());
        std::size_t h2 = std::hash<int>()(PartitionDescriptorPOD.getTotalNumberOfPartitions());
        std::size_t h3 = std::hash<int>()(PartitionDescriptorPOD.getNumberOfSubpartitions());
        std::size_t h4 = std::hash<int>()(PartitionDescriptorPOD.getConnectionIndex());
        std::size_t h5 =
            std::hash<omnistream::IntermediateResultPartitionIDPOD>()(PartitionDescriptorPOD.getPartitionId());
        std::size_t h6 = std::hash<omnistream::AbstractIDPOD>()(PartitionDescriptorPOD.getResultId());

        size_t seed = 0;
        seed ^= h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h4 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h5 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h6 + 0x9e3779b9 + (seed << 6) + (seed >> 2);

        return seed;
    }
};
} // namespace std

#endif
