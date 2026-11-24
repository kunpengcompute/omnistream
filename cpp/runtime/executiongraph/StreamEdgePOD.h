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

#ifndef STREAMEDGEPOD_H
#define STREAMEDGEPOD_H

#include <iostream>
#include <string>
#include <nlohmann/json.hpp>
#include "StreamPartitionerPOD.h"

namespace omnistream {

class StreamEdgePOD {
public:
    StreamEdgePOD() : sourceId(0), targetId(0), typeNumber(0), partitioner(), bufferTimeout(0)
    {
    }
    StreamEdgePOD(
        int sourceId, int targetId, int typeNumber, const StreamPartitionerPOD& partitioner, long bufferTimeout)
        : sourceId(sourceId),
          targetId(targetId),
          typeNumber(typeNumber),
          partitioner(partitioner),
          bufferTimeout(bufferTimeout)
    {
    }

    StreamEdgePOD(const StreamEdgePOD& other)
        : sourceId(other.sourceId),
          targetId(other.targetId),
          typeNumber(other.typeNumber),
          partitioner(other.partitioner),
          bufferTimeout(other.bufferTimeout)
    {
    }

    StreamEdgePOD(StreamEdgePOD&& other) noexcept
        : sourceId(other.sourceId),
          targetId(other.targetId),
          typeNumber(other.typeNumber),
          partitioner(std::move(other.partitioner)),
          bufferTimeout(other.bufferTimeout)
    {
    }

    StreamEdgePOD& operator=(const StreamEdgePOD& other)
    {
        if (this == &other) {
            return *this;
        }
        sourceId = other.sourceId;
        targetId = other.targetId;
        typeNumber = other.typeNumber;
        partitioner = other.partitioner;
        bufferTimeout = other.bufferTimeout;
        return *this;
    }

    StreamEdgePOD& operator=(StreamEdgePOD&& other) noexcept
    {
        if (this == &other) {
            return *this;
        }
        sourceId = other.sourceId;
        targetId = other.targetId;
        typeNumber = other.typeNumber;
        partitioner = std::move(other.partitioner);
        bufferTimeout = other.bufferTimeout;
        return *this;
    }

    friend bool operator==(const StreamEdgePOD& lhs, const StreamEdgePOD& rhs)
    {
        return lhs.sourceId == rhs.sourceId && lhs.targetId == rhs.targetId && lhs.typeNumber == rhs.typeNumber &&
               lhs.partitioner == rhs.partitioner && lhs.bufferTimeout == rhs.bufferTimeout;
    }

    friend bool operator!=(const StreamEdgePOD& lhs, const StreamEdgePOD& rhs)
    {
        return !(lhs == rhs);
    }

    friend std::size_t hash_value(const StreamEdgePOD& obj)
    {
        std::size_t seed = 0x4A79509F;
        seed ^= (seed << 6) + (seed >> 2) + 0x54EA18F5 + static_cast<std::size_t>(obj.sourceId);
        seed ^= (seed << 6) + (seed >> 2) + 0x02144FCA + static_cast<std::size_t>(obj.targetId);
        seed ^= (seed << 6) + (seed >> 2) + 0x322BC259 + static_cast<std::size_t>(obj.typeNumber);
        seed ^= (seed << 6) + (seed >> 2) + 0x2C554FB0 + hash_value(obj.partitioner);
        seed ^= (seed << 6) + (seed >> 2) + 0x73A23BB4 + static_cast<std::size_t>(obj.bufferTimeout);
        return seed;
    }

    int getSourceId() const
    {
        return sourceId;
    }
    void setSourceId(int sourceId_)
    {
        this->sourceId = sourceId_;
    }

    int getTargetId() const
    {
        return targetId;
    }
    void setTargetId(int targetId_)
    {
        this->targetId = targetId_;
    }

    int getTypeNumber() const
    {
        return typeNumber;
    }
    void setTypeNumber(int typeNumber_)
    {
        this->typeNumber = typeNumber_;
    }

    const StreamPartitionerPOD& getPartitioner() const
    {
        return partitioner;
    }
    void setPartitioner(const StreamPartitionerPOD& partitioner_)
    {
        this->partitioner = partitioner_;
    }

    long getBufferTimeout() const
    {
        return bufferTimeout;
    }
    void setBufferTimeout(long bufferTimeout_)
    {
        this->bufferTimeout = bufferTimeout_;
    }

    std::string toString() const
    {
        return "StreamEdgePOJO{  sourceId=" + std::to_string(sourceId) + ", targetId=" + std::to_string(targetId) +
               ", typeNumber=" + std::to_string(typeNumber) +
               ", partitioner=" + partitioner.toString() + // Call toString() on partitioner
               ", bufferTimeout=" + std::to_string(bufferTimeout) + "}";
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(StreamEdgePOD, sourceId, targetId, typeNumber, partitioner, bufferTimeout)
private:
    int sourceId;
    int targetId;
    int typeNumber;
    StreamPartitionerPOD partitioner;
    long bufferTimeout;
};

} // namespace omnistream

// Specialize std::hash  (REQUIRED)
namespace std {
template <>
struct hash<omnistream::StreamEdgePOD> {
    std::size_t operator()(const omnistream::StreamEdgePOD& obj) const
    {
        return hash_value(obj); // O
    }
};
} // namespace std

#endif
