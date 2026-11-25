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

#ifndef STREAMPARTITIONERPOD_H
#define STREAMPARTITIONERPOD_H

#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include "KeyFieldInfoPOD.h"

namespace omnistream {

class StreamPartitionerPOD {
public:
    static constexpr const char* FORWARD = "forward";
    static constexpr const char* REBALANCE = "rebalance";
    static constexpr const char* RESCALE = "rescale";
    static constexpr const char* GLOBAL = "global";
    static constexpr const char* HASH = "hash";
    static constexpr const char* NONE = "none";

    StreamPartitionerPOD() = default;

    StreamPartitionerPOD(const std::string& partitionerName, const std::vector<KeyFieldInfoPOD>& hashFields)
        : partitionerName(partitionerName),
          hashFields(hashFields)
    {
    }

    StreamPartitionerPOD(const StreamPartitionerPOD& other)
        : partitionerName(other.partitionerName),
          hashFields(other.hashFields)
    {
    }

    StreamPartitionerPOD(StreamPartitionerPOD&& other) noexcept
        : partitionerName(std::move(other.partitionerName)),
          hashFields(std::move(other.hashFields))
    {
    }

    StreamPartitionerPOD& operator=(const StreamPartitionerPOD& other)
    {
        if (this == &other) {
            return *this;
        }
        partitionerName = other.partitionerName;
        hashFields = other.hashFields;
        return *this;
    }

    StreamPartitionerPOD& operator=(StreamPartitionerPOD&& other) noexcept
    {
        if (this == &other) {
            return *this;
        }
        partitionerName = std::move(other.partitionerName);
        hashFields = std::move(other.hashFields);
        return *this;
    }

    friend bool operator==(const StreamPartitionerPOD& lhs, const StreamPartitionerPOD& rhs)
    {
        return lhs.partitionerName == rhs.partitionerName && lhs.hashFields == rhs.hashFields;
    }

    friend bool operator!=(const StreamPartitionerPOD& lhs, const StreamPartitionerPOD& rhs)
    {
        return !(lhs == rhs);
    }

    static std::size_t hash_value(const std::vector<KeyFieldInfoPOD>& hashFields_)
    {
        std::size_t seed = 0x0151AD2E;
        for (const KeyFieldInfoPOD& element : hashFields_) {
            seed ^= std::hash<KeyFieldInfoPOD>{}(element) + 0x9e3779b9 + (seed << 6) + (seed >> 2); // Good mixing
        }
        return seed;
    }

    friend std::size_t hash_value(const StreamPartitionerPOD& obj)
    {
        std::size_t seed = 0x0151AD2E;
        seed ^= (seed << 6) + (seed >> 2) + 0x7CA90669 + std::hash<std::string>{}(obj.partitionerName);
        seed ^= (seed << 6) + (seed >> 2) + 0x1E961E13 + hash_value(obj.hashFields);
        return seed;
    }

    std::string getPartitionerName() const;
    void setPartitionerName(const std::string& partitionerName);

    const std::vector<KeyFieldInfoPOD>& getHashFields() const;
    void setHashFields(const std::vector<KeyFieldInfoPOD>& hashFields);
    std::string toString() const;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(StreamPartitionerPOD, partitionerName, hashFields)

private:
    std::string partitionerName;
    std::vector<KeyFieldInfoPOD> hashFields;
};

} // namespace omnistream

// Specialize std::hash  (REQUIRED)
namespace std {
template <>
struct hash<omnistream::StreamPartitionerPOD> {
    std::size_t operator()(const omnistream::StreamPartitionerPOD& obj) const
    {
        return hash_value(obj); // O
    }
};
} // namespace std

#endif
