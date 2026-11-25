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

#ifndef RESULT_SUBPARTITION_INFO_POD_H
#define RESULT_SUBPARTITION_INFO_POD_H

#include <string>

namespace omnistream {

class ResultSubpartitionInfoPOD {
public:
    ResultSubpartitionInfoPOD();
    ResultSubpartitionInfoPOD(int partitionIdx, int subPartitionIdx);
    ResultSubpartitionInfoPOD& operator=(const ResultSubpartitionInfoPOD&) = default;
    ResultSubpartitionInfoPOD(const ResultSubpartitionInfoPOD& other);
    ~ResultSubpartitionInfoPOD();

    int getPartitionIdx() const;
    void setPartitionIdx(int partitionIdx);

    int getSubPartitionIdx() const;
    void setSubPartitionIdx(int subPartitionIdx);

    bool operator==(const ResultSubpartitionInfoPOD& other) const;
    bool operator!=(const ResultSubpartitionInfoPOD& other) const;
    bool operator<(const ResultSubpartitionInfoPOD& other) const;

    std::string toString() const;
    int hashcode() const
    {
        return (partitionIdx << 16) ^ subPartitionIdx;
    }

    bool equals(const ResultSubpartitionInfoPOD& other) const
    {
        return partitionIdx == other.partitionIdx && subPartitionIdx == other.subPartitionIdx;
    }

private:
    int partitionIdx;
    int subPartitionIdx;
};
struct ResultSubpartitionInfoPODHash {
    std::size_t operator()(const ResultSubpartitionInfoPOD& own) const
    {
        return own.hashcode();
    }
};

struct IResultSubpartitionInfoPODEqual {
    bool operator()(const ResultSubpartitionInfoPOD& lhs, const ResultSubpartitionInfoPOD& rhs) const
    {
        if (lhs == rhs) return true;
        return lhs.equals(rhs);
    }
};

} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::ResultSubpartitionInfoPOD> {
    size_t operator()(const omnistream::ResultSubpartitionInfoPOD& obj) const
    {
        size_t seed = 0;
        // Example: Hashing based on the members of ResultSubpartitionInfoPOD
        // Modify this based on your actual members
        size_t h1 = std::hash<int>()(obj.getPartitionIdx());
        size_t h2 = std::hash<int>()(obj.getSubPartitionIdx());
        // Combine the hashes using a technique such as XOR and prime numbers
        seed ^= h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        return seed;
    }
};
} // namespace std

#endif // RESULT_SUBPARTITION_INFO_POD_H
