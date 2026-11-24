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

#ifndef INTERMEDIATEDATASETIDPOD_H
#define INTERMEDIATEDATASETIDPOD_H
#include "AbstractIDPOD.h"
#include "table/data/RowData.h"

namespace omnistream {
class IntermediateDataSetIDPOD : public AbstractIDPOD {
public:
    IntermediateDataSetIDPOD() : AbstractIDPOD() {};

    IntermediateDataSetIDPOD(long upper, long lower) : AbstractIDPOD(upper, lower)
    {
    }

    IntermediateDataSetIDPOD(const IntermediateDataSetIDPOD& other) : AbstractIDPOD(other)
    {
    }

    IntermediateDataSetIDPOD(IntermediateDataSetIDPOD&& other) noexcept : AbstractIDPOD(std::move(other))
    {
    }

    ~IntermediateDataSetIDPOD() = default;

    IntermediateDataSetIDPOD& operator=(const IntermediateDataSetIDPOD& other)
    {
        if (this == &other) {
            return *this;
        }
        AbstractIDPOD::operator=(other);
        return *this;
    }

    IntermediateDataSetIDPOD& operator=(IntermediateDataSetIDPOD&& other) noexcept
    {
        if (this == &other) {
            return *this;
        }
        AbstractIDPOD::operator=(std::move(other));
        return *this;
    }

    bool operator==(const IntermediateDataSetIDPOD& other) const
    {
        return upperPart == other.upperPart && lowerPart == other.lowerPart;
    }

    bool operator==(const IntermediateDataSetIDPOD& other)
    {
        return upperPart == other.upperPart && lowerPart == other.lowerPart;
    }

    std::size_t operator()(const IntermediateDataSetIDPOD& p)
    {
        std::size_t seed = 0;
        seed = hash_combine(seed, p.upperPart);
        return hash_combine(seed, p.lowerPart);
    }

    std::size_t operator()(const IntermediateDataSetIDPOD& p) const
    {
        std::size_t seed = 0;
        seed = hash_combine(seed, p.upperPart);
        return hash_combine(seed, p.lowerPart);
    }
};
} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::IntermediateDataSetIDPOD> {
    size_t operator()(const omnistream::IntermediateDataSetIDPOD& intermediateDataSetID) const
    {
        std::size_t h1 = std::hash<int>()(intermediateDataSetID.getUpperPart());
        std::size_t h2 = std::hash<int>()(intermediateDataSetID.getLowerPart());
        return h1 ^ (h2 << 1);
    }
};
} // namespace std

#endif
