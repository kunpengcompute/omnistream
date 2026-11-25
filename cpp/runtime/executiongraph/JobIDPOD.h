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

#ifndef JOBIDPOD_H
#define JOBIDPOD_H

#include <iostream>
#include <string>
#include <nlohmann/json.hpp> // Include for JSON serialization
#include "runtime/executiongraph/common/AbstractIDPOD.h"

namespace omnistream {

class JobIDPOD : public AbstractIDPOD {
public:
    JobIDPOD() : AbstractIDPOD()
    {
    } // Default constructor
    JobIDPOD(uint64_t upperPart, uint64_t lowerPart) : AbstractIDPOD(upperPart, lowerPart)
    {
    }
    JobIDPOD(std::vector<uint8_t> buf) : AbstractIDPOD(buf)
    {
    }

    JobIDPOD(const JobIDPOD& other) : AbstractIDPOD(other)
    {
    }

    JobIDPOD(JobIDPOD&& other) noexcept : AbstractIDPOD(std::move(other))
    {
    }

    static JobIDPOD* generate()
    {
        return new JobIDPOD();
    }

    JobIDPOD& operator=(const JobIDPOD& other)
    {
        if (this == &other) {
            return *this;
        }
        upperPart = other.upperPart;
        lowerPart = other.lowerPart;
        return *this;
    }

    JobIDPOD& operator=(JobIDPOD&& other) noexcept
    {
        if (this == &other) {
            return *this;
        }
        upperPart = other.upperPart;
        lowerPart = other.lowerPart;
        return *this;
    }

    friend bool operator==(const JobIDPOD& lhs, const JobIDPOD& rhs)
    {
        return lhs.upperPart == rhs.upperPart && lhs.lowerPart == rhs.lowerPart;
    }

    friend bool operator!=(const JobIDPOD& lhs, const JobIDPOD& rhs)
    {
        return !(lhs == rhs);
    }

    friend std::size_t hash_value(const JobIDPOD& obj)
    {
        std::size_t seed = 0x3DD43D4E;
        seed ^= (seed << 6) + (seed >> 2) + 0x4218D36C + static_cast<std::size_t>(obj.upperPart);
        seed ^= (seed << 6) + (seed >> 2) + 0x7AA0C6A1 + static_cast<std::size_t>(obj.lowerPart);
        return seed;
    }

    long getUpperPart() const
    {
        return upperPart;
    }
    void setUpperPart(long long upper)
    {
        upperPart = upper;
    }

    long getLowerPart() const
    {
        return lowerPart;
    }
    void setLowerPart(long long lower)
    {
        lowerPart = lower;
    }

    std::string toString() const
    {
        return "JobIDPOD{ upperPart=" + std::to_string(upperPart) + ", lowerPart=" + std::to_string(lowerPart) + '}';
    }

    static JobIDPOD* fromByteArray(std::vector<uint8_t> buf)
    {
        return new JobIDPOD(buf);
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(JobIDPOD, upperPart, lowerPart)
};

} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::JobIDPOD> {
    std::size_t operator()(const omnistream::JobIDPOD& obj) const
    {
        return hash_value(obj);
    }
};
} // namespace std

#endif
