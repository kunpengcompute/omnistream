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

#ifndef ABSTRACTIDPOD_H
#define ABSTRACTIDPOD_H

#include <chrono>
#include <iostream>
#include <nlohmann/json.hpp>
#include <random>
#include <string>

#include "core/include/common.h"

namespace omnistream {

class AbstractIDPOD {
public:
    static constexpr int SIZE_OF_LONG = 8;
    static constexpr int SIZE = 2 * SIZE_OF_LONG;

    // Default constructor
    AbstractIDPOD() : upperPart(randomLong()), lowerPart(randomLong())
    {
    }

    // Full argument constructor
    AbstractIDPOD(long upperPart, long lowerPart) : upperPart(upperPart), lowerPart(lowerPart)
    {
    }

    AbstractIDPOD(const AbstractIDPOD& other) : upperPart(other.upperPart), lowerPart(other.lowerPart)
    {
    }

    AbstractIDPOD(AbstractIDPOD&& other) noexcept : upperPart(other.upperPart), lowerPart(other.lowerPart)
    {
    }

    explicit AbstractIDPOD(const std::vector<uint8_t>& bytes)
    {
        if (bytes.size() != SIZE) {
            throw std::invalid_argument("Argument bytes must be an array of " + std::to_string(SIZE) + " bytes");
        }
        lowerPart = static_cast<long>(byteArrayToLong(bytes, 0));
        upperPart = static_cast<long>(byteArrayToLong(bytes, SIZE_OF_LONG));
    }

    static uint64_t byteArrayToLong(const std::vector<uint8_t>& ba, int offset)
    {
        uint64_t value = 0;
        for (int i = 0; i < SIZE_OF_LONG; ++i) {
            value = (value << 8) | ba[offset + i];
        }
        return value;
    }

    AbstractIDPOD& operator=(const AbstractIDPOD& other)
    {
        if (this == &other) {
            return *this;
        }
        upperPart = other.upperPart;
        lowerPart = other.lowerPart;
        return *this;
    }

    AbstractIDPOD& operator=(AbstractIDPOD&& other) noexcept
    {
        if (this == &other) {
            return *this;
        }
        upperPart = other.upperPart;
        lowerPart = other.lowerPart;
        return *this;
    }

    // Getters
    virtual long getUpperPart() const
    {
        return upperPart;
    }
    virtual long getLowerPart() const
    {
        return lowerPart;
    }

    // Setters
    virtual void setUpperPart(long upper)
    {
        upperPart = upper;
    }
    virtual void setLowerPart(long lower)
    {
        lowerPart = lower;
    }

    std::vector<uint8_t> getBytes() const
    {
        std::vector<uint8_t> ba(SIZE);
        longToByteArray(lowerPart, ba, 0);
        longToByteArray(upperPart, ba, SIZE_OF_LONG);
        return ba;
    }

    virtual std::string toString() const
    {
        return bytesToHex(getBytes().data(), SIZE);
    }

    std::string getBytesToString() const
    {
        auto data = getBytes();
        std::string res(data.begin(), getBytes().end());
        return res;
    }

    friend bool operator==(const AbstractIDPOD& lhs, const AbstractIDPOD& rhs)
    {
        return lhs.upperPart == rhs.upperPart && lhs.lowerPart == rhs.lowerPart;
    }

    friend bool operator!=(const AbstractIDPOD& lhs, const AbstractIDPOD& rhs)
    {
        return !(lhs == rhs);
    }

    bool operator<(const AbstractIDPOD& other) const
    {
        if (upperPart != other.upperPart) {
            return upperPart < other.upperPart;
        }
        return lowerPart < other.lowerPart;
    }

    friend std::size_t hash_value(const AbstractIDPOD& obj)
    {
        std::size_t seed = 0x358D0F80;
        seed ^= (seed << 6) + (seed >> 2) + 0x105088E5 + static_cast<std::size_t>(obj.upperPart);
        seed ^= (seed << 6) + (seed >> 2) + 0x291D5EEC + static_cast<std::size_t>(obj.lowerPart);
        return seed;
    }

    // friend std::ostream& operator<<(std::ostream& os, const AbstractIDPOD& obj);

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(AbstractIDPOD, upperPart, lowerPart)
protected:
    long upperPart;
    long lowerPart;

private:
    static uint64_t randomLong()
    {
        static thread_local std::mt19937_64 rng(std::chrono::high_resolution_clock::now().time_since_epoch().count());
        return rng();
    }

    static void longToByteArray(uint64_t l, std::vector<uint8_t>& ba, int offset)
    {
        for (int i = 0; i < SIZE_OF_LONG; ++i) {
            // This calculates the shift to get the most significant byte first
            int shift = (SIZE_OF_LONG - 1 - i) * 8;
            ba[offset + i] = static_cast<uint8_t>((l >> shift) & 0xff);
        }
    }
};

} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::AbstractIDPOD> {
    size_t operator()(const omnistream::AbstractIDPOD& key) const
    {
        return hash_value(key);
    }
};
} // namespace std
#endif // ABSTRACTIDPOD_H
