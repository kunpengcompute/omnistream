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

#ifndef FLINK_TNEL_UUID_H
#define FLINK_TNEL_UUID_H

#include <cstdint>
#include <random>
#include <stdexcept>
#include <sstream>
#include <iomanip>

class UUID {
public:
    UUID(uint64_t mostSigBits, uint64_t leastSigBits) : mostSigBits(mostSigBits), leastSigBits(leastSigBits)
    {
    }
    UUID() : mostSigBits(0), leastSigBits(0)
    {
    }

    static UUID randomUUID()
    {
        static thread_local std::random_device rd;
        static thread_local std::mt19937_64 gen(rd());
        uint64_t msb = gen();
        uint64_t lsb = gen();

        // Set version (4) and variant (2)
        msb = (msb & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL; // version 4
        lsb = (lsb & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL; // IETF variant

        return UUID(msb, lsb);
    }

    static UUID FromString(const std::string& str)
    {
        if (str.length() != 36 || str[8] != '-' || str[13] != '-' || str[18] != '-' || str[23] != '-') {
            throw std::invalid_argument("Invalid UUID string format: " + str);
        }

        auto hexToULL = [](const std::string& s) -> uint64_t {
            uint64_t val = 0;
            std::istringstream iss(s);
            iss >> std::hex >> val;
            if (iss.fail()) {
                throw std::invalid_argument("Invalid hex in UUID: " + s);
            }
            return val;
        };

        uint64_t msb = 0;
        uint64_t lsb = 0;
        msb = (hexToULL(str.substr(0, 8)) << 32) | (hexToULL(str.substr(9, 4)) << 16) | hexToULL(str.substr(14, 4));
        lsb = (hexToULL(str.substr(19, 4)) << 48) | hexToULL(str.substr(24, 12));

        return UUID(msb, lsb);
    }

    std::string ToString() const
    {
        std::ostringstream oss;
        oss << std::hex << std::setfill('0') << std::setw(8) << ((mostSigBits >> 32) & 0xFFFFFFFFULL) << "-"
            << std::setw(4) << ((mostSigBits >> 16) & 0xFFFFULL) << "-" << std::setw(4) << (mostSigBits & 0xFFFFULL)
            << "-" << std::setw(4) << ((leastSigBits >> 48) & 0xFFFFULL) << "-" << std::setw(12)
            << (leastSigBits & 0xFFFFFFFFFFFFULL);
        return oss.str();
    }

    uint64_t getMostSignificantBits() const
    {
        return mostSigBits;
    }

    uint64_t getLeastSignificantBits() const
    {
        return leastSigBits;
    }

    bool operator==(const UUID& other) const
    {
        return mostSigBits == other.mostSigBits && leastSigBits == other.leastSigBits;
    }

    bool operator!=(const UUID& other) const
    {
        return !(*this == other);
    }

    bool operator<(const UUID& other) const
    {
        return std::tie(mostSigBits, leastSigBits) < std::tie(other.mostSigBits, other.leastSigBits);
    }

    std::size_t hashCode() const
    {
        std::size_t seed = 0x9e3779b9;
        seed ^= std::hash<uint64_t>()(mostSigBits) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= std::hash<uint64_t>()(leastSigBits) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        return seed;
    }

private:
    uint64_t mostSigBits;
    uint64_t leastSigBits;
};

namespace std {
template <>
struct hash<UUID> {
    size_t operator()(const UUID& uuid) const noexcept
    {
        return std::hash<uint64_t>()(uuid.getMostSignificantBits()) ^
               std::hash<uint64_t>()(uuid.getLeastSignificantBits());
    }
};
} // namespace std

#endif // FLINK_TNEL_UUID_H
