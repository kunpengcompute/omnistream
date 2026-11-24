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

#ifndef KEYFIELDINFOPOD_H
#define KEYFIELDINFOPOD_H

#include <string>
#include <nlohmann/json.hpp>

namespace omnistream {

class KeyFieldInfoPOD {
public:
    KeyFieldInfoPOD(); // No-argument constructor declaration
    KeyFieldInfoPOD(const std::string& fieldName, const std::string& fieldTypeName, int fieldIndex);

    KeyFieldInfoPOD(const KeyFieldInfoPOD& other)
        : fieldName(other.fieldName),
          fieldTypeName(other.fieldTypeName),
          fieldIndex(other.fieldIndex)
    {
    }

    KeyFieldInfoPOD(KeyFieldInfoPOD&& other) noexcept
        : fieldName(std::move(other.fieldName)),
          fieldTypeName(std::move(other.fieldTypeName)),
          fieldIndex(other.fieldIndex)
    {
    }

    KeyFieldInfoPOD& operator=(const KeyFieldInfoPOD& other)
    {
        if (this == &other) {
            return *this;
        }
        fieldName = other.fieldName;
        fieldTypeName = other.fieldTypeName;
        fieldIndex = other.fieldIndex;
        return *this;
    }

    KeyFieldInfoPOD& operator=(KeyFieldInfoPOD&& other) noexcept
    {
        if (this == &other) {
            return *this;
        }
        fieldName = std::move(other.fieldName);
        fieldTypeName = std::move(other.fieldTypeName);
        fieldIndex = other.fieldIndex;
        return *this;
    }

    friend bool operator==(const KeyFieldInfoPOD& lhs, const KeyFieldInfoPOD& rhs)
    {
        return lhs.fieldName == rhs.fieldName && lhs.fieldTypeName == rhs.fieldTypeName &&
               lhs.fieldIndex == rhs.fieldIndex;
    }

    friend bool operator!=(const KeyFieldInfoPOD& lhs, const KeyFieldInfoPOD& rhs)
    {
        return !(lhs == rhs);
    }

    friend std::size_t hash_value(const KeyFieldInfoPOD& obj)
    {
        std::size_t seed = 0x6419BE79;
        seed ^= (seed << 6) + (seed >> 2) + 0x58266EA5 + std::hash<std::string>{}(obj.fieldName);
        seed ^= (seed << 6) + (seed >> 2) + 0x703CCA2A + std::hash<std::string>{}(obj.fieldTypeName);
        seed ^= (seed << 6) + (seed >> 2) + 0x77515FE5 + static_cast<std::size_t>(obj.fieldIndex);
        return seed;
    }

    std::string getFieldName() const;
    std::string getFieldTypeName() const;
    int getFieldIndex() const;

    void setFieldName(const std::string& fieldName);
    void setFieldTypeName(const std::string& fieldTypeName);
    void setFieldIndex(int fieldIndex);

    std::string toString() const;

    // Use NLOHMANN_DEFINE_TYPE_INTRUSIVE
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(KeyFieldInfoPOD, fieldName, fieldTypeName, fieldIndex)
private:
    std::string fieldName;
    std::string fieldTypeName;
    int fieldIndex;
};

} // namespace omnistream
// Specialize std::hash  (REQUIRED)
namespace std {
template <>
struct hash<omnistream::KeyFieldInfoPOD> {
    std::size_t operator()(const omnistream::KeyFieldInfoPOD& obj) const
    {
        return hash_value(obj); // O
    }
};
} // namespace std

#endif
