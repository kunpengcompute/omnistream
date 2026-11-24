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

#ifndef TYPEDESCRIPTIONPOD_H
#define TYPEDESCRIPTIONPOD_H

#include <string>
#include <nlohmann/json.hpp>

namespace omnistream {

class TypeDescriptionPOD {
public:
    std::string kind;
    bool isNull;
    int precision;
    std::string type;
    int timestampKind;
    std::string fieldName;

    TypeDescriptionPOD() : kind(""), isNull(false), precision(0), type(""), timestampKind(0), fieldName("")
    {
    }

    TypeDescriptionPOD(
        const std::string& kind,
        bool isNull,
        int precision,
        const std::string& type,
        int timestampKind,
        std::string fieldName)
        : kind(kind),
          isNull(isNull),
          precision(precision),
          type(type),
          timestampKind(timestampKind),
          fieldName(fieldName)
    {
    }

    TypeDescriptionPOD& operator=(const TypeDescriptionPOD&) = default;
    TypeDescriptionPOD(const TypeDescriptionPOD& other) = default;

    std::string getKind() const
    {
        return kind;
    }
    void setKind(const std::string& kind_)
    {
        this->kind = kind_;
    }

    bool getIsNull() const
    {
        return isNull;
    }
    void setIsNull(bool isNull_)
    {
        this->isNull = isNull_;
    }

    int getPrecision() const
    {
        return precision;
    }
    void setPrecision(int precision_)
    {
        this->precision = precision_;
    }

    std::string getType() const
    {
        return type;
    }
    void setType(const std::string& type_)
    {
        this->type = type_;
    }

    int getTimestampKind() const
    {
        return timestampKind;
    }
    void setTimestampKind(int timestampKind_)
    {
        this->timestampKind = timestampKind_;
    }

    std::string getFieldName() const
    {
        return fieldName;
    }
    void setFieldName(const std::string& fieldName_)
    {
        this->fieldName = fieldName_;
    }

    std::string toString() const
    {
        return "TypeDescriptionPOD{kind='" + kind + "', isNull=" + std::to_string(isNull) +
               ", precision=" + std::to_string(precision) + ", type='" + type +
               "', timestampKind=" + std::to_string(timestampKind) + "', fieldName=" + fieldName + "}";
    }

    bool operator==(const TypeDescriptionPOD& other) const
    {
        return this->kind == other.kind && this->isNull == other.isNull && this->precision == other.precision &&
               this->type == other.type && this->timestampKind == other.timestampKind &&
               this->fieldName == other.fieldName;
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(TypeDescriptionPOD, kind, isNull, precision, type, timestampKind, fieldName)
};

} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::TypeDescriptionPOD> {
    std::size_t operator()(const omnistream::TypeDescriptionPOD& obj) const
    {
        size_t h1 = std::hash<std::string>{}(obj.kind);
        size_t h2 = std::hash<long>{}(obj.isNull);
        size_t h3 = std::hash<int>{}(obj.precision);
        size_t h4 = std::hash<std::string>{}(obj.type);
        size_t h5 = std::hash<int>{}(obj.timestampKind);
        size_t h8 = std::hash<std::string>{}(obj.fieldName);

        size_t seed = 0;

        seed ^= (h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
        seed ^= (h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
        seed ^= (h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
        seed ^= (h4 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
        seed ^= (h5 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
        seed ^= (h8 + 0x9e3779b9 + (seed << 6) + (seed >> 2));

        return seed;
    }
};
} // namespace std

#endif
