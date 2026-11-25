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

#pragma once

#include <memory>
#include <stdexcept>
#include <string>

#include "TypeSerializer.h"

class TypeSerializerSchemaCompatibility {
public:
    enum class Type {
        COMPATIBLE_AS_IS,
        COMPATIBLE_AFTER_MIGRATION,
        COMPATIBLE_WITH_RECONFIGURED_SERIALIZER,
        INCOMPATIBLE
    };

    static TypeSerializerSchemaCompatibility compatibleAsIs()
    {
        return TypeSerializerSchemaCompatibility(Type::COMPATIBLE_AS_IS, nullptr);
    }

    static TypeSerializerSchemaCompatibility compatibleAfterMigration()
    {
        return TypeSerializerSchemaCompatibility(Type::COMPATIBLE_AFTER_MIGRATION, nullptr);
    }

    static TypeSerializerSchemaCompatibility compatibleWithReconfiguredSerializer(
        TypeSerializer* reconfiguredSerializer)
    {
        if (reconfiguredSerializer == nullptr) {
            THROW_LOGIC_EXCEPTION("reconfiguredSerializer must not be null");
        }
        return TypeSerializerSchemaCompatibility(Type::COMPATIBLE_WITH_RECONFIGURED_SERIALIZER, reconfiguredSerializer);
    }

    static TypeSerializerSchemaCompatibility incompatible()
    {
        return TypeSerializerSchemaCompatibility(Type::INCOMPATIBLE, nullptr);
    }

    bool isCompatibleAsIs() const
    {
        return resultType_ == Type::COMPATIBLE_AS_IS;
    }

    bool isCompatibleAfterMigration() const
    {
        return resultType_ == Type::COMPATIBLE_AFTER_MIGRATION;
    }

    bool isCompatibleWithReconfiguredSerializer() const
    {
        return resultType_ == Type::COMPATIBLE_WITH_RECONFIGURED_SERIALIZER;
    }

    TypeSerializer* getReconfiguredSerializer() const
    {
        if (!isCompatibleWithReconfiguredSerializer()) {
            THROW_LOGIC_EXCEPTION(
                "It is only possible to get a reconfigured serializer if the compatibility type is "
                "COMPATIBLE_WITH_RECONFIGURED_SERIALIZER, but the type is " +
                std::to_string(static_cast<int>(resultType_)));
        }
        return reconfiguredNewSerializer_;
    }

    bool isIncompatible() const
    {
        return resultType_ == Type::INCOMPATIBLE;
    }

    std::string toString() const
    {
        std::string result;
        switch (resultType_) {
            case Type::COMPATIBLE_AS_IS: result = "COMPATIBLE_AS_IS"; break;
            case Type::COMPATIBLE_AFTER_MIGRATION: result = "COMPATIBLE_AFTER_MIGRATION"; break;
            case Type::COMPATIBLE_WITH_RECONFIGURED_SERIALIZER:
                result = "COMPATIBLE_WITH_RECONFIGURED_SERIALIZER";
                break;
            case Type::INCOMPATIBLE: result = "INCOMPATIBLE"; break;
        }
        return "TypeSerializerSchemaCompatibility{resultType=" + result +
               ", reconfiguredNewSerializer=" + (reconfiguredNewSerializer_ ? "not null" : "null") + "}";
    }

private:
    TypeSerializerSchemaCompatibility(Type resultType, TypeSerializer* reconfiguredNewSerializer)
        : resultType_(resultType),
          reconfiguredNewSerializer_(reconfiguredNewSerializer)
    {
    }

    Type resultType_;
    TypeSerializer* reconfiguredNewSerializer_;
};
