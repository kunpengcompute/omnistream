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

#ifndef FLINK_TNEL_LOGICALTYPE_H
#define FLINK_TNEL_LOGICALTYPE_H

#include <vector>
#include <memory>
#include <optional>
#include <utility>
#include <unordered_map>
#include <string>
#include "OmniOperatorJIT/core/src/type/data_type.h"
#include <nlohmann/json.hpp>
#include "core/include/common.h"

class LogicalType {
public:
    LogicalType(int typeId, bool isNullable);

    LogicalType(bool isNullable, int typeId, const std::string& typeName);

    virtual ~LogicalType() = default;

    int getTypeId() const;

    bool isNullable() const;
    const std::string& getTypeName() const
    {
        return typeName_;
    }
    virtual std::vector<LogicalType*> getChildren() = 0;

    virtual nlohmann::json toJson() const;

    static omniruntime::type::DataTypeId flinkTypeToOmniTypeId(const std::string& flinkType);

    static LogicalType* flinkTypeToOmniType(const std::string& flinkType);

    static bool isSharedLogicalType(const LogicalType* logicalType);

    static std::unordered_map<std::string, omniruntime::type::DataTypeId> nameToIdMap;
    static void buildNameToIdMap();

private:
    static LogicalType* parseRawType(
        const std::string& flinkType, const std::string& basicStrippedType, bool isNullable);

protected:
    bool isNullable_;
    int typeId_;
    std::string typeName_;
};

class BasicLogicalType : public LogicalType {
public:
    BasicLogicalType(int typeId, bool isNullable) : LogicalType(typeId, isNullable) {};

    BasicLogicalType(bool isNullable, int typeId, const std::string& typeName)
        : LogicalType(isNullable, typeId, typeName) {};

    BasicLogicalType(
        bool isNullable, int typeId, const std::string& typeName, std::vector<std::unique_ptr<LogicalType>> children)
        : LogicalType(isNullable, typeId, typeName),
          children_(std::move(children))
    {
    }

    std::vector<LogicalType*> getChildren()
    {
        std::vector<LogicalType*> children;
        children.reserve(children_.size());
        for (const auto& child : children_) {
            children.push_back(child.get());
        }
        return children;
    }

    nlohmann::json toJson() const override
    {
        nlohmann::json result = LogicalType::toJson();
        nlohmann::json types = nlohmann::json::array();
        for (const auto& child : children_) {
            types.push_back(child->toJson());
        }
        if (types.size() > 0) {
            result["children"] = types;
        }

        return result;
    }

    static BasicLogicalType* BOOLEAN;
    static BasicLogicalType* INTEGER;
    static BasicLogicalType* BIGINT;
    static BasicLogicalType* VARCHAR;
    static BasicLogicalType* DOUBLE;
    static BasicLogicalType* DATE;
    static BasicLogicalType* TIME_WITHOUT_TIME_ZONE;
    static BasicLogicalType* TIMESTAMP_WITHOUT_TIME_ZONE;
    static BasicLogicalType* TIMESTAMP_WITH_TIME_ZONE;
    static BasicLogicalType* TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    static BasicLogicalType* TIMESTAMP;
    static BasicLogicalType* INVALID_TYPE;

    static BasicLogicalType* getTypeBy(omniruntime::type::DataTypeId typeId, const nlohmann::json& options);

    static BasicLogicalType* getTypeBy(
        std::optional<bool> nullable, omniruntime::type::DataTypeId typeId, const nlohmann::json& options);

private:
    std::vector<std::unique_ptr<LogicalType>> children_;
};
#endif
