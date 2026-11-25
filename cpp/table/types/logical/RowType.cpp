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

#include "RowType.h"

#include <utility>
#include <set>

using namespace omniruntime::type;

omnistream::RowField::RowField(string name, LogicalType* type, string description)
    : name_(std::move(name)),
      type_(type),
      description_(std::move(description))
{
}

omnistream::RowField::RowField(const string& name, LogicalType* type) : RowField(name, type, "")
{
}

LogicalType* omnistream::RowField::getType() const
{
    return type_;
}

std::string omnistream::RowField::getName() const
{
    return name_;
}

nlohmann::json omnistream::RowField::toJson() const
{
    nlohmann::json result;
    result["name"] = name_;
    result["fieldType"] = type_->toJson();
    result["description"] = description_;

    return result;
}

//////////////////Row Type

omnistream::RowType::RowType(bool isNull, const std::vector<RowField>& fields)
    : LogicalType(isNull, DataTypeId::OMNI_CONTAINER, "ROW"),
      fields_(fields)
{
}

std::vector<LogicalType*> omnistream::RowType::getChildren()
{
    if (types.size() != fields_.size()) {
        for (const auto& field : fields_) {
            LogicalType* type = field.getType();
            types.push_back(type);
        }
    }

    return types;
}

nlohmann::json omnistream::RowType::toJson() const
{
    nlohmann::json result = LogicalType::toJson();
    nlohmann::json fields = nlohmann::json::array();
    for (const auto& item : fields_) {
        fields.push_back(item.toJson());
    }
    result["fields"] = fields;

    return result;
}

omnistream::RowType::RowType(bool isNull, const std::vector<std::string>& typeName)
    : LogicalType(isNull, DataTypeId::OMNI_CONTAINER, "ROW")
{
    fields_.reserve(typeName.size());
    int idx = 0;
    for (const auto& name : typeName) {
        auto logicalType = LogicalType::flinkTypeToOmniType(name);
        fields_.emplace_back("f" + std::to_string(idx++), logicalType, "");
    }
}
