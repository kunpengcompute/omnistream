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

#ifndef FLINK_TNEL_ROWTYPE_H
#define FLINK_TNEL_ROWTYPE_H

#include <string>
#include <vector>
#include "LogicalType.h"

using string = std::string;
namespace omnistream {
class RowField {
public:
    RowField(string name, LogicalType* type, string description);

    RowField(const string& name, LogicalType* type);

    [[nodiscard]] LogicalType* getType() const;

    std::string getName() const;

    nlohmann::json toJson() const;

private:
    string name_;
    LogicalType* type_;
    string description_;
};

class RowType : public LogicalType {
public:
    RowType(bool isNull, const std::vector<RowField>& fields);
    RowType(bool isNull, const std::vector<std::string>& typeName);
    std::vector<LogicalType*> getChildren() override;

    nlohmann::json toJson() const override;

private:
    std::vector<RowField> fields_;
    std::vector<LogicalType*> types;
};
} // namespace omnistream

#endif // FLINK_TNEL_ROWTYPE_H
