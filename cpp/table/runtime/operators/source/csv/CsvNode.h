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

#ifndef CSVNODE_H
#define CSVNODE_H

#pragma once

#include <string>
#include <vector>
#include "CsvSchema.h"
#include "type/data_type.h"

namespace omnistream {
namespace csv {

class CsvNode {
public:
    CsvNode(const std::string& value, omniruntime::type::DataTypeId type) : value_(value), type_(type)
    {
    }
    ~CsvNode() = default;
    std::string getValue() const
    {
        return value_;
    }
    omniruntime::type::DataTypeId getType() const
    {
        return type_;
    }

private:
    std::string value_;
    omniruntime::type::DataTypeId type_;
};

} // namespace csv
} // namespace omnistream

#endif
