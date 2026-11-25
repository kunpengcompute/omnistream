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

#include "RawType.h"

#include <stdexcept>

namespace omnistream {
RawType::RawType(bool isNullable, const std::string& className, const std::string& serializerString)
    : BasicLogicalType(isNullable, omniruntime::type::DataTypeId::OMNI_CONTAINER, "RAW"),
      serializerString_(serializerString),
      class_(className)
{
    if (class_.empty()) {
        throw std::invalid_argument("Class must not be empty");
    }
}

nlohmann::json RawType::toJson() const
{
    nlohmann::json result = LogicalType::toJson();
    result["class"] = class_;
    result["serializer"] = getSerializerString();

    return result;
}
} // namespace omnistream
