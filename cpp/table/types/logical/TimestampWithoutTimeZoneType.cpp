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

#include "TimestampWithoutTimeZoneType.h"

using namespace omniruntime::type;

TimestampWithoutTimeZoneType::TimestampWithoutTimeZoneType(bool isNull, int32_t precision)
    : BasicLogicalType(isNull, DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE, "TIMESTAMP_WITHOUT_TIME_ZONE"),
      precision(precision)
{
}

std::vector<LogicalType*> TimestampWithoutTimeZoneType::getChildren()
{
    NOT_IMPL_EXCEPTION;
}

nlohmann::json TimestampWithoutTimeZoneType::toJson() const
{
    nlohmann::json result = LogicalType::toJson();
    result["precision"] = precision;

    return result;
}

int32_t TimestampWithoutTimeZoneType::getPrecision()
{
    return this->precision;
}
