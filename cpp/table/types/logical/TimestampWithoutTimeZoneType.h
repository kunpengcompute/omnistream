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

#ifndef FLINK_TNEL_TIMESTAMP_WITHOUT_TIME_ZONE_H
#define FLINK_TNEL_TIMESTAMP_WITHOUT_TIME_ZONE_H

#include "LogicalType.h"

class TimestampWithoutTimeZoneType : public BasicLogicalType {
public:
    explicit TimestampWithoutTimeZoneType(bool isNull, int32_t precision = 0);
    int32_t getPrecision();
    std::vector<LogicalType*> getChildren() override;

    nlohmann::json toJson() const override;

private:
    int32_t precision;
};

#endif
