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

#ifndef FLINK_TNEL_LocalDateTime_H
#define FLINK_TNEL_LocalDateTime_H

#include <chrono>
#include <ctime>
#include "java_time_ZoneId.h"
#include "java_time_temporal_TemporalAccessor.h"
#include "java_time_Instant.h"

#define SEC_TO_NSEC 1000000000

class LocalDateTime : public TemporalAccessor {
public:
    static LocalDateTime* ofInstant(Instant* instant, ZoneId* zoneId);

    std::tm* getTm();

private:
    std::tm now_tm;
};

#endif // FLINK_TNEL_LocalDateTime_H
