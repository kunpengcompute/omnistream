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

#include "thirdlibrary/java_time_LocalDateTime.h"

LocalDateTime* LocalDateTime::ofInstant(Instant* instant, ZoneId* zoneId)
{
    //        time_t t = instant->getEpochSecond() + instant->getNano() / SEC_TO_NSEC;
    time_t t = instant->getEpochSecond();
    LocalDateTime* n = new LocalDateTime();
    localtime_r((const time_t*)&t, &n->now_tm);
    return n;
}

std::tm* LocalDateTime::getTm()
{
    return &now_tm;
}
