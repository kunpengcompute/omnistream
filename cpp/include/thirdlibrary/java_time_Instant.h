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

#ifndef FLINK_TNEL_Instant_H
#define FLINK_TNEL_Instant_H

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include "basictypes/Object.h"

#define MIN_SECOND (-31557014167219200L)
#define MAX_SECOND 31556889864403199L

class Instant : public Object {
public:
    Instant(int64_t second, int32_t nano);

    ~Instant();

    static Instant* ofEpochMilli(int64_t epochMilli);

    int64_t getEpochSecond();

    int32_t getNano();

private:
    int64_t seconds;
    int32_t nanos;

    static int floorDiv(int a, int b);

    static int floorMod(int a, int b);
};

#endif // FLINK_TNEL_Instant_H
