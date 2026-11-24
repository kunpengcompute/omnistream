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

#include "ResultPartitionType.h"

namespace omnistream {
const int ResultPartitionType::PIPELINED = ResultPartitionType::encode(true, true, false, false, false);
const int ResultPartitionType::PIPELINED_BOUNDED = ResultPartitionType::encode(true, true, true, false, false);
const int ResultPartitionType::PIPELINED_APPROXIMATE = ResultPartitionType::encode(true, true, true, false, true);
const int ResultPartitionType::BLOCKING_PERSISTENT = ResultPartitionType::encode(false, false, false, true, true);
const int ResultPartitionType::BLOCKING = ResultPartitionType::encode(false, false, false, false, true);

int ResultPartitionType::encode(
    bool isPipelined, bool hasBackPressure, bool isBounded, bool isPersistent, bool isReconnectable)
{
    unsigned int uflags = 0;

    if (isPipelined) {
        uflags |= PIPELINED_MASK;
    }
    if (hasBackPressure) {
        uflags |= BACK_PRESSURE_MASK;
    }
    if (isBounded) {
        uflags |= BOUNDED_MASK;
    }
    if (isPersistent) {
        uflags |= PERSISTENT_MASK;
    }
    if (isReconnectable) {
        uflags |= RECONNECTABLE_MASK;
    }

    return static_cast<int>(uflags);
}

std::string ResultPartitionType::getNameByType(int type)
{
    if (type == PIPELINED) {
        return "PIPELINED";
    } else if (type == PIPELINED_BOUNDED) {
        return "PIPELINED_BOUNDED";
    } else if (type == BLOCKING_PERSISTENT) {
        return "BLOCKING_PERSISTENT";
    } else if (type == BLOCKING) {
        return "BLOCKING";
    } else if (type == PIPELINED_APPROXIMATE) {
        return "PIPELINED_APPROXIMATE";
    } else {
        return "UNKNOWN";
    }
}
} // namespace omnistream
