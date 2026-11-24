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

#include "WatermarkStrategy.h"

std::shared_ptr<WatermarkStrategy> WatermarkStrategy::ForMonotonousTimestamps()
{
    return std::make_shared<MonotonousTimestampsStrategy>();
}

std::shared_ptr<WatermarkStrategy> WatermarkStrategy::ForBoundedOutOfOrderness(
    int32_t rowtimeFieldIndex, long maxOutOfOrderness)
{
    return std::make_shared<BoundedOutOfOrdernessStrategy>(rowtimeFieldIndex, maxOutOfOrderness);
}

std::shared_ptr<WatermarkStrategy> WatermarkStrategy::NoWatermarks()
{
    return std::make_shared<NoWatermarkStrategy>();
}
