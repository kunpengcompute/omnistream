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

#include "DescriptiveStatisticsHistogram.h"

namespace omnistream {
DescriptiveStatisticsHistogram::DescriptiveStatisticsHistogram(int windowSize) : descriptiveStatistics(windowSize)
{
}

void DescriptiveStatisticsHistogram::Update(long value)
{
    descriptiveStatistics.AddValue(static_cast<double>(value));
}

long DescriptiveStatisticsHistogram::GetCount()
{
    return descriptiveStatistics.GetElementsSeen();
}

HistogramStatistics* DescriptiveStatisticsHistogram::GetStatistics()
{
    return new DescriptiveStatisticsHistogramStatistics(&descriptiveStatistics);
}
} // namespace omnistream
