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

#ifndef DESCRIPTIVESTATISTICSHISTOGRAM_H
#define DESCRIPTIVESTATISTICSHISTOGRAM_H
#include "Histogram.h"
#include "CircularDoubleArray.h"
#include "DescriptiveStatisticsHistogramStatistics.h"
#include "HistogramStatistics.h"

namespace omnistream {
class DescriptiveStatisticsHistogram : public Histogram {
public:
    explicit DescriptiveStatisticsHistogram(int windowSize);
    void Update(long value) override;
    long GetCount() override;
    HistogramStatistics* GetStatistics() override;

private:
    CircularDoubleArray descriptiveStatistics;
};
} // namespace omnistream
#endif // DESCRIPTIVESTATISTICSHISTOGRAM_H
