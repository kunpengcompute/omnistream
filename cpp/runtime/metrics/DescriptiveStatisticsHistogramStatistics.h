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

#ifndef DESCRIPTIVESTATISTICSHISTOGRAMSTATISTICS_H
#define DESCRIPTIVESTATISTICSHISTOGRAMSTATISTICS_H
#include <vector>
#include <string>
#include "HistogramStatistics.h"
#include "CircularDoubleArray.h"

namespace omnistream {
class DescriptiveStatisticsHistogramStatistics : public HistogramStatistics {
public:
    explicit DescriptiveStatisticsHistogramStatistics(CircularDoubleArray* cda);
    ~DescriptiveStatisticsHistogramStatistics() override;
    double GetQuantile(double quantile) override;
    std::vector<long> GetValues() override;
    int Size() override;
    double GetMean() override;
    double GetStdDev() override;
    long GetMax() override;
    long GetMin() override;

private:
    CircularDoubleArray* circularArray;
    std::vector<double> GetData() const; // Helper to retrieve current data
};
} // namespace omnistream
#endif // DESCRIPTIVESTATISTICSHISTOGRAMSTATISTICS_H
