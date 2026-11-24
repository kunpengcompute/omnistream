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

#ifndef HISTOGRAM_H
#define HISTOGRAM_H
#include "Metric.h"
#include "HistogramStatistics.h"
#include "MetricType.h"

namespace omnistream {
class Histogram : public Metric {
public:
    ~Histogram() override
    {
    }

    virtual void Update(long var1) = 0;
    virtual long GetCount() = 0;
    virtual HistogramStatistics* GetStatistics() = 0;

    virtual MetricType GetMetricType()
    {
        return MetricType::HISTOGRAM;
    }
};
} // namespace omnistream
#endif // HISTOGRAM_H
