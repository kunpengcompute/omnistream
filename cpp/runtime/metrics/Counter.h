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

#ifndef COUNTER_H
#define COUNTER_H
#include "Metric.h"
#include "MetricType.h"

namespace omnistream {
// Define MetricType if not defined elsewhere.
class Counter : public Metric {
public:
    ~Counter() override = default;
    virtual void Inc() = 0;
    virtual void Inc(long var1) = 0;
    virtual void Dec() = 0;
    virtual void Dec(long var1) = 0;
    virtual long GetCount() = 0;

    virtual MetricType GetMetricType() const
    {
        return MetricType::COUNTER;
    }
};
} // namespace omnistream
#endif // COUNTER_H
