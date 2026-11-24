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

#ifndef GAUGE_H
#define GAUGE_H
#include "Metric.h"
#include "MetricType.h"

namespace omnistream {
template <typename T>
class Gauge : public Metric {
public:
    ~Gauge() override = default;
    virtual T GetValue() const = 0;

    virtual MetricType GetMetricType() const
    {
        return MetricType::GAUGE;
    }
};
} // namespace omnistream
#endif // GAUGE_H
