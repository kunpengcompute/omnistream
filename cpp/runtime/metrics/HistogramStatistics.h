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

#ifndef HISTOGRAMSTATISTICS_H
#define HISTOGRAMSTATISTICS_H
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <stdexcept>

namespace omnistream {
class HistogramStatistics {
public:
    HistogramStatistics()
    {
    }

    virtual ~HistogramStatistics()
    {
    }

    virtual double GetQuantile(double var1) = 0;
    virtual std::vector<long> GetValues() = 0;
    virtual int Size() = 0;
    virtual double GetMean() = 0;
    virtual double GetStdDev() = 0;
    virtual long GetMax() = 0;
    virtual long GetMin() = 0;

    virtual std::string ToString()
    {
        std::ostringstream oss;
        oss << "{";
        oss << "\"mean\": " << GetMean() << ", ";
        oss << "\"stdDev\": " << GetStdDev() << ", ";
        oss << "\"max\": " << GetMax() << ", ";
        oss << "\"min\": " << GetMin() << ", ";
        oss << "\"size\": " << Size() << ", ";
        oss << "\"values\": [";
        const auto& values = GetValues();
        for (size_t i = 0; i < values.size(); ++i) {
            oss << values[i];
            if (i < values.size() - 1) {
                oss << ", ";
            }
        }
        oss << "]";
        oss << "}";
        return oss.str();
    }
};
} // namespace omnistream
#endif // HISTOGRAMSTATISTICS_H
