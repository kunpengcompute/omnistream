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

#ifndef OMNISTREAM_WATERMARKALIGNMENTPARAMS_H
#define OMNISTREAM_WATERMARKALIGNMENTPARAMS_H

#include <string>
#include <cstdint>

class WatermarkAlignmentParams {
public:
    static const WatermarkAlignmentParams* watermarkAlignmentDisabled;

    WatermarkAlignmentParams(long maxAllowedWatermarkDrift, long updateInterval, std::string watermarkGroup) noexcept
        : maxAllowedWatermarkDrift(maxAllowedWatermarkDrift),
          updateInterval(updateInterval),
          watermarkGroup(watermarkGroup)
    {
    }

    bool IsEnabled()
    {
        return maxAllowedWatermarkDrift < INT64_MAX;
    }

    long GetMaxAllowedWatermarkDrift()
    {
        return maxAllowedWatermarkDrift;
    }

    std::string GetWatermarkGroup()
    {
        return watermarkGroup;
    }

    long GetUpdateInterval()
    {
        return updateInterval;
    }

private:
    long maxAllowedWatermarkDrift;
    long updateInterval;
    std::string watermarkGroup;
};

#endif // OMNISTREAM_WATERMARKALIGNMENTPARAMS_H
