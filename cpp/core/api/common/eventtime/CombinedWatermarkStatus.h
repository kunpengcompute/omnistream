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

#ifndef OMNISTREAM_COMBINEDWATERMARKSTATUS_H
#define OMNISTREAM_COMBINEDWATERMARKSTATUS_H
#include <vector>
#include <limits>
#include <cstdint>
#include <algorithm>
namespace omnistream {
class PartialWatermark {
public:
    long GetWatermark() const;

    bool SetWatermark(long watermark);

    bool IsIdle() const
    {
        return idle;
    }

    void SetIdle(bool idle0)
    {
        this->idle = idle0;
    }

private:
    long watermark_ = INT64_MIN;
    bool idle = false;
};

class CombinedWatermarkStatus {
public:
    ~CombinedWatermarkStatus()
    {
        for (auto ptr : partialWatermarks) {
            delete ptr;
        }
        partialWatermarks.clear();
    }
    long GetCombinedWatermark() const
    {
        return combinedWatermark;
    }

    bool IsIdle() const
    {
        return idle;
    }

    void Add(PartialWatermark* output)
    {
        partialWatermarks.push_back(output);
    }
    bool Remove(PartialWatermark* o);

    bool UpdateCombinedWatermark();

private:
    std::vector<PartialWatermark*> partialWatermarks;
    long combinedWatermark = std::numeric_limits<long>::min();
    bool idle = false;
};
} // namespace omnistream
#endif // OMNISTREAM_COMBINEDWATERMARKSTATUS_H
