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

#include "CombinedWatermarkStatus.h"
#include "common.h"
namespace omnistream {
long PartialWatermark::GetWatermark() const
{
    if (idle) {
        THROW_RUNTIME_ERROR("PartialWatermark::GetWatermark(), status is idle");
    }
    return watermark_;
}

bool PartialWatermark::SetWatermark(long watermark)
{
    this->idle = false;
    bool updated = watermark > this->watermark_;
    this->watermark_ = std::max(watermark, this->watermark_);
    return updated;
}

bool CombinedWatermarkStatus::Remove(PartialWatermark* o)
{
    auto it = std::find(partialWatermarks.begin(), partialWatermarks.end(), o);
    if (it != partialWatermarks.end()) {
        partialWatermarks.erase(it);
        return true;
    }
    return false;
}

bool CombinedWatermarkStatus::UpdateCombinedWatermark()
{
    long minimumOverAllOutputs = std::numeric_limits<long>::max();

    if (partialWatermarks.empty()) {
        return false;
    }
    bool allIdle = true;
    for (auto* partialWatermark : partialWatermarks) {
        if (!partialWatermark->IsIdle()) {
            minimumOverAllOutputs = std::min(minimumOverAllOutputs, partialWatermark->GetWatermark());
            allIdle = false;
        }
    }
    this->idle = allIdle;
    if (!allIdle && minimumOverAllOutputs > combinedWatermark) {
        combinedWatermark = minimumOverAllOutputs;
        return true;
    }
    return false;
}

} // namespace omnistream
