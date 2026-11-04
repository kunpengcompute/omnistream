/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
 * See the Mulan PSL v2 for more details.
 */

#include "CumulativeWindowAssigner.h"
#include "common.h"

CumulativeWindowAssigner::CumulativeWindowAssigner(int64_t maxSize, int64_t step, int64_t offset, bool eventTime)
    : maxSize_(maxSize),
      step_(step),
      offset_(offset),
      eventTime_(eventTime)
{
    if (maxSize <= 0 || step <= 0) {
        THROW_LOGIC_EXCEPTION("CumulativeWindowAssigner parameters must satisfy step > 0 and maxSize > 0");
    }
    if (maxSize % step != 0) {
        THROW_LOGIC_EXCEPTION("CumulativeWindowAssigner requires maxSize must be an integral multiple of step.");
    }
}

std::vector<TimeWindow> CumulativeWindowAssigner::assignWindows(const RowData* element, int64_t timestamp)
{
    std::vector<TimeWindow> windows;
    int64_t start = TimeWindow::getWindowStartWithOffset(timestamp, offset_, maxSize_);
    int64_t lastEnd = start + maxSize_;
    int64_t firstEnd = TimeWindow::getWindowStartWithOffset(timestamp, offset_, step_) + step_;
    for (int64_t end = firstEnd; end <= lastEnd; end += step_) {
        windows.emplace_back(start, end);
    }
    return windows;
}

bool CumulativeWindowAssigner::isEventTime() const
{
    return eventTime_;
}

CumulativeWindowAssigner* CumulativeWindowAssigner::withEventTime()
{
    return new CumulativeWindowAssigner(maxSize_, step_, offset_, true);
}

CumulativeWindowAssigner* CumulativeWindowAssigner::withProcessingTime()
{
    return new CumulativeWindowAssigner(maxSize_, step_, offset_, false);
}
