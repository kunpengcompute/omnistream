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

#include "SlidingWindowAssigner.h"
#include "common.h"

SlidingWindowAssigner::SlidingWindowAssigner(int64_t size, int64_t slide, int64_t offset, bool eventTime)
    : size_(size),
      slide_(slide),
      offset_(offset),
      eventTime_(eventTime)
{
    if (size <= 0 || slide <= 0) {
        THROW_LOGIC_EXCEPTION("SlidingWindowAssigner parameters must satisfy slide > 0 and size > 0");
    }
    paneSize_ = std::gcd(size, slide);
    if (size / paneSize_ > INT32_MAX) {
        THROW_LOGIC_EXCEPTION("SlidingWindowAssigner parameters must numPanesPerWindow_ <= INT32_MAX");
    }
    numPanesPerWindow_ = static_cast<int32_t>(size / paneSize_);
}

std::vector<TimeWindow> SlidingWindowAssigner::assignWindows(const RowData* element, int64_t timestamp)
{
    std::vector<TimeWindow> windows;
    int64_t lastStart = TimeWindow::getWindowStartWithOffset(timestamp, offset_, slide_);
    for (int64_t start = lastStart; start > timestamp - size_; start -= slide_) {
        windows.emplace_back(start, start + size_);
    }
    return windows;
}

TimeWindow SlidingWindowAssigner::assignPane(const RowData* element, int64_t timestamp)
{
    int64_t start = TimeWindow::getWindowStartWithOffset(timestamp, offset_, paneSize_);
    return {start, start + paneSize_};
}

std::vector<TimeWindow> SlidingWindowAssigner::splitIntoPanes(const TimeWindow& window)
{
    std::vector<TimeWindow> panes;
    panes.reserve(numPanesPerWindow_);
    int64_t paneStart = window.getStart();
    for (int32_t i = 0; i < numPanesPerWindow_; ++i) {
        panes.emplace_back(paneStart, paneStart + paneSize_);
        paneStart += paneSize_;
    }
    return panes;
}

TimeWindow SlidingWindowAssigner::getLastWindow(const TimeWindow& pane)
{
    int64_t lastStart = TimeWindow::getWindowStartWithOffset(pane.getStart(), offset_, slide_);
    return {lastStart, lastStart + size_};
}

bool SlidingWindowAssigner::isEventTime() const
{
    return eventTime_;
}

SlidingWindowAssigner* SlidingWindowAssigner::withEventTime()
{
    return new SlidingWindowAssigner(size_, slide_, offset_, true);
}

SlidingWindowAssigner* SlidingWindowAssigner::withProcessingTime()
{
    return new SlidingWindowAssigner(size_, slide_, offset_, false);
}
