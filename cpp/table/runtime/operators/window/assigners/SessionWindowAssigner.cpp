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

#include "SessionWindowAssigner.h"

#include <unordered_set>

TimeWindow SessionWindowAssigner::mergeWindow(
    TimeWindow& curWindow, const TimeWindow& other, std::unordered_set<TimeWindow>& mergedWindow)
{
    if (curWindow.intersects(other)) {
        mergedWindow.emplace(other);
        return curWindow.cover(other);
    }
    return curWindow;
}

SessionWindowAssigner::SessionWindowAssigner(int64_t sessionGap, bool eventTime)
    : sessionGap_(sessionGap),
      eventTime_(eventTime)
{
    if (sessionGap <= 0) {
        THROW_LOGIC_EXCEPTION("SessionWindowAssigner parameters must satisfy 0 < size");
    }
}

void SessionWindowAssigner::mergeWindows(
    const TimeWindow& newWindow, std::set<TimeWindow>* sortedWindows, MergeResultCollector& callback)
{
    auto ceiling = sortedWindows->lower_bound(newWindow);
    // upper_bound功能与java set的floor还不一致
    auto floor = sortedWindows->upper_bound(newWindow);

    reuseMergedWindows_->clear();
    TimeWindow mergeResult = newWindow;
    if (ceiling != sortedWindows->end()) {
        mergeResult = mergeWindow(mergeResult, *ceiling, *reuseMergedWindows_);
    }

    if (floor != sortedWindows->begin()) {
        floor = std::prev(floor);
        mergeResult = mergeWindow(mergeResult, *floor, *reuseMergedWindows_);
    }

    if (!reuseMergedWindows_->empty()) {
        reuseMergedWindows_->emplace(newWindow);
        callback.emplace(mergeResult, reuseMergedWindows_.get());
    }
}

std::vector<TimeWindow> SessionWindowAssigner::assignWindows(const RowData* element, int64_t timestamp)
{
    return {TimeWindow(timestamp, timestamp + sessionGap_)};
}

bool SessionWindowAssigner::isEventTime() const
{
    return eventTime_;
}

SessionWindowAssigner* SessionWindowAssigner::withEventTime()
{
    return new SessionWindowAssigner(sessionGap_, true);
}

SessionWindowAssigner* SessionWindowAssigner::withProcessingTime()
{
    return new SessionWindowAssigner(sessionGap_, false);
}

SessionWindowAssigner* SessionWindowAssigner::withGap(int64_t size)
{
    return new SessionWindowAssigner(size, false);
}
