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

#include "TumblingWindowAssigner.h"

TumblingWindowAssigner::TumblingWindowAssigner(int64_t size, int64_t offset, bool eventTime)
    : size_(size),
      offset_(offset),
      eventTime_(eventTime)
{
    if (size <= 0) {
        THROW_LOGIC_EXCEPTION("TumblingWindowAssigner parameters must satisfy size > 0");
    }
}

std::vector<TimeWindow> TumblingWindowAssigner::assignWindows(const RowData* element, int64_t timestamp)
{
    int64_t start = TimeWindow::getWindowStartWithOffset(timestamp, offset_, size_);
    return {TimeWindow(start, start + size_)};
}

bool TumblingWindowAssigner::isEventTime() const
{
    return eventTime_;
}

TumblingWindowAssigner* TumblingWindowAssigner::withEventTime()
{
    return new TumblingWindowAssigner(size_, offset_, true);
}

TumblingWindowAssigner* TumblingWindowAssigner::withProcessingTime()
{
    return new TumblingWindowAssigner(size_, offset_, false);
}
