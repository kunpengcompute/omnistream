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

#pragma once

#include <cstdint>
#include <vector>

#include "InternalTimeWindowAssigner.h"
#include "PanedWindowAssigner.h"
#include "table/runtime/operators/window/TimeWindow.h"

class SlidingWindowAssigner : public PanedWindowAssigner<TimeWindow>, public InternalTimeWindowAssigner {
public:
    SlidingWindowAssigner(int64_t size, int64_t slide, int64_t offset, bool eventTime);

    ~SlidingWindowAssigner() override = default;

    std::vector<TimeWindow> assignWindows(const RowData* element, int64_t timestamp) override;

    TimeWindow assignPane(const RowData* element, int64_t timestamp) override;

    std::vector<TimeWindow> splitIntoPanes(const TimeWindow& window) override;

    TimeWindow getLastWindow(const TimeWindow& pane) override;

    bool isEventTime() const override;

    SlidingWindowAssigner* withEventTime() override;

    SlidingWindowAssigner* withProcessingTime() override;

private:
    int64_t size_;
    int64_t slide_;
    int64_t offset_;
    int64_t paneSize_;
    int32_t numPanesPerWindow_;
    bool eventTime_;
};
