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
#include <unordered_set>
#include "MergingWindowAssigner.h"
#include "InternalTimeWindowAssigner.h"
#include "../TimeWindow.h"

class SessionWindowAssigner : public MergingWindowAssigner<TimeWindow>, public InternalTimeWindowAssigner {
public:
    SessionWindowAssigner(int64_t sessionGap, bool eventTime);

    ~SessionWindowAssigner() override = default;

    void mergeWindows(
        const TimeWindow& newWindow, std::set<TimeWindow>* sortedWindows, MergeResultCollector& callback) override;

    std::vector<TimeWindow> assignWindows(const RowData* element, int64_t timestamp) override;

    bool isEventTime() const override;

    SessionWindowAssigner* withEventTime() override;

    SessionWindowAssigner* withProcessingTime() override;

    static SessionWindowAssigner* withGap(int64_t size);

private:
    static TimeWindow mergeWindow(
        TimeWindow& curWindow, const TimeWindow& other, std::unordered_set<TimeWindow>& mergedWindow);

    const int64_t sessionGap_;
    const bool eventTime_;

    std::unique_ptr<std::unordered_set<TimeWindow>> reuseMergedWindows_ =
        std::make_unique<std::unordered_set<TimeWindow>>();
};
