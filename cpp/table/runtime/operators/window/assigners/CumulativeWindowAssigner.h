/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <cstdint>
#include <vector>

#include "InternalTimeWindowAssigner.h"
#include "WindowAssigner.h"
#include "table/runtime/operators/window/TimeWindow.h"

class CumulativeWindowAssigner : public WindowAssigner<TimeWindow>, public InternalTimeWindowAssigner {
public:
    CumulativeWindowAssigner(int64_t maxSize, int64_t step, int64_t offset, bool eventTime);

    ~CumulativeWindowAssigner() override = default;

    std::vector<TimeWindow> assignWindows(const RowData* element, int64_t timestamp) override;

    bool isEventTime() const override;

    CumulativeWindowAssigner* withEventTime() override;

    CumulativeWindowAssigner* withProcessingTime() override;

private:
    int64_t maxSize_;
    int64_t step_;
    int64_t offset_;
    bool eventTime_;
};
