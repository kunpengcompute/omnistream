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

#include "Trigger.h"
#include "table/utils/TimeWindowUtil.h"

template <typename W>
class WindowTrigger : public Trigger<W> {
public:
    WindowTrigger() = default;

    ~WindowTrigger() override = default;

    int64_t triggerTime(const W& window)
    {
        return TimeWindowUtil::toEpochMillsForTimer(window.maxTimestamp(), ctx->getShiftTimeZone());
    }

protected:
    typename Trigger<W>::TriggerContext* ctx;
};
