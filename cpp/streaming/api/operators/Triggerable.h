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

#ifndef FLINK_TNEL_TRIGGERABLE_H
#define FLINK_TNEL_TRIGGERABLE_H

#include "TimerHeapInternalTimer.h"

template <typename K, typename N>
class Triggerable {
public:
    virtual void onEventTime(TimerHeapInternalTimer<K, N>* timer) = 0;
    virtual void onProcessingTime(TimerHeapInternalTimer<K, N>* timer) = 0;
};
#endif // FLINK_TNEL_TRIGGERABLE_H
