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

#ifndef INTERNALTIMERSSNAPSHOT_H
#define INTERNALTIMERSSNAPSHOT_H

#pragma once

#include <memory>
#include <vector>
#include <utility>

#include "core/typeutils/TypeSerializer.h"
#include "streaming/api/operators/TimerHeapInternalTimer.h"

/**
 * Per key-group snapshot of one InternalTimerService.
 *
 * Flink's InternalTimersSnapshot carries serializers plus event-time and
 * processing-time timers for one timer service and one key-group.  This class is
 * currently only used by the CP raw keyed write path; restore readers are added
 * but not wired in this patch.
 */
template <typename K, typename N>
class InternalTimersSnapshot {
public:
    using TimerPtr = std::shared_ptr<TimerHeapInternalTimer<K, N>>;

    InternalTimersSnapshot() = default;

    InternalTimersSnapshot(
        TypeSerializer* keySerializer,
        TypeSerializer* namespaceSerializer,
        std::vector<TimerPtr> eventTimeTimers,
        std::vector<TimerPtr> processingTimeTimers)
        : keySerializer_(keySerializer),
          namespaceSerializer_(namespaceSerializer),
          eventTimeTimers_(std::move(eventTimeTimers)),
          processingTimeTimers_(std::move(processingTimeTimers))
    {
    }

    TypeSerializer* getKeySerializer() const
    {
        return keySerializer_;
    }

    void setKeySerializer(TypeSerializer* keySerializer)
    {
        keySerializer_ = keySerializer;
    }

    TypeSerializer* getNamespaceSerializer() const
    {
        return namespaceSerializer_;
    }

    void setNamespaceSerializer(TypeSerializer* namespaceSerializer)
    {
        namespaceSerializer_ = namespaceSerializer;
    }

    const std::vector<TimerPtr>& getEventTimeTimers() const
    {
        return eventTimeTimers_;
    }

    void addEventTimeTimer(const TimerPtr& timer)
    {
        eventTimeTimers_.push_back(timer);
    }

    const std::vector<TimerPtr>& getProcessingTimeTimers() const
    {
        return processingTimeTimers_;
    }

    void addProcessingTimeTimer(const TimerPtr& timer)
    {
        processingTimeTimers_.push_back(timer);
    }

private:
    TypeSerializer* keySerializer_ = nullptr;
    TypeSerializer* namespaceSerializer_ = nullptr;
    std::vector<TimerPtr> eventTimeTimers_;
    std::vector<TimerPtr> processingTimeTimers_;
};

#endif
