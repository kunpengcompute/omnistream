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

#ifndef INTERNALWINDOWPROCESSFUNCTION_H
#define INTERNALWINDOWPROCESSFUNCTION_H

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "table/runtime/operators/window/assigners/WindowAssigner.h"
#include "table/runtime/operators/window/Window.h"
#include "table/runtime/generated/NamespaceAggsHandleFunctionBase.h"
#include "table/data/RowData.h"
#include "table/utils/TimeWindowUtil.h"

template <typename K, typename W>
class Context {
public:
    virtual ~Context() = default;

    virtual K currentKey() = 0;

    virtual int64_t currentProcessingTime() = 0;

    virtual int64_t currentWatermark() = 0;

    virtual RowData* getWindowAccumulators(const W& window) = 0;

    virtual void setWindowAccumulators(const W& window, RowData* accumulators) = 0;

    virtual void clearWindowState(const W& window) = 0;

    virtual void clearPreviousState(const W& window) = 0;

    virtual void clearTrigger(const W& window) = 0;

    virtual void onMerge(const W& window, std::vector<W>& windows) = 0;

    virtual void deleteCleanupTimer(const W& window) = 0;

    virtual const std::string& getShiftTimeZone() const
    {
        static const std::string utc = "UTC";
        return utc;
    }
};

template <typename K, typename W>
class InternalWindowProcessFunction {
    static_assert(std::is_base_of_v<Window, W>, "typename W must inherit from Window");

public:
    InternalWindowProcessFunction(
        WindowAssigner<W>* windowAssigner,
        NamespaceAggsHandleFunctionBase<W>* windowAggregator,
        int64_t allowedLateness)
        : windowAssigner(windowAssigner),
          windowAggregator(windowAggregator),
          allowedLateness(allowedLateness)
    {
    }

    virtual ~InternalWindowProcessFunction() = default;

    virtual std::vector<W> assignStateNamespace(RowData* inputRow, int64_t timestamp) = 0;

    virtual std::vector<W> assignActualWindows(RowData* inputRow, int64_t timestamp) = 0;

    virtual void prepareAggregateAccumulatorForEmit(const W& window) = 0;

    virtual void cleanWindowIfNeeded(const W& window, int64_t currentTime) = 0;

    virtual void close()
    {
    }

    // the input raw pointer will be managed by unique_ptr
    virtual void open(Context<K, W>* ctx_)
    {
        this->ctx = std::unique_ptr<Context<K, W>>(ctx_);
        windowAssigner->open();
    }

protected:
    WindowAssigner<W>* windowAssigner;
    NamespaceAggsHandleFunctionBase<W>* windowAggregator;
    int64_t allowedLateness;
    std::unique_ptr<Context<K, W>> ctx;

    bool isCleanupTime(const W& window, int64_t time) const
    {
        return time == cleanupTime(window);
    }

    bool isWindowLate(const W& window) const
    {
        return windowAssigner->isEventTime() && cleanupTime(window) <= ctx->currentWatermark();
    }

private:
    int64_t cleanupTime(const W& window) const
    {
        if (windowAssigner->isEventTime()) {
            return TimeWindowUtil::toCleanupTimerMills(window.maxTimestamp(), allowedLateness, ctx->getShiftTimeZone());
        }
        return window.maxTimestamp();
    }
};

#endif
