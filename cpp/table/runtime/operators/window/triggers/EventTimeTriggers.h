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
#include <string>
#include "core/api/common/state/ValueStateDescriptor.h"
#include "WindowTrigger.h"
#include "Trigger.h"

template <typename W>
class EventTimeTriggers {
public:
    class AfterEndOfWindowEarlyAndLate : public WindowTrigger<W> {
    public:
        AfterEndOfWindowEarlyAndLate(Trigger<W>* earlyTrigger, Trigger<W>* lateTrigger)
            : earlyTrigger(earlyTrigger),
              lateTrigger(lateTrigger),
              hasFiredOnTimeStateDesc("eventTime-afterEOW", (TypeSerializer*)nullptr)
        {
        }

        void open(typename Trigger<W>::TriggerContext* ctx) override
        {
            this->ctx = ctx;
            if (earlyTrigger != nullptr) {
                earlyTrigger->open(ctx);
            }
            if (lateTrigger != nullptr) {
                lateTrigger->open(ctx);
            }
        }

        bool onElement(RowData* element, int64_t timestamp, W window) override
        {
            bool hasFired = this->ctx->getPartitionedState(hasFiredOnTimeStateDesc).value();
            if (hasFired) {
                // this is to cover the case where we recover from a failure and the watermark
                // is Long.MIN_VALUE but the window is already in the late phase.
                return lateTrigger != nullptr && lateTrigger->onElement(element, timestamp, window);
            } else {
                if (this->triggerTime(window) <= this->ctx->getCurrentWatermark()) {
                    // we are in the late phase

                    // if there is no late trigger then we fire on every late element
                    // This also covers the case of recovery after a failure
                    // where the currentWatermark will be Long.MIN_VALUE
                    return true;
                } else {
                    // we are in the early phase
                    this->ctx->registerEventTimeTimer(this->triggerTime(window));
                    return earlyTrigger != nullptr && earlyTrigger->onElement(element, timestamp, window);
                }
            }
        }

        bool onProcessingTime(int64_t time, W window) override
        {
            bool hasFired = this->ctx->getPartitionedState(hasFiredOnTimeStateDesc).value();
            if (hasFired) {
                // late fire
                return lateTrigger != nullptr && lateTrigger->onProcessingTime(time, window);
            } else {
                // early fire
                return earlyTrigger != nullptr && earlyTrigger->onProcessingTime(time, window);
            }
        }

        bool onEventTime(int64_t time, W window)
        {
            ValueState<bool>* hasFiredState = this->ctx->getPartitionedState(hasFiredOnTimeStateDesc);
            bool hasFired = hasFiredState->value();
            if (hasFired) {
                // late fire
                return lateTrigger != nullptr && lateTrigger->onEventTime(time, window);
            } else {
                if (time == this->triggerTime(window)) {
                    // fire on time and update state
                    hasFiredState->update(true);
                    return true;
                } else {
                    // early fire
                    return earlyTrigger != nullptr && earlyTrigger->onEventTime(time, window);
                }
            }
        }

        bool canMerge() override
        {
            return (earlyTrigger == nullptr || earlyTrigger->canMerge()) &&
                   (lateTrigger == nullptr || lateTrigger->canMerge());
        }

        void onMerge(W window, OnMergeContext<W>* mergeContext) override
        {
            if (earlyTrigger != nullptr) {
                earlyTrigger->onMerge(window, mergeContext);
            }
            if (lateTrigger != nullptr) {
                lateTrigger->onMerge(window, mergeContext);
            }

            // we assume that the new merged window has not fired yet its on-time timer.
            this->ctx->getPartitionedState(hasFiredOnTimeStateDesc).update(false);
            this->ctx->registerEventTimeTimer(this->triggerTime(window));
        }

        void clear(W window) override
        {
            if (earlyTrigger != nullptr) {
                earlyTrigger->clear(window);
            }
            if (lateTrigger != nullptr) {
                lateTrigger->clear(window);
            }
            this->ctx->deleteEventTimeTimer(this->triggerTime(window));
            this->ctx->getPartitionedState(hasFiredOnTimeStateDesc).clear();
        }

    private:
        Trigger<W>* earlyTrigger;
        Trigger<W>* lateTrigger;
        ValueStateDescriptor<void> hasFiredOnTimeStateDesc;
    };

    class AfterEndOfWindow : public WindowTrigger<W> {
    public:
        AfterEndOfWindow() = default;

        void open(typename Trigger<W>::TriggerContext* ctx) override
        {
            this->ctx = ctx;
        }

        bool onElement(RowData* element, int64_t timestamp, W window)
        {
            if (this->triggerTime(window) <= this->ctx->getCurrentWatermark()) {
                // if the watermark is already past the window fire immediately
                return true;
            } else {
                this->ctx->registerEventTimeTimer(this->triggerTime(window));
                return false;
            }
        }

        bool onProcessingTime(int64_t time, W window) override
        {
            return false;
        }

        bool onEventTime(int64_t time, W window) override
        {
            return time == this->triggerTime(window);
        }

        void clear(W window) override
        {
            this->ctx->deleteEventTimeTimer(this->triggerTime(window));
        }

        void onMerge(W window, OnMergeContext<W>* mergeContext) override
        {
            this->ctx->registerEventTimeTimer(this->triggerTime(window));
        }
    };
};
