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

#ifndef TRIGGER_H
#define TRIGGER_H

#pragma once

#include <cstdint>
#include <string>

template <typename W>
class OnMergeContext;

template <typename W>
class Trigger {
public:
    virtual ~Trigger() = default;

    class TriggerContext {
    public:
        virtual ~TriggerContext() = default;

        virtual int64_t getCurrentProcessingTime() = 0;

        virtual int64_t getCurrentWatermark() = 0;

        virtual void registerProcessingTimeTimer(int64_t time) = 0;

        virtual void registerEventTimeTimer(int64_t time) = 0;

        virtual void deleteProcessingTimeTimer(int64_t time) = 0;

        virtual void deleteEventTimeTimer(int64_t time) = 0;

        virtual const std::string& getShiftTimeZone() const
        {
            static const std::string utc = "UTC";
            return utc;
        }
    };

    virtual void open(TriggerContext* ctx) = 0;

    virtual bool onElement(RowData* element, int64_t timestamp, W window) = 0;

    virtual bool onProcessingTime(int64_t time, W window) = 0;

    virtual bool onEventTime(int64_t time, W window) = 0;

    virtual bool canMerge()
    {
        return false;
    }

    virtual void onMerge(W window, OnMergeContext<W>* mergeContext)
    {
        throw std::runtime_error("This trigger does not support merging.");
    }

    virtual void clear(W window) = 0;
};

template <typename W>
class OnMergeContext : public Trigger<W>::TriggerContext {};

#endif
