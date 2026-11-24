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

#ifndef FLINK_TNEL_WATERMARKSTATUS_H
#define FLINK_TNEL_WATERMARKSTATUS_H

#include "streaming/runtime/streamrecord/StreamElement.h"
#include "common.h"

class WatermarkStatus final : public StreamElement {
public:
    const int status_;
    static constexpr int idleStatus = -1;
    static constexpr int activeStatus = 0;
    static WatermarkStatus* idle()
    {
        static WatermarkStatus s(idleStatus);
        return &s;
    }

    static WatermarkStatus* active()
    {
        static WatermarkStatus s(activeStatus);
        return &s;
    }

    explicit WatermarkStatus(int status) : status_(status)
    {
        if (status != idleStatus && status != activeStatus) {
            THROW_LOGIC_EXCEPTION("Invalid status value for WatermarkStatus");
        }
        setTag(StreamElementTag::TAG_STREAM_STATUS);
    }

    bool IsIdle()
    {
        return status_ == idleStatus;
    }

    bool IsActive()
    {
        return !IsIdle();
    }

    bool Equals(WatermarkStatus* other)
    {
        if (other == nullptr) {
            return false;
        }
        return other->GetStatus() == status_;
    }

    bool Equals(int other)
    {
        return other == status_;
    }

    int GetStatus()
    {
        return status_;
    }
};
#endif // FLINK_TNEL_WATERMARKSTATUS_H
