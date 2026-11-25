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

#ifndef OMNISTREAM_BUFFERAVAILABILITYLISTENER_H_
#define OMNISTREAM_BUFFERAVAILABILITYLISTENER_H_

#include <memory>
#include <sstream>
#include <string>
#include "virtual_enable_shared_from_this_base.h"

namespace omnistream {
// check
class BufferAvailabilityListener : public virtual_enable_shared_from_this<BufferAvailabilityListener> {
public:
    ~BufferAvailabilityListener() override = default;

    virtual void notifyDataAvailable() = 0;

    virtual void notifyPriorityEvent(int prioritySequenceNumber)
    {
    }

    virtual std::string toString() const
    {
        std::stringstream ss;
        ss << "BufferAvailabilityListener";
        return ss.str();
    }
};

class DefaultBufferAvailabilityListener : public BufferAvailabilityListener {
public:
    DefaultBufferAvailabilityListener() = default;
    ~DefaultBufferAvailabilityListener() override = default;

    void notifyDataAvailable() override
    {
    }

    void notifyPriorityEvent(int prioritySequenceNumber) override
    {
    }

    std::string toString() const override
    {
        std::stringstream ss;
        ss << "DefaultBufferAvailabilityListener";
        return ss.str();
    }
};

} // namespace omnistream

#endif // OMNISTREAM_BUFFERAVAILABILITYLISTENER_H_
