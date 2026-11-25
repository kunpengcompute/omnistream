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

#ifndef EVENTDATABUFFERRECYCLER_H
#define EVENTDATABUFFERRECYCLER_H

#include "BufferRecycler.h"
#include "core/include/common.h"
namespace omnistream {
class EventDataBufferRecycler : public BufferRecycler {
public:
    ~EventDataBufferRecycler() override = default;

    static std::shared_ptr<EventDataBufferRecycler> GetInstance()
    {
        static std::shared_ptr<EventDataBufferRecycler> instance = std::make_shared<EventDataBufferRecycler>();
        return instance;
    }

    // Recycle the segment
    void recycle(Segment* objectBuffer) override;

    // Convert to string representation
    [[nodiscard]] std::string toString() const override
    {
        return "EventDataBufferRecycler";
    }
};
} // namespace omnistream

#endif // EVENTDATABUFFERRECYCLER_H
