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

#include "EventDataBufferRecycler.h"
#include "core/memory/MemorySegment.h"

namespace omnistream {
void EventDataBufferRecycler::recycle(Segment* segment)
{
    auto* memorySegment = dynamic_cast<MemorySegment*>(segment);
    if (memorySegment) {
        LOG_PART("EventDataBufferRecycler recycled " << memorySegment->getData());
    }
}
}; // namespace omnistream
