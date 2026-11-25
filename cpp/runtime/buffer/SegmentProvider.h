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

#ifndef OMNISTREAM_SEGMENTPROVIDER_H
#define OMNISTREAM_SEGMENTPROVIDER_H
#include <iostream>
#include <vector>
#include <memory>
#include "core/memory/MemorySegment.h"
#include "ObjectSegment.h"

namespace omnistream {
class SegmentProvider {
public:
    virtual std::vector<MemorySegment*> requestUnpooledMemorySegments(int numberOfSegmentsToRequest) = 0;

    virtual void recycleUnpooledMemorySegments(const std::vector<MemorySegment*>& segments) = 0;

    virtual std::vector<ObjectSegment*> requestUnpooledObjectSegments(int numberOfSegmentsToRequest) = 0;

    virtual void recycleUnpooledObjectSegments(const std::vector<ObjectSegment*>& segments) = 0;
};
} // namespace omnistream
#endif // OMNISTREAM_SEGMENTPROVIDER_H
