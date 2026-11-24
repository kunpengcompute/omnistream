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

#ifndef OMNISTREAM_OBJECTBUFFERPOOL_H
#define OMNISTREAM_OBJECTBUFFERPOOL_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include "ObjectBufferProvider.h"
#include "ObjectBufferRecycler.h"
#include "ObjectBuffer.h"
#include "ObjectSegment.h"

namespace omnistream {

class ObjectBufferPool : public ObjectBufferProvider, public ObjectBufferRecycler {
public:
    ObjectBufferPool() = default;

    virtual std::shared_ptr<ObjectSegment> requestObjectSegment() = 0;
    virtual std::shared_ptr<ObjectSegment> requestObjectSegmentBlocking() = 0;
    std::string toString() const override = 0;
};

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERPOOL_H
