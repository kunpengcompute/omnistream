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

#ifndef OMNISTREAM_NETWORKBUFFERPOOL_H
#define OMNISTREAM_NETWORKBUFFERPOOL_H

#include "BufferPoolFactory.h"
#include "SegmentProvider.h"

namespace omnistream {
class NetworkBufferPool : public BufferPoolFactory, public SegmentProvider, public AvailabilityProvider {
public:
    virtual std::shared_ptr<CompletableFuture> GetAvailableFuture() = 0;
};
} // namespace omnistream

#endif // OMNISTREAM_NETWORKBUFFERPOOL_H
