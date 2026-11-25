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

#ifndef BUFFERPOOLFACTORY_H
#define BUFFERPOOLFACTORY_H

#include "BufferPool.h"

namespace omnistream {

class BufferPoolFactory {
public:
    virtual ~BufferPoolFactory() = default;

    virtual std::shared_ptr<BufferPool> createBufferPool(int numRequiredBuffers, int maxUsedBuffers) = 0;

    virtual std::shared_ptr<BufferPool> createBufferPool(
        int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel) = 0;

    virtual void destroyBufferPool(std::shared_ptr<BufferPool> bufferPool) = 0;

    virtual std::string toString() const
    {
        std::stringstream ss;
        ss << "ObjectBufferPoolFactory";
        return ss.str();
    }
};

} // namespace omnistream

#endif // BUFFERPOOLFACTORY_H
