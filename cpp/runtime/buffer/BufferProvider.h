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

#ifndef BUFFERPROVIDER_H
#define BUFFERPROVIDER_H

#include "BufferBuilder.h"
#include "BufferListener.h"
#include "Buffer.h"
#include <cstdint>
#include <memory>
#include <string>
#include <io/AvailabilityProvider.h>

namespace omnistream {
class BufferProvider : public AvailabilityProvider {
public:
    ~BufferProvider() override = default;

    virtual std::shared_ptr<Buffer> requestBuffer() = 0;

    virtual BufferBuilder* requestBufferBuilder() = 0;

    virtual BufferBuilder* requestBufferBuilder(int targetChannel, uint64_t bytes = 0) = 0;

    virtual BufferBuilder* requestBufferBuilderBlocking() = 0;

    virtual BufferBuilder* requestBufferBuilderBlocking(int targetChannel, uint64_t bytes = 0) = 0;

    virtual bool addBufferListener(std::shared_ptr<BufferListener> listener) = 0;

    virtual bool isDestroyed() = 0;

    virtual std::string toString() const = 0;
};
} // namespace omnistream

#endif // BUFFERPROVIDER_H
