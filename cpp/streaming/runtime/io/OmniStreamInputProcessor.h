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

#ifndef OMNISTREAM_OMNISTREAMINPUTPROCESSOR_H
#define OMNISTREAM_OMNISTREAMINPUTPROCESSOR_H

#include "runtime/io/AvailabilityProvider.h"
#include "streaming/runtime/io/DataInputStatus.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/checkpoint/channel/ChannelStateWriter.h"

namespace omnistream {

class OmniStreamInputProcessor : public AvailabilityProvider {
public:
    virtual DataInputStatus processInput() = 0;

    std::shared_ptr<CompletableFuture> GetAvailableFuture() override
    {
        return nullptr;
    }

    virtual void close() = 0;

    //        virtual CompletableFutureV2<void> *PrepareSnapshot(std::shared_ptr<ChannelStateWriter> writer, long
    //        checkpointID)
    //        {
    //            return nullptr;
    //        };

    virtual std::shared_ptr<CompletableFutureV2<void>> PrepareSnapshot(
        std::shared_ptr<ChannelStateWriter> writer, long checkpointID)
    {
        return nullptr;
    };
};
} // namespace omnistream

#endif
