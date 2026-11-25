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

#pragma once

#include "buffer/ReadOnlySlicedNetworkBuffer.h"
#include "buffer/ReadOnlySlicedVectorBatchBuffer.h"

namespace omnistream {

// RecycleBuffer() releases the complete object for regular buffers, while the
// read-only slice implementations only release their retained parent. Delete
// the slice wrapper after recycling its parent so both kinds have one release
// entry point for channel-state ownership paths.
inline void ReleaseCheckpointBuffer(Buffer* buffer)
{
    if (buffer == nullptr) {
        return;
    }

    const bool isReadOnlySlice = dynamic_cast<datastream::ReadOnlySlicedNetworkBuffer*>(buffer) != nullptr ||
                                 dynamic_cast<ReadOnlySlicedVectorBatchBuffer*>(buffer) != nullptr;
    buffer->RecycleBuffer();
    if (isReadOnlySlice) {
        delete buffer;
    }
}

} // namespace omnistream
