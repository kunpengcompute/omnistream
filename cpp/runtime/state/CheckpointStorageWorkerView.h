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

#ifndef FLINK_TNEL_CHECKPOINTSTORAGEWORKERVIEW_H
#define FLINK_TNEL_CHECKPOINTSTORAGEWORKERVIEW_H
#include "KeyGroupRange.h"

#include <cstdint>
#include <stdexcept>
#include "CheckpointStorageLocationReference.h"
#include "CheckpointStreamFactory.h"
#include "CheckpointStateToolset.h"
namespace omnistream {
class CheckpointStorageWorkerView {
public:
    virtual std::shared_ptr<CheckpointStreamFactory> resolveCheckpointStorageLocation(
        int64_t checkpointId, std::shared_ptr<CheckpointStorageLocationReference> reference) = 0;

    virtual CheckpointStateOutputStream* createTaskOwnedStateStream() = 0;

    virtual CheckpointStateToolset* createTaskOwnedCheckpointStateToolset() = 0;

    virtual ~CheckpointStorageWorkerView() = default;
};
} // namespace omnistream
#endif // FLINK_TNEL_CHECKPOINTSTORAGEWORKERVIEW_H
