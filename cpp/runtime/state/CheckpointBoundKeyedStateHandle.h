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

#ifndef FLINK_TNEL_CHECKPOINTBOUNDKEYEDSTATEHANDLE_H
#define FLINK_TNEL_CHECKPOINTBOUNDKEYEDSTATEHANDLE_H

#include "runtime/state/KeyedStateHandle.h"
#include <memory>

class CheckpointBoundKeyedStateHandle : virtual public KeyedStateHandle {
public:
    // destructor
    virtual ~CheckpointBoundKeyedStateHandle() = default;

    /**
     * Returns the ID of the checkpoint for which the handle was created or used.
     */
    virtual int64_t GetCheckpointId() const = 0;

    /**
     * Returns a new {@link CheckpointBoundKeyedStateHandle} Rebounding checkpoint id to a specific
     * checkpoint id.
     *
     * @param checkpointId rebounded checkpoint id.
     */
    virtual std::shared_ptr<CheckpointBoundKeyedStateHandle> rebound(int64_t checkpointId) const = 0;
};

#endif // FLINK_TNEL_CHECKPOINTBOUNDKEYEDSTATEHANDLE_H
