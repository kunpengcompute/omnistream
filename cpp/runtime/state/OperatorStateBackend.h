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

#ifndef OMNISTREAM_OPERATORSTATEBACKEND
#define OMNISTREAM_OPERATORSTATEBACKEND

#include "runtime/checkpoint/CheckpointOptions.h"
#include "core/api/common/state/OperatorStateStore.h"

#include "SnapshotResult.h"
#include "OperatorStateHandle.h"
#include "CheckpointStreamFactory.h"

class OperatorStateBackend : public OperatorStateStore {
public:
    virtual std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>> snapshot(
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        CheckpointOptions* checkpointOptions) = 0;

    virtual void dispose() = 0;
};

#endif // OMNISTREAM_OPERATORSTATEBACKEND
