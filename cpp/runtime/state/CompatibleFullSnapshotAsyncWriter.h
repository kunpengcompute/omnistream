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

#include <memory>
#include <string>

#include "runtime/checkpoint/OperatorSavepointAdaptor.h"
#include "state/CompatibleSavepointSnapshotResources.h"
#include "state/KeyedStateHandle.h"
#include "state/SnapshotStrategy.h"

// Compatible full savepoint 的异步写入器。该类拥有本次写入的 stream、key-group offsets、adaptor
// 调用顺序和 SnapshotResult 封装生命周期；adaptor 只在 get() 调用期写入已打开 stream，不拥有 bridge、
// checkpointId、checkpointOptions 或 snapshot resources。
class CompatibleFullSnapshotAsyncWriter : public SnapshotResultSupplier<KeyedStateHandle> {
public:
    CompatibleFullSnapshotAsyncWriter(
        long checkpointId,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<CompatibleSavepointSnapshotResources> snapshotResources,
        std::string keySerializer,
        std::unique_ptr<omnistream::OperatorSavepointAdaptor> adaptor);

    std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(std::shared_ptr<omnistream::OmniTaskBridge> bridge) override;

private:
    long checkpointId_;
    CheckpointOptions* checkpointOptions_;
    std::shared_ptr<CompatibleSavepointSnapshotResources> snapshotResources_;
    std::string keySerializer_;
    std::unique_ptr<omnistream::OperatorSavepointAdaptor> adaptor_;
};
