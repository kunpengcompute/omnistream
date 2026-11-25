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
#include <stdexcept>
#include <string>
#include <utility>

#include "state/CompatibleFullSnapshotAsyncWriter.h"
#include "state/CompatibleSavepointSnapshotResources.h"
#include "state/KeyedStateHandle.h"
#include "state/SnapshotStrategy.h"
#include "common.h"

// Compatible savepoint 的 snapshot strategy，与 SavepointSnapshotStrategy 对位。该类只负责把同步准备好的
// resources 和 adaptor ownership 交给 async 阶段；metadata validation、empty result 和真正的写入生命周期
// 均由后续 CompatibleFullSnapshotAsyncWriter 负责，strategy 不创建 stream、不执行 validate/save。
class CompatibleSavepointSnapshotStrategy
    : public SnapshotStrategy<KeyedStateHandle, CompatibleSavepointSnapshotResources> {
public:
    explicit CompatibleSavepointSnapshotStrategy(std::shared_ptr<CompatibleSavepointSnapshotResources> resources)
        : resources_(std::move(resources))
    {
        if (resources_ == nullptr) {
            INFO_RELEASE("Error:CompatibleSavepointSnapshotStrategy resources are null");
            throw std::invalid_argument("Compatible savepoint resources must not be null");
        }
    }

    std::shared_ptr<CompatibleSavepointSnapshotResources> syncPrepareResources(long) override
    {
        return resources_;
    }

    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<CompatibleSavepointSnapshotResources>& snapshotResources,
        long checkpointId,
        long,
        std::shared_ptr<CheckpointStreamFactory>,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer = "") override
    {
        if (snapshotResources == nullptr) {
            INFO_RELEASE(
                "Error:CompatibleSavepointSnapshotStrategy async snapshot resources are null, cp=" << checkpointId);
            throw std::invalid_argument("Compatible savepoint snapshot resources must not be null");
        }
        return std::make_shared<CompatibleFullSnapshotAsyncWriter>(
            checkpointId, checkpointOptions, snapshotResources, keySerializer, snapshotResources->takeAdaptor());
    }

private:
    std::shared_ptr<CompatibleSavepointSnapshotResources> resources_;
};
