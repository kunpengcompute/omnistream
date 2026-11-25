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

#ifndef OMNISTREAM_CHECKPOINTABLEKEYEDSTATEBACKEND
#define OMNISTREAM_CHECKPOINTABLEKEYEDSTATEBACKEND

#include "KeyedStateBackend.h"
#include "KeyGroupRange.h"
#include "CheckpointStreamFactory.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "state/SavepointResources.h"
#include "SnapshotResult.h"
#include "KeyedStateHandle.h"
#include <future>
namespace omnistream {
class OperatorStateMetricGroup;
}

template <typename K>
class CheckpointableKeyedStateBackend : public KeyedStateBackend<K> {
public:
    virtual KeyGroupRange* getKeyGroupRange() = 0;
    virtual std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> snapshot(
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        CheckpointOptions* checkpointOptions) = 0;

    ~CheckpointableKeyedStateBackend() = default;

    virtual std::shared_ptr<SavepointResources> savepoint() = 0;

    // 为需要 adaptor 转换的 compatible savepoint 准备快照资源，默认复用普通 savepoint()。
    // OmniIsCompatible 分支语义上仍复用普通 savepoint()，不应因为请求格式为 COMPATIBLE 而自动调用本入口。
    virtual std::shared_ptr<SavepointResources> compatibleSavepoint()
    {
        return savepoint();
    }

    virtual void SetOperatorStateMetricGroup(omnistream::OperatorStateMetricGroup* group)
    {
        operatorStateMetricGroup_ = group;
    }

    omnistream::OperatorStateMetricGroup* getOperatorStateMetricGroup() const
    {
        return operatorStateMetricGroup_;
    }

protected:
    omnistream::OperatorStateMetricGroup* operatorStateMetricGroup_ = nullptr;
};

#endif // OMNISTREAM_CHECKPOINTABLEKEYEDSTATEBACKEND
