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
#include <unordered_map>
#include <vector>

#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyValueStateIterator.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"

namespace compatible_savepoint_test {

// Compatible savepoint UT 共享的最小 FullSnapshotResources 实现。调用方可选择是否提供 metadata 以及 key-group
// 范围，并可通过 cleanupCount() 观察资源清理；它不模拟 KV 迭代或 key serializer，相关测试应自带专用桩。
class CompatibleSavepointTestFullSnapshotResources : public FullSnapshotResources {
public:
    CompatibleSavepointTestFullSnapshotResources(bool withMetaInfo, int startKeyGroup = 0, int endKeyGroup = 0)
        : keyGroupRange_(std::make_unique<KeyGroupRange>(startKeyGroup, endKeyGroup))
    {
        if (withMetaInfo) {
            metaInfos_.push_back(
                std::make_shared<StateMetaInfoSnapshot>(
                    "state",
                    StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
                    std::unordered_map<std::string, std::string>{},
                    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{}));
        }
    }

    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() override
    {
        return metaInfos_;
    }

    KeyGroupRange* getKeyGroupRange() override
    {
        return keyGroupRange_.get();
    }

    TypeSerializer* getKeySerializer() override
    {
        return nullptr;
    }

    std::shared_ptr<KeyValueStateIterator> createKVStateIterator() override
    {
        return nullptr;
    }

    void cleanup() override
    {
        cleanupCount_++;
    }

    int cleanupCount() const
    {
        return cleanupCount_;
    }

private:
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos_;
    std::unique_ptr<KeyGroupRange> keyGroupRange_;
    int cleanupCount_ = 0;
};

inline FlinkSavepointAdaptorInfo makeCompatibleTestAdaptorInfo()
{
    return {FlinkSavepointAdaptorType::GroupAggAdaptor, "group agg compatible adaptor"};
}

} // namespace compatible_savepoint_test
