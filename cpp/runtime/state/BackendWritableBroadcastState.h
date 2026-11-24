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

#ifndef OMNISTREAM_BACKENDWRITABLEBROADCASTSTATE_H
#define OMNISTREAM_BACKENDWRITABLEBROADCASTSTATE_H

#include <memory>

#include "core/api/common/state/BroadcastState.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "runtime/state/RegisteredBroadcastStateBackendMetaInfo.h"

template <typename K, typename V>
class BackendWritableBroadcastState : public BroadcastState<K, V> {
public:
    virtual ~BackendWritableBroadcastState() = default;

    virtual std::shared_ptr<BackendWritableBroadcastState<K, V>> deepCopy() = 0;

    virtual long write(long startPos, DataOutputSerializer& out) = 0;

    virtual void setStateMetaInfo(const std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo>& stateMetaInfo) = 0;

    virtual std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo> getStateMetaInfo() const = 0;
};

#endif // OMNISTREAM_BACKENDWRITABLEBROADCASTSTATE_H
