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

#ifndef FLINK_TNEL_STATEINITIALIZATIONCONTEXTIMPL_H
#define FLINK_TNEL_STATEINITIALIZATIONCONTEXTIMPL_H

#include <memory>
#include <optional>
#include <stdexcept>
#include "runtime/state/OperatorStateBackend.h"
#include "runtime/state/DefaultKeyedStateStore.h"

/**
 * StateInitializationContextImpl 类整合了所有状态初始化相关的功能
 * 它合并了原来的 ManagedInitializationContext、FunctionInitializationContext 和 StateInitializationContext 接口的功能
 */
class StateInitializationContextImpl {
private:
    /** Signal whether any state to restore was found */
    std::optional<uint64_t> restoredCheckpointId;

    OperatorStateBackend* operatorStateBackend;

    void* keyedStateStore;

public:
    /**
     * 构造函数
     * @param restoredCheckpointId 恢复的检查点ID
     * @param operatorStateBackend 操作符状态后端
     * @param keyedStateStore 键控状态存储
     */
    template <typename K>
    StateInitializationContextImpl(
        std::optional<uint64_t> restoredCheckpointId,
        OperatorStateBackend* operatorStateBackend,
        DefaultKeyedStateStore<K>* keyedStateStore)
        : restoredCheckpointId(restoredCheckpointId),
          operatorStateBackend(operatorStateBackend),
          keyedStateStore(keyedStateStore)
    {
        // No additional initialization needed
    }

    /**
     * Returns true, if state was restored from the snapshot of a previous execution.
     * 如果状态是从先前执行的快照中恢复的，则返回true。
     */
    bool isRestored() const
    {
        return restoredCheckpointId.has_value();
    }

    /**
     * Returns id of the restored checkpoint, if state was restored from the snapshot of a previous
     * execution.
     * 如果状态是从先前执行的快照中恢复的，则返回恢复的检查点的ID。
     */
    std::optional<uint64_t> getRestoredCheckpointId() const
    {
        return restoredCheckpointId;
    }

    /**
     * Returns an interface that allows for registering operator state with the backend.
     * 返回一个允许向后端注册操作符状态的接口。
     */
    OperatorStateBackend* getOperatorStateBackend() const
    {
        return operatorStateBackend;
    }

    /**
     * Returns an interface that allows for registering keyed state with the backend.
     * 返回一个允许向后端注册键控状态的接口。
     */
    template <typename K>
    DefaultKeyedStateStore<K>* getKeyedStateStore() const
    {
        if (keyedStateStore != nullptr) {
            return static_cast<DefaultKeyedStateStore<K>*>(keyedStateStore);
        }
        return nullptr;
    }
};

#endif // FLINK_TNEL_STATEINITIALIZATIONCONTEXTIMPL_H
