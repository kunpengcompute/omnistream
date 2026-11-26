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

#ifndef FLINK_TNEL_STREAMINGRUNTIMECONTEXT_H
#define FLINK_TNEL_STREAMINGRUNTIMECONTEXT_H
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "functions/RuntimeContext.h"
#include "runtime/state/DefaultKeyedStateStore.h"
#include "core/api/common/TaskInfoImpl.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/ValueState.h"
#include "table/data/RowData.h"
#include "runtime/execution/OmniEnvironment.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"

template <typename K>
class StreamingRuntimeContext : public RuntimeContext {
public:
    StreamingRuntimeContext() = default;
    explicit StreamingRuntimeContext(DefaultKeyedStateStore<K>* keyedStateStore, omnistream::EnvironmentV2* env)
        : keyedStateStore(keyedStateStore),
          environment(env)
    {
    }

    void close() override
    {
    }

    void setKeyedStateStore(DefaultKeyedStateStore<K>* keyedStateStore)
    {
        this->keyedStateStore = keyedStateStore;
    }

    omnistream::StateType getStateType() const
    {
        return keyedStateStore->getStateType();
    }

    // Only a fresh task with a state manager and no restore may skip the
    // first backend lookup. Unknown contexts must rebuild derived metadata.
    bool startsWithEmptyKeyedState() const
    {
        auto manager = environment == nullptr ? nullptr : environment->getTaskStateManager();
        return manager != nullptr && !manager->hasJobManagerTaskRestore();
    }

    // This does not exist in Flink, flink send environment from constructor
    void setEnvironment(omnistream::EnvironmentV2* env)
    {
        this->environment = env;
    }
    template <typename UK, typename UV>
    MapState<UK, UV>* getMapState(MapStateDescriptor<UK, UV>* stateProperties)
    {
        return keyedStateStore->template getMapState<UK, UV>(stateProperties);
    }

    template <typename T>
    ValueState<T>* getState(ValueStateDescriptor<T>* stateProperties)
    {
        return keyedStateStore->template getState<T>(stateProperties);
    }

    template <typename T>
    ValueState<T>* getStateForWindow(ValueStateDescriptor<T>* stateProperties)
    {
        return keyedStateStore->template getStateForWindow<T>(stateProperties);
    }

    template <typename T>
    ListState<T>* getListState(ListStateDescriptor<T>* stateProperties)
    {
        return keyedStateStore->template getListState<T>(stateProperties);
    }

    int getNumberOfParallelSubtasks() override
    {
        return environment->taskConfiguration().getNumberOfSubtasks();
    }

    int getIndexOfThisSubtask() override
    {
        return environment->taskConfiguration().getIndexOfSubtask();
    }

    int getMaxNumberOfSubtasks()
    {
        return environment->taskConfiguration().getMaxNumberOfSubtasks();
    }

    long getAutoWatermarkInterval()
    {
        return static_cast<omnistream::RuntimeEnvironmentV2*>(environment)
            ->jobConfiguration()
            .getAutoWatermarkInterval();
    }

private:
    DefaultKeyedStateStore<K>* keyedStateStore;
    omnistream::EnvironmentV2* environment;
};
#endif // FLINK_TNEL_STREAMINGRUNTIMECONTEXT_H
