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

#ifndef FLINK_TNEL_DEFAULTKEYEDSTATESTORE_H
#define FLINK_TNEL_DEFAULTKEYEDSTATESTORE_H
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/StateDescriptor.h"
#include "VoidNamespace.h"
#include "VoidNamespaceSerializer.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "HeapKeyedStateBackend.h"
#include "RocksdbKeyedStateBackend.h"
#include "UserFacingMapState.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/BssKeyedStateBackend.h"

template <typename K>
class DefaultKeyedStateStore {
public:
    DefaultKeyedStateStore(AbstractKeyedStateBackend<K>* backend) : backend(backend) {};
    omnistream::StateType getStateType() const
    {
        return backend->getStateType();
    }

    template <typename UK, typename UV>
    MapState<UK, UV>* getMapState(MapStateDescriptor<UK, UV>* descriptor)
    {
#ifdef WITH_OMNISTATESTORE
        if (dynamic_cast<BssKeyedStateBackend<K>*>(backend) != nullptr) {
            LOG("backend is Bss");
            using S = BssMapState<K, VoidNamespace, UK, UV>;
            return getPartitionedState<S, emhash7::HashMap<UK, UV>*>(descriptor);
        }
#endif
        if (dynamic_cast<RocksdbKeyedStateBackend<K>*>(backend) != nullptr) {
            LOG("backend is Rocksdb");
            using S = RocksdbMapState<K, VoidNamespace, UK, UV>;
            return getPartitionedState<S, emhash7::HashMap<UK, UV>*>(descriptor);
        } else {
            LOG("backend is hashMap");
            using S = HeapMapState<K, VoidNamespace, UK, UV>;
            return getPartitionedState<S, emhash7::HashMap<UK, UV>*>(descriptor);
        }
    };
    template <typename V>
    ValueState<V>* getState(ValueStateDescriptor<V>* descriptor)
    {
        LOG("get value state");
#ifdef WITH_OMNISTATESTORE
        if (dynamic_cast<BssKeyedStateBackend<K>*>(backend) != nullptr) {
            LOG("get BssKeyedStateBackend state");
            using S = BssValueState<K, VoidNamespace, V>;
            return getPartitionedState<S, V>(descriptor);
        }
#endif
        if (dynamic_cast<RocksdbKeyedStateBackend<K>*>(backend) != nullptr) {
            using S = RocksdbValueState<K, VoidNamespace, V>;
            LOG("get RocksdbKeyedStateBackend state");
            return getPartitionedState<S, V>(descriptor);
        } else {
            LOG("get HeapValueState state");
            using S = HeapValueState<K, VoidNamespace, V>;
            return getPartitionedState<S, V>(descriptor);
        }
    };
    template <typename V>
    ValueState<V>* getStateForWindow(ValueStateDescriptor<V>* descriptor)
    {
        return getPartitionedState_window<HeapValueState<K, TimeWindow, V>, V>(descriptor);
    };
    template <typename V>
    ListState<V>* getListState(ListStateDescriptor<V>* descriptor)
    {
        using S = HeapListState<K, VoidNamespace, V>;
        return getPartitionedState<S, std::vector<V>*>(descriptor);
    }

protected:
    template <typename S, typename V>
    S* getPartitionedState(StateDescriptor* stateDescriptor)
    {
        return backend->template getPartitionedState<VoidNamespace, S, V>(
            VoidNamespace(), new VoidNamespaceSerializer(), stateDescriptor);
    };

    template <typename S, typename V>
    S* getPartitionedState_window(StateDescriptor* stateDescriptor)
    {
        return backend->template getPartitionedState<TimeWindow, S, V>(
            TimeWindow(0, 0), new TimeWindow::Serializer(), stateDescriptor);
    };

private:
    AbstractKeyedStateBackend<K>* backend;
};
#endif // FLINK_TNEL_DEFAULTKEYEDSTATESTORE_H
