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

#ifndef FLINK_TNEL_STATEMAP_H
#define FLINK_TNEL_STATEMAP_H

#include <stdint.h>
#include <cstdint>
#include <functional>
#include <tuple>
#include "../StateTransformationFunction.h"
#include "runtime/state/internal/InternalKvState.h"

#ifdef EMH_SIZE_TYPE_16BIT
typedef uint16_t size_type;
#elif EMH_SIZE_TYPE_64BIT
typedef uint64_t size_type;
#else
typedef uint32_t size_type;
#endif
template <typename K, typename N, typename S>
class StateMap {
public:
    inline bool empty()
    {
        return size() == 0;
    };

    virtual inline size_type size() const = 0;

    virtual S get(const K& key, const N& nameSpace) = 0;
    virtual S get(const K& key, const N& nameSpace) const = 0;

    virtual inline bool containsKey(const K& key, const N& nmspace) const = 0;

    virtual void put(const K& key, const N& nameSpace, const S& state) = 0;
    virtual void put(K&& key, N&& nmspace, S&& state) = 0;

    virtual S putAndGetOld(const K& key, const N& nameSpace, const S& state) = 0;

    virtual void remove(const K& key, const N& nameSpace) = 0;

    virtual S removeAndGetOld(const K& key, const N& nameSpace) = 0;

    virtual typename InternalKvState<K, N, S>::StateIncrementalVisitor* getStateIncrementalVisitor(
        int recommendedMaxNumberOfReturnedRecords) = 0;

    // Approximate-size support (task-thread only; never races with put/remove).
    //
    // sumEntryCounts (the container element-count walk) was removed -- container
    // states are now sized incrementally from StateTable::liveNumElements_, so no map walk remains.
    // sampleEntryBytes (which derefed a live entry) was removed too -- the
    // metric-reporter thread must never touch a CopyOnWriteStateMap entry. Width sampling now goes
    // exclusively through sampleFirstEntry below, called on the task thread to fill cached widths.
    // Copies the first live entry's key+value into the out params (shallow; pointers copied by
    // value). Returns false when the map is empty. Used once per state to sample widths.
    virtual bool sampleFirstEntry(K& outKey, S& outValue) = 0;

    virtual ~StateMap() = default;

    //        template<typename T>
    // todo: implement this in CopyOnWriteStateMap
    //    void transform(const K& key, const N& nameSpace, const T& value, StateTransformationFunction<S, T>
    //    transformation) {};
};

#endif // FLINK_TNEL_STATEMAP_H
