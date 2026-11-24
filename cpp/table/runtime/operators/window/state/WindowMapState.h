/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef WINDOWMAPSTATE_H
#define WINDOWMAPSTATE_H

#include "core/api/common/state/MapState.h"
#include "runtime/state/internal/InternalKvState.h"
#include "WindowState.h"

/**
 * The windowed wrapper of MapState, mirroring Flink's
 * org.apache.flink.table.runtime.operators.window.state.WindowMapState.
 * The underlying keyed state is namespace-partitioned by the window end
 * (Long), so all operations first switch the current namespace.
 *
 * KeyType: the keyed-stream key type (e.g. std::shared_ptr<RowData>).
 * W: the window namespace type (int64_t window end).
 * UK/UV: the user key/value types of the wrapped map state
 *        (e.g. RowData* sort key -> std::vector<RowData*>* row list,
 *        the native counterpart of Flink's MapState<RowData, List<RowData>>).
 */
template <typename KeyType, typename W, typename UK, typename UV>
class WindowMapState : public WindowState<W> {
public:
    using StateMap = emhash7::HashMap<UK, UV>;

    // kvState and mapState must point to the same state object
    // (e.g. HeapMapState / RocksdbMapState implement both interfaces).
    WindowMapState(InternalKvState<KeyType, W, StateMap*>* kvState, MapState<UK, UV>* mapState)
        : kvState_(kvState), mapState_(mapState)
    {
    }

    ~WindowMapState() override
    {
        delete mapState_;
    }

    void clear(W window) override
    {
        kvState_->setCurrentNamespace(window);
        mapState_->clear();
    }

    std::optional<UV> get(W window, const UK& userKey)
    {
        kvState_->setCurrentNamespace(window);
        return mapState_->get(userKey);
    }

    void put(W window, const UK& userKey, const UV& userValue)
    {
        kvState_->setCurrentNamespace(window);
        mapState_->put(userKey, userValue);
    }

    void remove(W window, const UK& userKey)
    {
        kvState_->setCurrentNamespace(window);
        mapState_->remove(userKey);
    }

    // Returns the live map on HEAP (owned by the state table, do not free) or
    // a deserialized copy on ROCKSDB (borrowed from the entries cache; call
    // clearEntriesCache() to release it together with its keys).
    emhash7::HashMap<UK, UV>* entries(W window)
    {
        kvState_->setCurrentNamespace(window);
        return mapState_->entries();
    }

    // ROCKSDB only: drops the per-key deserialized copies; frees the cached
    // maps and their key objects (values stay caller-owned). No-op on HEAP.
    void clearEntriesCache()
    {
        mapState_->clearEntriesCache();
    }

private:
    InternalKvState<KeyType, W, StateMap*>* kvState_;
    MapState<UK, UV>* mapState_;
};

#endif
