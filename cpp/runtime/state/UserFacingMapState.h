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

#ifndef FLINK_TNEL_USERFACINGMAPSTATE_H
#define FLINK_TNEL_USERFACINGMAPSTATE_H
#include "core/api/common/state/MapState.h"

template <typename K, typename V>
class UserFacingMapState : public MapState<K, V> {
public:
    void clear() override {};
    UserFacingMapState(MapState<K, V>* originalState) : originalState(originalState) {};
    std::optional<V> get(const K& key) override
    {
        return originalState->get(key);
    };
    void remove(const K& key) override {};
    void put(const K& key, const V& value) override
    {
        originalState->put(key, value);
    };
    bool contains(const K& key) override
    {
        return originalState->contains(key);
    };

    typename emhash7::HashMap<K, V>::Iterator* iterator() override
    {
        return originalState->iterator();
    };

private:
    MapState<K, V>* originalState;
};

#endif // FLINK_TNEL_USERFACINGMAPSTATE_H
