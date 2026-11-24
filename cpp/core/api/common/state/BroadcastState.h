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

#ifndef OMNISTREAM_BROADCASTSTATE_H
#define OMNISTREAM_BROADCASTSTATE_H

#include <map>
#include <vector>
#include <utility>
#include <iterator>

#include "ReadOnlyBroadcastState.h"

template <typename K, typename V>
class BroadcastState : public ReadOnlyBroadcastState<K, V> {
public:
    virtual ~BroadcastState() = default;

    virtual void put(const K& key, const V& value) = 0;

    virtual void putAll(const std::map<K, V>& map) = 0;

    virtual void remove(const K& key) = 0;

    virtual std::vector<std::pair<K, V>> entries() = 0;
};

#endif // OMNISTREAM_BROADCASTSTATE_H
