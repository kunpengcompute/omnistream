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

#ifndef OMNISTREAM_READONLYBROADCASTSTATE_H
#define OMNISTREAM_READONLYBROADCASTSTATE_H

#include <optional>
#include <vector>
#include <utility>

#include "State.h"

template <typename K, typename V>
class ReadOnlyBroadcastState : public State {
public:
    virtual ~ReadOnlyBroadcastState() = default;

    virtual std::optional<V> get(const K& key) const = 0;

    virtual bool contains(const K& key) const = 0;

    virtual std::vector<std::pair<K, V>> immutableEntries() const = 0;
};

#endif // OMNISTREAM_READONLYBROADCASTSTATE_H
