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

#ifndef FLINK_TNEL_LISTSTATE_H
#define FLINK_TNEL_LISTSTATE_H

#include <vector>
#include <optional>
#include "State.h"
template <typename UV>
class ListState : virtual public State {
public:
    virtual ~ListState() = default;

    // Adds a single value to the list state
    virtual void add(const UV& value) = 0;

    // Updates the list state with a new set of values
    virtual void update(const std::vector<UV>& values) = 0;

    // Retrieves the current list state
    virtual std::vector<UV>* get() = 0;

    // Merges another list into the current state
    virtual void merge(const std::vector<UV>& other) = 0;

    // Adds another list into the current list
    virtual void addAll(const std::vector<UV>& values) = 0;

    // Clears the list state
    virtual void clear() = 0;
};

using DataStreamListState = ListState<Object*>;

#endif // FLINK_TNEL_LISTSTATE_H
