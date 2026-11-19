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

#pragma once

#include <unordered_map>
#include <type_traits>

#include "State.h"

class Object;
class RowData;

/**
 * T: such as Object*
 * */
template <typename T>
class ValueState : virtual public State {
public:
    ~ValueState() override = default;
    virtual T value() = 0;
    virtual void update(const T& value, bool copyKey = false) = 0;
    virtual void updateByBatch(std::unordered_map<RowData*, T>& pendingUpdates) {};

    // Presence of a state entry, independent of the stored value. This is what
    // makes nullable scalar state (e.g. Flink's ValueState<Long>) possible: a
    // stored 0 / LONG_MAX must not be confused with "not set". Scalar backends
    // override this with a key-existence check; for pointer values the default
    // (value() == nullptr) is exact.
    virtual bool isEmpty()
    {
        if constexpr (std::is_pointer_v<T>) {
            return value() == nullptr;
        } else {
            return false;
        }
    }

    // Reads the stored value only when present, bypassing the scalar sentinel
    // collapsing (numeric_limits<T>::max() -> default) so all values including
    // LONG_MAX round-trip. Returns false when the entry is absent.
    virtual bool getIfPresent(T& out)
    {
        if (isEmpty()) {
            return false;
        }
        out = value();
        return true;
    }
};

using DataStreamValueState = ValueState<Object*>;
