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

#pragma once

#include "State.h"

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
};

using DataStreamValueState = ValueState<Object*>;
