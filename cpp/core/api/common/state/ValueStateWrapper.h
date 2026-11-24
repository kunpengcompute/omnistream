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

#ifndef OMNISTREAM_VALUESTATEWRAPPER_H
#define OMNISTREAM_VALUESTATEWRAPPER_H
#include "State.h"
#include "ValueState.h"

class ValueStateWrapper : public State, public Object {
public:
    ValueStateWrapper(ValueState<Object*>* valueState) : valueState(valueState)
    {
    }

    // can not delete valueState, because it will be deleted when RocksdbKeyedStateBackend delete.
    ~ValueStateWrapper() = default;

    int hashCode() override
    {
        NOT_IMPL_EXCEPTION;
    }

    bool equals(Object* obj) override
    {
        NOT_IMPL_EXCEPTION;
    }

    std::string toString() override
    {
        NOT_IMPL_EXCEPTION;
    }

    Object* clone() override
    {
        NOT_IMPL_EXCEPTION;
    }

    void clear() override
    {
        valueState->clear();
    }

    Object* value()
    {
        return valueState->value();
    }

    void update(Object* value, bool copyKey = false)
    {
        valueState->update(value, copyKey);
    }

private:
    ValueState<Object*>* valueState;
};
#endif // OMNISTREAM_VALUESTATEWRAPPER_H
