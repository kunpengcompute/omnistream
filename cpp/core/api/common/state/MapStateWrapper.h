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

#ifndef OMNISTREAM_MAPSTATEWRAPPER_H
#define OMNISTREAM_MAPSTATEWRAPPER_H
#include "State.h"
#include "MapState.h"

class MapStateWrapper : public State, public Object {
public:
    MapStateWrapper(MapState<Object*, Object*>* mapState) : mapState(mapState)
    {
    }

    // can not delete mapState, because it will be deleted when RocksdbKeyedStateBackend delete.
    ~MapStateWrapper() = default;

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
        mapState->clear();
    }

    Object* get(Object* key)
    {
        return mapState->Get(key);
    }

    void put(Object* key, Object* value)
    {
        mapState->put(key, value);
    }

    void updateOrCreate(Object* key, Object* defaultValue, std::function<std::optional<Object*>(Object*)> transformFunc)
    {
        mapState->updateOrCreate(key, defaultValue, transformFunc);
    }

    void remove(Object* key)
    {
        mapState->remove(key);
    }

    bool contains(Object* key)
    {
        return mapState->contains(key);
    }

    void update(Object* key, Object* value)
    {
        mapState->update(key, value);
    }

    java_util_Iterator* iterator()
    {
        return mapState->iterator();
    }

private:
    MapState<Object*, Object*>* mapState;
};
#endif // OMNISTREAM_MAPSTATEWRAPPER_H
