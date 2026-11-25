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

#include <optional>
#include "State.h"
#include "emhash7.hpp"
#include "basictypes/java_util_Iterator.h"
#include "utils/Iterator.h"
#include "utils/Map.h"

template <typename UK, typename UV>
class MapState : virtual public State {
public:
    using ValueTransformFuncPtr = UV (*)(UV& oldValue);
    using DataStreamMapState = MapState<Object*, Object*>;
    using MapEntryV2 = typename omnistream::utils::Map<UK, UV>::Entry;
    using IteratorV2 = typename omnistream::utils::Iterator<MapEntryV2&>;

    virtual ~MapState() override = default;
    // for DataStream used
    virtual Object* Get(Object* key) = 0;
    // virtual void updateOrCreate(const UK &key, const UV defaultValue, MapState<UK, UV>::ValueTransformFuncPtr
    // transformFunc) = 0;
    virtual std::optional<UV> get(const UK& key) = 0;
    virtual void put(const UK& key, const UV& value) = 0;
    virtual void updateOrCreate(const UK& key, UV defaultValue, std::function<std::optional<UV>(UV&)> transformFunc)
    {
        // Default fallback: emulate with get, update, remove
        auto record = this->get(key); // for rockdb backend , the record need to be deleted each time
        if (record.has_value()) {
            auto maybeUpdated = transformFunc(*record);
            if (maybeUpdated) {
                this->update(key, *maybeUpdated);
            } else {
                this->remove(key); // so for the memory backend , the data need to be removed
            }
        } else if (transformFunc(defaultValue).has_value()) {
            this->put(key, defaultValue);
        }
    };
    virtual void remove(const UK& key) = 0;
    virtual bool contains(const UK& key) = 0;
    virtual void update(const UK& key, const UV& value) = 0;

    virtual typename emhash7::HashMap<UK, UV>* entries() = 0;
    virtual void clearEntriesCache()
    {
    }

    virtual java_util_Iterator* iterator() = 0; // For datastream
    virtual std::unique_ptr<IteratorV2> iteratorV2()
    {
        NOT_IMPL_EXCEPTION;
    } // For common scenario
};
