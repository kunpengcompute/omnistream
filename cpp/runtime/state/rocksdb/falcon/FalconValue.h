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

//
// Created by xxx on 2025/6/23.
//

#ifndef OMNISTREAM_FALCONVALUE_H
#define OMNISTREAM_FALCONVALUE_H

#include <utility>
#include <functional>
#include "basictypes/Object.h"
#include "table/data/RowData.h"

/**
 * Falcon value of RocksDBValueState、RocksDBMapState and RocksDBListState, which is composed deserialized value and a
 * flag indicates whether the state should be flushed to memTable and RocksDB.
 * @param [V] Type of deserialized value
 */
template <typename V>
class FalconValue {
public:
    FalconValue(const V& value, bool isDirty)
    {
        // V type can be Object*, RowData*(BinaryRowData*, GenericRowData* or JoinedRowData*), std::vector<long>*,
        // int32_t or int64_t. When V type is Object*, it means we are running in dataStream case; otherwise, it means
        // we are running in SQL case.
        if constexpr (std::is_same_v<V, Object*>) {
            this->value = value;
            // [refcount] when value is create and insert into cache, increase its refcount; when value is delete and
            // remove from cache, decrease its refcount. In this way, value's refcount is controlled by falcon itself.
            if (this->value != nullptr) {
                reinterpret_cast<Object*>(this->value)->getRefCount();
            }
        } else if constexpr (std::is_same_v<V, RowData*>) {
            // deep copy value. todo: GenericRowData* and JoinedRowData* do not implement copy method
            if (value != nullptr) {
                this->value = reinterpret_cast<RowData*>(value)->copy();
            }
        } else if constexpr (std::is_same_v<V, std::vector<long>*>) {
            if (value != nullptr) {
                this->value = new std::vector<long>(*value); // deep copy value using new
            }
        } else {
            this->value = value; // int32_t and int64_t type do not need deepcopy
        }
        dirty = isDirty;
    }
    FalconValue() // used for LinkedHashMap to initialize link head and link tail
    {
        if constexpr (std::is_same_v<V, Object*>) {
            this->value = V();
            // [refcount] when value is create and insert into cache, increase its refcount; when value is delete and
            // remove from cache, decrease its refcount. In this way, value's refcount is controlled by falcon itself.
            if (this->value != nullptr) {
                reinterpret_cast<Object*>(this->value)->getRefCount();
            }
        } else if constexpr (std::is_same_v<V, RowData*>) {
            // deep copy value. todo: GenericRowData* and JoinedRowData* do not implement copy method
            V value = V();
            if (value != nullptr) {
                this->value = reinterpret_cast<RowData*>(value)->copy();
            }
        } else if constexpr (std::is_same_v<V, std::vector<long>*>) {
            V value = V();
            if (value != nullptr) {
                this->value = new std::vector<long>(*value); // deep copy value using new
            }
        } else {
            this->value = V(); // int32_t and int64_t type do not need deepcopy
        }
        dirty = false;
    }
    ~FalconValue()
    {
        if constexpr (std::is_same_v<V, Object*>) {
            // [refcount] when falconValue is removed from cache, value's ref count should -1 to avoid memory leak.
            if (value != nullptr) {
                reinterpret_cast<Object*>(this->value)->putRefCount();
                value = nullptr;
            }
        } else if constexpr (std::is_same_v<V, RowData*>) {
            if (value != nullptr) {
                delete value;
                value = nullptr;
            }
        } else if constexpr (std::is_same_v<V, std::vector<long>*>) {
            if (value != nullptr) {
                delete value;
                value = nullptr;
            }
        } else {
            // if V is int type, do nothing
        }
    }

    const V& getValue() const
    {
        return value;
    }
    [[nodiscard]] bool isDirty() const
    {
        return dirty;
    }
    void markAsDirty()
    {
        dirty = true;
    }
    void markAsClean()
    {
        dirty = false;
    }

private:
    V value;
    bool dirty; // isDirty=true means the value should be flushed to memTable and RocksDB
};

#endif // OMNISTREAM_FALCONVALUE_H
