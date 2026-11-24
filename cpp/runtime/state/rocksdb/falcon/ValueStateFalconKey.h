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

#include <utility>
#include <functional>
#include "basictypes/Object.h"
#include "table/data/RowData.h"
#include "core/utils/key_type_traits.h"

/**
 * Falcon key of RocksDBValueState, which is composed deserialized key, namespace and columnFamily.
 * @param [K] Type of deserialized key
 * @param [N] Type of deserialized namespace
 */
template <typename K, typename N>
class ValueStateFalconKey {
public:
    ValueStateFalconKey(const K& key, const N& ns)
    {
        // K type can be Object*, RowData*(BinaryRowData*, GenericRowData* or JoinedRowData*), int32_t or int64_t.
        if constexpr (std::is_same_v<K, Object*>) {
            this->key = key;
            // [refcount] when key is create and insert into cache, increase its refcount; when key is delete and remove
            // from cache, decrease its refcount. In this way, key's refcount is controlled by falcon itself.
            if (this->key != nullptr) {
                reinterpret_cast<Object*>(this->key)->getRefCount();
            }
        } else if constexpr (KeyTypeTraits<K>::isRowKey) {
            // deep copy key. todo: GenericRowData* and JoinedRowData* do not implement copy method
            if (key != nullptr) {
                this->key = static_cast<RowData*>(key)->copy();
            }
        } else if constexpr (KeyTypeTraits<K>::isSharedRowKey) {
            if (key != nullptr) {
                using KeyBaseType = unwrap_shared_ptr_t<K>;
                this->key = std::shared_ptr<KeyBaseType>(static_cast<KeyBaseType*>(key->copy()));
            }
        } else {
            this->key = key; // int32_t and int64_t type do not need deepcopy
        }
        this->ns = ns; // N type can be int64_t, TimeWindow ot VoidNamespace, do not need deepcopy.
    }
    ValueStateFalconKey()
    {
        // K type can be Object*, RowData*(BinaryRowData*, GenericRowData* or JoinedRowData*), int32_t or int64_t.
        if constexpr (std::is_same_v<K, Object*>) {
            this->key = K();
            // [refcount] when key is create and insert into cache, increase its refcount; when key is delete and remove
            // from cache, decrease its refcount. In this way, key's refcount is controlled by falcon itself.
            if (this->key != nullptr) {
                reinterpret_cast<Object*>(this->key)->getRefCount();
            }
        } else if constexpr (KeyTypeTraits<K>::isRowKey) {
            K key = K();
            if (key != nullptr) {
                // deep copy key. todo: GenericRowData* and JoinedRowData* do not implement copy method
                this->key = static_cast<RowData*>(key)->copy();
            }
        } else if constexpr (KeyTypeTraits<K>::isSharedRowKey) {
            this->key = K(); // default sharedRowKey is empty
        } else {
            this->key = K(); // int32_t and int64_t type do not need deepcopy
        }
        this->ns = N(); // N type can be int64_t, TimeWindow ot VoidNamespace, do not need deepcopy.
    }
    ~ValueStateFalconKey()
    {
        if constexpr (std::is_same_v<K, Object*>) { // if K is Object*, putRefCount
            // [refcount] when falconKey is removed from cache, key's ref count should -1 to avoid memory leak.
            if (key != nullptr) {
                reinterpret_cast<Object*>(key)->putRefCount();
                key = nullptr;
            }
        } else if constexpr (KeyTypeTraits<K>::isRowKey) { // if K is RowData*, delete if needed
            if (key != nullptr) {
                delete key;
                key = nullptr;
            }
        } else if constexpr (KeyTypeTraits<K>::isSharedRowKey) {
            key.reset();
        } else {
            // if K is int type, do nothing
        }
    }

    const K& getKey() const
    {
        return key;
    }
    const N& getNamespace() const
    {
        return ns;
    }

    // equals function for ValueStateFalconKey
    bool operator==(const ValueStateFalconKey& other) const
    {
        if constexpr (std::is_same_v<K, Object*>) { // if K is Object*, use equals to compare
            if (key == nullptr && other.key == nullptr) {
                return ns == other.ns;
            } else if (key != nullptr && other.key != nullptr) {
                return reinterpret_cast<Object*>(key)->equals(other.key) && ns == other.ns;
            } else {
                return false;
            }
        } else if constexpr (KeyTypeTraits<K>::isSharedRowKey) { // if K is std::shared<RowData>, compare after
                                                                 // dereferencing
            if (key == nullptr && other.key == nullptr) {
                return ns == other.ns;
            } else if (key != nullptr && other.key != nullptr) {
                return *key == *other.key && ns == other.ns;
            } else {
                return false;
            }
        } else { // if K is RowData* or int type, use == to compare. todo: GenericRowData == is not implemented
            return key == other.key && ns == other.ns;
        }
    }

private:
    K key;
    N ns;
};

namespace std {
// hash function for ValueStateFalconKey
template <typename K, typename N>
struct hash<ValueStateFalconKey<K, N>> {
    size_t operator()(const ValueStateFalconKey<K, N>& v) const
    {
        size_t keyHash;
        if constexpr (std::is_same_v<K, Object*>) {
            keyHash =
                v.getKey() == nullptr ? 0 : static_cast<size_t>(reinterpret_cast<Object*>(v.getKey())->hashCode());
        } else if constexpr (KeyTypeTraits<K>::isRowKey) {
            // todo: GenericRowData* hash method is not implemented
            keyHash = v.getKey() == nullptr ? 0 : static_cast<size_t>(v.getKey()->hashCode());
        } else if constexpr (KeyTypeTraits<K>::isSharedRowKey) {
            keyHash = v.getKey() == nullptr ? 0 : static_cast<size_t>(v.getKey()->hashCode());
        } else {
            keyHash = std::hash<K>()(v.getKey());
        }
        size_t nsHash = std::hash<N>()(v.getNamespace()) << 1;
        return keyHash ^ nsHash;
    }
};

// hash function for ValueStateFalconKey*
template <typename K, typename N>
struct hash<ValueStateFalconKey<K, N>*> {
    size_t operator()(const ValueStateFalconKey<K, N>* v) const
    {
        if (v == nullptr) {
            return 0;
        } else {
            return hash<ValueStateFalconKey<K, N>>{}(*v);
        }
    }
};

// equals function for ValueStateFalconKey*
template <typename K, typename N>
struct equal_to<ValueStateFalconKey<K, N>*> {
    bool operator()(const ValueStateFalconKey<K, N>* lhs, const ValueStateFalconKey<K, N>* rhs) const
    {
        if (lhs == rhs) return true;
        if (lhs == nullptr || rhs == nullptr) return false;
        return *lhs == *rhs;
    }
};
} // namespace std
