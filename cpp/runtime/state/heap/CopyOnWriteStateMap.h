/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * Modify this part of the code to implement native execution of Flink operators.
 */

#ifndef COPYONWRITESTATEMAP_H
#define COPYONWRITESTATEMAP_H
// emhash7::HashMap for C++11/14/17
// version 2.2.5
// https://github.com/ktprime/emhash/blob/master/hash_table7.hpp
//
// Licensed under the MIT License <http://opensource.org/licenses/MIT>.
// SPDX-License-Identifier: MIT
// Copyright (c) 2019-2024 Huang Yuanbing & bailuzhou AT 163.com
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE

// From
// NUMBER OF PROBES / LOOKUP       Successful            Unsuccessful
// Quadratic collision resolution  1 - ln(1-L) - L/2     1/(1-L) - L - ln(1-L)
// Linear collision resolution     [1+1/(1-L)]/2         [1+1/(1-L)2]/2
// separator chain resolution      1 + L / 2             exp(-L) + L

// -- enlarge_factor --           0.10  0.50  0.60  0.75  0.80  0.90  0.99
// QUADRATIC COLLISION RES.
//    probes/successful lookup    1.05  1.44  1.62  2.01  2.21  2.85  5.11
//    probes/unsuccessful lookup  1.11  2.19  2.82  4.64  5.81  11.4  103.6
// LINEAR COLLISION RES.
//    probes/successful lookup    1.06  1.5   1.75  2.5   3.0   5.5   50.5
//    probes/unsuccessful lookup  1.12  2.5   3.6   8.5   13.0  50.0
// SEPARATE CHAN RES.
//    probes/successful lookup    1.05  1.25  1.3   1.25  1.4   1.45  1.50
//    probes/unsuccessful lookup  1.00  1.11  1.15  1.22  1.25  1.31  1.37
//    clacul/unsuccessful lookup  1.01  1.25  1.36, 1.56, 1.64, 1.81, 1.97

/****************
    under random hashCodes, the frequency of nodes in bins follows a Poisson
distribution(http://en.wikipedia.org/wiki/Poisson_distribution) with a parameter of about 0.5
on average for the default resizing threshold of 0.75, although with a large variance because
of resizing granularity. Ignoring variance, the expected occurrences of list size k are
(exp(-0.5) * pow(0.5, k)/factorial(k)). The first values are:
0: 0.60653066
1: 0.30326533
2: 0.07581633
3: 0.01263606
4: 0.00157952
5: 0.00015795
6: 0.00001316
7: 0.00000094
8: 0.00000006

  ============== buckets size ration ========
    1   1543981  0.36884964|0.36787944  36.885
    2    768655  0.36725597|0.36787944  73.611
    3    256236  0.18364065|0.18393972  91.975
    4     64126  0.06127757|0.06131324  98.102
    5     12907  0.01541710|0.01532831  99.644
    6      2050  0.00293841|0.00306566  99.938
    7       310  0.00051840|0.00051094  99.990
    8        49  0.00009365|0.00007299  99.999
    9         4  0.00000860|0.00000913  100.000
========== collision miss ration ===========
  _num_filled aver_size k.v size_kv = 4185936, 1.58, x.x 24
  collision,possion,cache_miss hit_find|hit_miss, load_factor = 36.73%,36.74%,31.31%  1.50|2.00, 1.00
============== buckets size ration ========
*******************************************************/

#pragma once

#include <cstring>
#include <string>
#include <cmath>
#include <cstdlib>
#include <type_traits>
#include <cassert>
#include <utility>
#include <cstdint>
#include <functional>
#include <iterator>
#include <algorithm>
#include <emhash7.hpp>
#include "StateMap.h"
#include "core/typeutils/TypeSerializer.h"
#include "../internal/InternalKvState.h"
#include "table/data/binary/BinaryRowData.h"

namespace cowstatemap_detail {
// Type trait to detect emhash7::HashMap pointer types
template <typename T>
struct IsEmhashMapPtrType : std::false_type {};
template <typename UK, typename UV>
struct IsEmhashMapPtrType<emhash7::HashMap<UK, UV>*> : std::true_type {};

// Rank owns each row-list value and every row remaining in that list.
template <typename T>
struct IsRowListMapPtrType : std::false_type {};
template <>
struct IsRowListMapPtrType<emhash7::HashMap<RowData*, std::vector<RowData*>*>*> : std::true_type {};

// Helper to check if a type is emhash7::HashMap<Object*, ...>*
template <typename T>
struct HasObjectKeyInMap : std::false_type {};
template <typename UV>
struct HasObjectKeyInMap<emhash7::HashMap<Object*, UV>*> : std::true_type {};

template <typename T>
struct HasObjectValueInMap : std::false_type {};
template <typename UK>
struct HasObjectValueInMap<emhash7::HashMap<UK, Object*>*> : std::true_type {};

/**
 * Clean up Object* entries inside an inner emhash7::HashMap before deleting it.
 * This handles the case where HeapMapState<K, N, Object*, Object*>'s inner
 * HashMap is destroyed — the emhash7 destructor doesn't call putRefCount.
 */
template <typename V>
inline void cleanupInnerMapAndDelete(V mapPtr)
{
    if (mapPtr == nullptr) return;
    if constexpr (IsEmhashMapPtrType<V>::value) {
        if constexpr (IsRowListMapPtrType<V>::value) {
            for (auto& entry : *mapPtr) {
                if (entry.second != nullptr) {
                    for (RowData* row : *entry.second) {
                        delete row;
                    }
                    delete entry.second;
                }
            }
            // The emhash destructor owns the RowData* keys. Do not clear the
            // map here: that would prevent its destructor from deleting them.
        } else if constexpr (HasObjectKeyInMap<V>::value || HasObjectValueInMap<V>::value) {
            for (auto& entry : *mapPtr) {
                if constexpr (HasObjectKeyInMap<V>::value) {
                    Object* key = static_cast<Object*>(entry.first);
                    if (key != nullptr) {
                        key->putRefCount();
                    }
                }
                if constexpr (HasObjectValueInMap<V>::value) {
                    Object* value = static_cast<Object*>(entry.second);
                    if (value != nullptr) {
                        value->putRefCount();
                    }
                }
            }
            mapPtr->clear();
        }
    }
    delete mapPtr;
}
} // namespace cowstatemap_detail
#if EMH_WY_HASH
#include "wyhash.h"
#endif

#ifdef EMH_NEW
#undef EMH_KEY
#undef EMH_VAL
#undef EMH_PKV
#undef EMH_NEW
#undef EMH_SET
#undef EMH_BUCKET
#undef EMH_EMPTY
#undef EMH_MASK
#endif

// likely/unlikely
#if (__GNUC__ >= 4 || __clang__)
#define EMH_LIKELY(condition) __builtin_expect(condition, 1)
#define EMH_UNLIKELY(condition) __builtin_expect(condition, 0)
#else
#define EMH_LIKELY(condition) condition
#define EMH_UNLIKELY(condition) condition
#endif

#define EMH_KEY(p, n) p[n].first
#define EMH_VAL(p, n) p[n].second
#define EMH_NMSPACE(p, n) p[n].third
#define EMH_BUCKET(p, n) p[n].bucket
#define EMH_PKV(p, n) p[n]
#define EMH_NEW(key, val, nmspace, bucket, version)                             \
    new (_pairs + (bucket)) PairT(key, val, nmspace, bucket, version, version); \
    _num_filled++;                                                              \
    EMH_SET(bucket)

#ifndef EMH_MASK
#define EMH_MASK(n) uint8_t(1 << (n % MASK_BIT))
#endif
#ifndef EMH_SET
#define EMH_SET(n) _bitmask[n / MASK_BIT] &= ~(EMH_MASK(n))
#endif
#ifndef EMH_CLS
#define EMH_CLS(n) _bitmask[n / MASK_BIT] |= EMH_MASK(n)
#endif
#ifndef EMH_EMPTY
#define EMH_EMPTY(n) (_bitmask[n / MASK_BIT] & (EMH_MASK(n))) != 0
#endif

namespace omnistream {

#ifdef EMH_SIZE_TYPE_16BIT
static constexpr size_type INACTIVE = 0xFFFF;
#elif EMH_SIZE_TYPE_64BIT
static constexpr size_type INACTIVE = 0 - 0x1ull;
#else
static constexpr size_type INACTIVE = -1u;
#endif

#ifndef EMH_MALIGN
static constexpr uint32_t EMH_MALIGN = 16;
#endif
static_assert(EMH_MALIGN >= 16 && 0 == (EMH_MALIGN & (EMH_MALIGN - 1)));

#ifndef EMH_SIZE_TYPE_16BIT
static_assert((int)INACTIVE < 0, "INACTIVE must negative (to int)");
#endif

// count the leading zero bit
static inline size_type CTZ(size_t n)
{
#if defined(__x86_64__) || (__BYTE_ORDER__ && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)

#elif __BIG_ENDIAN__ || (__BYTE_ORDER__ && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    n = __builtin_bswap64(n);
#else
    static uint32_t endianness = 0x12345678;
    const auto is_big = *(const char*)&endianness == 0x12;
    if (is_big) n = __builtin_bswap64(n);
#endif

#if defined(__LP64__) || (SIZE_MAX == UINT64_MAX) || defined(__x86_64__)
    auto index = __builtin_ctzll(n);
#elif 1
    auto index = __builtin_ctzl(n);
#else
#if defined(__LP64__) || (SIZE_MAX == UINT64_MAX) || defined(__x86_64__)
    size_type index;
    __asm__("bsfq %1, %0\n" : "=r"(index) : "rm"(n) : "cc");
#else
    size_type index;
    __asm__("bsf %1, %0\n" : "=r"(index) : "rm"(n) : "cc");
#endif
#endif

    return (size_type)index;
}

template <typename K, typename N, typename S>
struct CopyOnWriteMapEntry {
    CopyOnWriteMapEntry(const CopyOnWriteMapEntry& rhs, int entryV)
        : second(rhs.second),
          first(rhs.first),
          third(rhs.third)
    {
        bucket = rhs.bucket;
        stateVersion = rhs.stateVersion;
        entryVersion = entryV;
    }
    CopyOnWriteMapEntry(CopyOnWriteMapEntry&& rhs, int entryV) noexcept
        : second(std::move(rhs.second)),
          first(std::move(rhs.first)),
          third(std::move(rhs.third))
    {
        bucket = rhs.bucket;
        stateVersion = rhs.stateVersion;
        entryVersion = entryV;
    }

    CopyOnWriteMapEntry(const K& key, const S& val, const N& nmspace, size_type ibucket, int stateV, int entryV)
        : second(val),
          first(key),
          third(nmspace)
    {
        bucket = ibucket;
        stateVersion = stateV;
        entryVersion = entryV;
    }

    CopyOnWriteMapEntry(K&& key, S&& val, N&& nmspace, size_type ibucket, int stateV, int entryV)
        : second(std::move(val)),
          first(std::move(key)),
          third(std::move(nmspace))
    {
        bucket = ibucket;
        stateVersion = stateV;
        entryVersion = entryV;
    }

    void setState(S state, int mapVersion)
    {
        // naturally, we can update the state version every time we replace the old state with a
        // different object
        if (state != second) {
            second = state;
            stateVersion = mapVersion;
        }
    }

    bool equals(const CopyOnWriteMapEntry<K, N, S>& rhs)
    {
        // todo: be careful with the == here. If State is a pointer, the meaing of == might not be what we want
        return second == rhs.second && first == rhs.first && third == rhs.third;
    }
    void swap(CopyOnWriteMapEntry<K, N, S>& o)
    {
        std::swap(second, o.second);
        std::swap(first, o.first);
        std::swap(third, o.third);
    }
#if EMH_ORDER_KV || EMH_SIZE_TYPE_64BIT
    K first;
    size_type bucket;
    S second;
#else
    S second;
    size_type bucket;
    K first;
#endif
    N third;
    int entryVersion;
    int stateVersion;
};

/// A cache-friendly hash table with open addressing, linear/qua probing and power-of-two capacity
/// Customized hasher and equaliser should take both key and namespace as input
struct CombineHash {
    template <typename K, typename N>
    size_t operator()(const K& _key, const N& _nmspace) const
    {
        return std::hash<K>()(_key) ^ std::hash<N>()(_nmspace);
    }
};
struct CombineEqual {
    template <typename K, typename N>
    size_t operator()(const K& _key1, const K& _key2, const N& _nmspace1, const N& _nmspace2) const
    {
        if constexpr (std::is_pointer_v<K>) {
            if (_key1 == nullptr && _key2 == nullptr) {
                return std::equal_to<N>{}(_nmspace1, _nmspace2);
            } else if (_key1 == nullptr || _key2 == nullptr) {
                return false;
            }
        }
        return std::equal_to<K>{}(_key1, _key2) && std::equal_to<N>{}(_nmspace1, _nmspace2);
    }
};

template <typename T>
struct NeedClone {
    static constexpr bool value = false;
};

template <>
struct NeedClone<Object*> {
    static constexpr bool value = true;
};

template <>
struct NeedClone<BinaryRowData*> {
    static constexpr bool value = true;
};

template <>
struct NeedClone<RowData*> {
    static constexpr bool value = true;
};

template <typename KeyT, typename N, typename ValueT, typename HashT = CombineHash, typename EqT = CombineEqual>
class CopyOnWriteStateMap : public StateMap<KeyT, N, ValueT> {
#ifndef EMH_DEFAULT_LOAD_FACTOR
    constexpr static float EMH_DEFAULT_LOAD_FACTOR = 0.80f;
#endif
    constexpr static float EMH_MIN_LOAD_FACTOR = 0.25f;

public:
    typedef CopyOnWriteStateMap<KeyT, N, ValueT> htype;
    typedef CopyOnWriteMapEntry<KeyT, N, ValueT> value_pair;
    typedef CopyOnWriteMapEntry<KeyT, N, ValueT> PairT;

    CopyOnWriteStateMap(TypeSerializer* serializer, size_type bucket = 2, float mlf = EMH_DEFAULT_LOAD_FACTOR) noexcept
    {
        stateSerializer = serializer;
        init(bucket, mlf);
    }

    class iterator : public InternalKvState<KeyT, N, ValueT>::StateIncrementalVisitor {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef std::ptrdiff_t difference_type;
        typedef value_pair value_type;

        typedef value_pair* pointer;
        typedef value_pair& reference;

        iterator() = default;
        iterator(const iterator& it) : _map(it._map), _bucket(it._bucket), _from(it._from), _bmask(it._bmask)
        {
        }

        // iterator(const htype* hash_map, size_type bucket, bool) : _map(hash_map), _bucket(bucket) { init(); }
#if EMH_ITER_SAFE
        iterator(const htype* hash_map, size_type bucket) : _map(hash_map), _bucket(bucket)
        {
            init();
        }
#else
        iterator(const htype* hash_map, size_type bucket) : _map(hash_map), _bucket(bucket), _bmask(0)
        {
            _from = size_type(-1);
        }
#endif

        // -------- Flink APIs-----------
        bool hasNext() override
        {
            return _bucket != _map->_num_buckets;
        }
        ValueT nextEntries() override
        {
            auto& val = _map->EMH_VAL(_pairs, _bucket);
            this->operator++();
            return val;
        }
        void init()
        {
            _from = (_bucket / SIZE_BIT) * SIZE_BIT;
            if (_bucket < _map->bucket_count()) {
                _bmask = *(size_t*)((size_t*)_map->_bitmask + _from / SIZE_BIT);
                _bmask |= (1ull << _bucket % SIZE_BIT) - 1;
                _bmask = ~_bmask;
            } else {
                _bmask = 0;
            }
        }

        size_type bucket() const
        {
            return _bucket;
        }

        void clear(size_type bucket)
        {
            if (_bucket / SIZE_BIT == bucket / SIZE_BIT) _bmask &= ~(1ull << (bucket % SIZE_BIT));
        }

        iterator& next()
        {
            goto_next_element();
            return *this;
        }

        iterator& operator++()
        {
#ifndef EMH_ITER_SAFE
            if (_from == (size_type)-1) init();
#endif
            _bmask &= _bmask - 1;
            goto_next_element();
            return *this;
        }

        iterator operator++(int)
        {
#ifndef EMH_ITER_SAFE
            if (_from == (size_type)-1) init();
#endif
            iterator old = *this;
            _bmask &= _bmask - 1;
            goto_next_element();
            return old;
        }

        reference operator*() const
        {
            return _map->EMH_PKV(_pairs, _bucket);
        }

        pointer operator->() const
        {
            return &(_map->EMH_PKV(_pairs, _bucket));
        }

        bool operator==(const iterator& rhs) const
        {
            return _bucket == rhs._bucket;
        }
        bool operator!=(const iterator& rhs) const
        {
            return _bucket != rhs._bucket;
        }

    public:
        const htype* _map;
        size_type _bucket;
        size_type _from;
        size_t _bmask;

    private:
        void goto_next_element()
        {
            if (_bmask != 0) {
                _bucket = _from + CTZ(_bmask);
                return;
            }

            do {
                _bmask = ~*(size_t*)((size_t*)_map->_bitmask + (_from += SIZE_BIT) / SIZE_BIT);
            } while (_bmask == 0);

            _bucket = _from + CTZ(_bmask);
        }
    };
    iterator* getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) override
    {
        iterator* it = new iterator(this->begin());
        return it;
    }
    inline size_type size() const
    {
        return _num_filled;
    }
    inline bool empty() const
    {
        return _num_filled == 0;
    }

    // Copies the first live entry's key+value out; false if empty. Task-thread only.
    bool sampleFirstEntry(KeyT& outKey, ValueT& outValue) override
    {
        auto it = begin();
        if (it == end()) {
            return false;
        }
        outKey = it->first;
        outValue = it->second;
        return true;
    }

    /// Returns the matching ValueT or nullptr if k isn't found.
    ValueT get(const KeyT& key, const N& nmspace) noexcept override
    {
        const auto bucket = find_filled_bucket(key, nmspace);
        if constexpr (std::is_pointer<ValueT>::value) {
            return bucket == _num_buckets ? nullptr : EMH_VAL(_pairs, bucket);
        } else {
            return bucket == _num_buckets ? std::numeric_limits<ValueT>::max() : EMH_VAL(_pairs, bucket);
        }
    }
    /// Const version of the above
    ValueT get(const KeyT& key, const N& nmspace) const noexcept override
    {
        const auto bucket = find_filled_bucket(key, nmspace);
        if constexpr (std::is_pointer<ValueT>::value) {
            return bucket == _num_buckets ? nullptr : EMH_VAL(_pairs, bucket);
        } else {
            return bucket == _num_buckets ? std::numeric_limits<ValueT>::max() : EMH_VAL(_pairs, bucket);
        }
    }

    // update value or insert a new key-value to map
    void put(const KeyT& key, const N& nmspace, const ValueT& value) noexcept override
    {
        check_expand_need();

        bool isempty;
        const auto bucket = find_or_allocate(key, nmspace, isempty);
        if (isempty) {
            if constexpr (NeedClone<KeyT>::value) {
                if constexpr (std::is_same<KeyT, Object*>::value) {
                    Object* cloned = (key != nullptr) ? reinterpret_cast<Object*>(key)->clone() : nullptr;
                    EMH_NEW(cloned, std::move(ValueT()), nmspace, bucket, stateMapVersion);
                }

                if constexpr (std::is_same<KeyT, BinaryRowData*>::value || std::is_same<KeyT, RowData*>::value) {
                    BinaryRowData* cloned = (key != nullptr) ? reinterpret_cast<BinaryRowData*>(
                                                                   reinterpret_cast<BinaryRowData*>(key)->copy())
                                                             : nullptr;
                    EMH_NEW(cloned, std::move(ValueT()), nmspace, bucket, stateMapVersion);
                }
            } else {
                EMH_NEW(key, std::move(ValueT()), nmspace, bucket, stateMapVersion);
            }
        }
        if constexpr (std::is_same_v<ValueT, Object*>) {
            Object* oldValue = static_cast<Object*>(EMH_VAL(_pairs, bucket));
            Object* newValue = static_cast<Object*>(value);
            if (newValue != nullptr) {
                newValue->getRefCount();
            }
            EMH_VAL(_pairs, bucket) = value;
            if (oldValue != nullptr) {
                oldValue->putRefCount();
            }
        } else {
            EMH_VAL(_pairs, bucket) = value;
        }
    }

    void put(KeyT&& key, N&& nmspace, ValueT&& value) noexcept override
    {
        check_expand_need();

        bool isempty;
        const auto bucket = find_or_allocate(key, nmspace, isempty);
        if (isempty) {
            if constexpr (NeedClone<KeyT>::value) {
                if constexpr (std::is_same<KeyT, Object*>::value) {
                    Object* cloned = (key != nullptr) ? reinterpret_cast<Object*>(key)->clone() : nullptr;
                    EMH_NEW(cloned, std::move(ValueT()), std::move(nmspace), bucket, stateMapVersion);
                }

                if constexpr (std::is_same<KeyT, BinaryRowData*>::value || std::is_same<KeyT, RowData*>::value) {
                    BinaryRowData* cloned = (key != nullptr) ? reinterpret_cast<BinaryRowData*>(
                                                                   reinterpret_cast<BinaryRowData*>(key)->copy())
                                                             : nullptr;
                    EMH_NEW(cloned, std::move(ValueT()), std::move(nmspace), bucket, stateMapVersion);
                }
            } else {
                EMH_NEW(std::move(key), std::move(ValueT()), std::move(nmspace), bucket, stateMapVersion);
            }
        }
        if constexpr (std::is_same_v<ValueT, Object*>) {
            Object* oldValue = static_cast<Object*>(EMH_VAL(_pairs, bucket));
            Object* newValue = static_cast<Object*>(value);
            if (newValue != nullptr) {
                newValue->getRefCount();
            }
            EMH_VAL(_pairs, bucket) = std::move(value);
            if (oldValue != nullptr) {
                oldValue->putRefCount();
            }
        } else {
            EMH_VAL(_pairs, bucket) = std::move(value);
        }
    }
    ValueT putAndGetOld(const KeyT& key, const N& nameSpace, const ValueT& state) override
    {
        const auto bucket = find_filled_bucket(key, nameSpace);
        if (bucket == _num_buckets) {
            put(key, nameSpace, state);
            if constexpr (std::is_pointer_v<ValueT>) {
                return nullptr;
            } else {
                return {};
            }
        }
        ValueT oldValue = EMH_VAL(_pairs, bucket);
        // Use put() which handles refcounting properly
        put(key, nameSpace, state);
        return oldValue;
    }
    ValueT removeAndGetOld(const KeyT& key, const N& nameSpace) override
    {
        const auto bucket = find_filled_bucket(key, nameSpace);
        if (bucket == _num_buckets) {
            if constexpr (std::is_pointer_v<ValueT>) {
                return nullptr;
            } else {
                return {};
            }
        }
        // Save key and value before erase_key may move data around
        KeyT savedKey = EMH_KEY(_pairs, bucket);
        ValueT savedValue = EMH_VAL(_pairs, bucket);
        const auto erased = erase_key(key, nameSpace);
        clear_bucket(erased);
        // Clean up the key (caller takes ownership of the value, so no cleanup for it)
        if constexpr (std::is_same_v<KeyT, Object*>) {
            if (savedKey != nullptr) {
                static_cast<Object*>(savedKey)->putRefCount();
            }
        } else if constexpr (NeedClone<KeyT>::value && std::is_pointer_v<KeyT>) {
            delete savedKey;
        }
        return savedValue;
    }

    // If key exist, return it. If not, create one with default value
    std::pair<iterator, bool> do_insert(KeyT&& key, N&& nmspace, ValueT&& defaultV)
    {
        bool isempty;
        const auto bucket = find_or_allocate(key, nmspace, isempty);
        if (isempty) {
            EMH_NEW(
                std::forward<KeyT>(key),
                std::forward<ValueT>(defaultV),
                std::forward<N>(nmspace),
                bucket,
                stateMapVersion);
        }
        return {{this, bucket}, isempty};
    }

    void remove(const KeyT& key, const N& nmspace) override
    {
        const auto bucket = find_filled_bucket(key, nmspace);
        if (bucket == _num_buckets) {
            return;
        }
        // Save pointers for cleanup BEFORE erase_key moves data around
        KeyT savedKey = EMH_KEY(_pairs, bucket);
        ValueT savedValue = EMH_VAL(_pairs, bucket);
        const auto erased = erase_key(key, nmspace);
        clear_bucket(erased);
        // Now clean up the saved Object* refs after the entry is fully erased
        cleanupSavedRefs(savedKey, savedValue);
    }

    /// Erase an element from the hash table.
    /// return 0 if element was not found
    size_type erase(const KeyT& key, const N& nmspace)
    {
        const auto bucket = find_filled_bucket(key, nmspace);
        if (bucket == _num_buckets) return 0;
        // Save pointers for cleanup BEFORE erase_key moves data around
        KeyT savedKey = EMH_KEY(_pairs, bucket);
        ValueT savedValue = EMH_VAL(_pairs, bucket);
        const auto erased = erase_key(key, nmspace);
        if (erased == INACTIVE) return 0;

        clear_bucket(erased);
        // Now clean up the saved Object* refs after the entry is fully erased
        cleanupSavedRefs(savedKey, savedValue);
        return 1;
    }

    inline bool containsKey(const KeyT& key, const N& nmspace) const noexcept override
    {
        return find_filled_bucket(key, nmspace) != _num_buckets;
    }

    inline size_type count(const KeyT& key, const N& nmspace) const noexcept
    {
        return find_filled_bucket(key, nmspace) != _num_buckets ? 1u : 0u;
    }

    inline iterator find(const KeyT& key, const N& nmspace, size_t key_hash) noexcept
    {
        return {this, find_filled_hash(key, nmspace, key_hash)};
    }

    inline iterator find(const KeyT& key, const N& nmspace) noexcept
    {
        return {this, find_filled_bucket(key, nmspace)};
    }

    ~CopyOnWriteStateMap() noexcept
    {
        if (is_triviall_destructable() && _num_filled) {
            for (auto it = begin(); _num_filled; ++it) {
                _num_filled--;
                it->~value_pair();
            }
        } else {
            for (auto it = begin(); _num_filled; ++it) {
                _num_filled--;
                releaseEntry(it->first, it->second, it->third);
            }
        }
        free(_pairs);
        _pairs = nullptr;
    }

    iterator begin() noexcept
    {
#ifdef EMH_ZERO_MOVE
        if (0 == _num_filled) return {this, _num_buckets};
#endif

        const auto bmask = ~(*(size_t*)_bitmask);
        if (bmask != 0) return {this, (size_type)CTZ(bmask)};

        iterator it(this, sizeof(bmask) * 8 - 1);
        it.init();
        return it.next();
    }

    iterator last() const
    {
        if (_num_filled == 0) return end();

        auto bucket = _num_buckets - 1;
        while (EMH_EMPTY(bucket)) bucket--;
        return {this, bucket};
    }

    inline iterator end() noexcept
    {
        return {this, _num_buckets};
    }

    inline size_type bucket_count() const
    {
        return _num_buckets;
    }

private:
    // Used by constructor
    void init(size_type bucket, float mlf = EMH_DEFAULT_LOAD_FACTOR)
    {
        _pairs = nullptr;
        _bitmask = nullptr;
        _num_buckets = _num_filled = 0;
        _mlf = (uint32_t)((1 << 27) / EMH_DEFAULT_LOAD_FACTOR);
        max_load_factor(mlf);
        rehash(bucket);
    }

    static size_t AllocSize(uint64_t num_buckets)
    {
        return (num_buckets + EPACK_SIZE) * sizeof(PairT) + (num_buckets + 7) / 8 + BIT_PACK;
    }

    static PairT* alloc_bucket(size_type num_buckets)
    {
#ifdef EMH_ALLOC
        auto* new_pairs = (PairT*)aligned_alloc(EMH_MALIGN, AllocSize(num_buckets));
#else
        auto* new_pairs = (PairT*)malloc(AllocSize(num_buckets));
#endif
        return new_pairs;
    }

    inline void max_load_factor(float mlf)
    {
        if (mlf <= 0.999f && mlf > EMH_MIN_LOAD_FACTOR) _mlf = (uint32_t)((1 << 28) / mlf);
    }

    inline constexpr float max_load_factor() const
    {
        return (1 << 27) / (float)_mlf;
    }
    inline constexpr size_type max_size() const
    {
        return 1ull << (sizeof(size_type) * 8 - 1);
    }

    size_type bucket_main() const
    {
        auto main_size = 0;
        for (size_type bucket = 0; bucket < _num_buckets; ++bucket) {
            if (EMH_BUCKET(_pairs, bucket) == bucket) main_size++;
        }
        return main_size;
    }

    static constexpr bool is_triviall_destructable()
    {
#if __cplusplus >= 201402L || _MSC_VER > 1600
        return !(std::is_trivially_destructible<KeyT>::value && std::is_trivially_destructible<ValueT>::value);
#else
        return !(std::is_pod<KeyT>::value && std::is_pod<ValueT>::value);
#endif
    }

    static constexpr bool is_copy_trivially()
    {
#if __cplusplus >= 201402L || _MSC_VER > 1600
        return (std::is_trivially_copyable<KeyT>::value && std::is_trivially_copyable<ValueT>::value);
#else
        return (std::is_pod<KeyT>::value && std::is_pod<ValueT>::value);
#endif
    }
    size_type erase_key(const KeyT& key, const N& nmspace)
    {
        const auto bucket = hash_key(key, nmspace) & _mask;
        if (EMH_EMPTY(bucket)) return INACTIVE;

        auto next_bucket = EMH_BUCKET(_pairs, bucket);
        const auto eqkey = _eq(key, EMH_KEY(_pairs, bucket), nmspace, EMH_NMSPACE(_pairs, bucket));
        if (eqkey) {
            if (next_bucket == bucket) return bucket;

            const auto nbucket = EMH_BUCKET(_pairs, next_bucket);
            if (is_copy_trivially())
                EMH_PKV(_pairs, bucket) = EMH_PKV(_pairs, next_bucket);
            else
                EMH_PKV(_pairs, bucket).swap(EMH_PKV(_pairs, next_bucket));

            EMH_BUCKET(_pairs, bucket) = (nbucket == next_bucket) ? bucket : nbucket;
            return next_bucket;
        } else if (next_bucket == bucket) {
            return INACTIVE;
        }

        auto prev_bucket = bucket;
        while (true) {
            const auto nbucket = EMH_BUCKET(_pairs, next_bucket);
            if (_eq(key, EMH_KEY(_pairs, next_bucket), nmspace, EMH_NMSPACE(_pairs, next_bucket))) {
                EMH_BUCKET(_pairs, prev_bucket) = (nbucket == next_bucket) ? prev_bucket : nbucket;
                return next_bucket;
            }

            if (nbucket == next_bucket) {
                break;
            }
            prev_bucket = next_bucket;
            next_bucket = nbucket;
        }

        return INACTIVE;
    }
    void clearkv()
    {
        if (is_triviall_destructable()) {
            auto it = begin();
            it.init();
            for (; _num_filled; ++it) clear_bucket(it.bucket());
        }
    }

    /// Remove all elements, keeping full capacity.
    void clear()
    {
        // The map owns its pointer entries -- cloned keys, refcounted Object*, inner HashMaps --
        // and neither branch below releases them: the memset only wipes the occupancy bitmask and
        // ~PairT() on a pointer member does nothing. This is also the last chance to free them,
        // because _num_filled is zeroed at the end, which stops the destructor from ever walking
        // these entries.
        if constexpr (std::is_pointer_v<KeyT> || std::is_pointer_v<ValueT> || std::is_pointer_v<N>) {
            auto it = begin();
            for (size_type n = _num_filled; n; --n, ++it) {
                releaseEntry(it->first, it->second, it->third);
            }
        }

        if (!is_triviall_destructable() && _num_filled) {
            memset_s(_bitmask, (_num_buckets + 7) / 8, (int)0xFFFFFFFF, (_num_buckets + 7) / 8);
            if (_num_buckets < 8) _bitmask[0] = uint8_t((1 << _num_buckets) - 1);
        } else if (_num_filled) {
            clearkv();
        }

        // EMH_BUCKET(_pairs, _num_buckets) = 0; //_last
        _num_filled = 0;
    }

    void shrink_to_fit()
    {
        rehash(_num_filled + 1);
    }

    /// Make room for this many elements
    bool reserve(uint64_t num_elems)
    {
        const auto required_buckets = (num_elems * _mlf >> 28);
        if (EMH_LIKELY(required_buckets < _num_buckets)) return false;

#if EMH_HIGH_LOAD
        if (required_buckets < 64 && _num_filled < _num_buckets) return false;
#endif

#if EMH_STATIS
        if (_num_filled > EMH_STATIS) dump_statics(true);
#endif
        rehash(required_buckets + 2);
        return true;
    }

    void rehash(uint64_t required_buckets)
    {
        if (required_buckets < _num_filled) return;

        uint64_t buckets = _num_filled > (1u << 16) ? (1u << 16) : 2u;
        while (buckets < required_buckets) {
            buckets *= 2;
        }

        if (buckets >= max_size() || buckets <= _num_filled) {
            throw std::runtime_error("CopyOnWriteStateMap too big");
        }

        auto num_buckets = (size_type)buckets;
        auto old_num_filled = _num_filled;
        auto old_mask = _num_buckets - 1;
        auto old_pairs = _pairs;
        auto* obmask = _bitmask;

        _num_filled = 0;
        _num_buckets = num_buckets;
        _mask = num_buckets - 1;

        _pairs = alloc_bucket(_num_buckets);
        memset_s((char*)(_pairs + _num_buckets), sizeof(PairT) * EPACK_SIZE, 0, sizeof(PairT) * EPACK_SIZE);

        _bitmask = decltype(_bitmask)(_pairs + EPACK_SIZE + num_buckets);

        const auto mask_byte = (num_buckets + 7) / 8;
        memset_s(_bitmask, mask_byte, (int)0xFFFFFFFF, mask_byte);
        memset_s(((char*)_bitmask) + mask_byte, BIT_PACK, 0, BIT_PACK);
        if (num_buckets < 8) _bitmask[0] = (uint8_t)((1 << num_buckets) - 1);

        for (size_type src_bucket = old_mask; _num_filled < old_num_filled; src_bucket--) {
            if (obmask[src_bucket / MASK_BIT] & (EMH_MASK(src_bucket))) continue;

            auto& key = EMH_KEY(old_pairs, src_bucket);
            auto& nmspace = EMH_NMSPACE(old_pairs, src_bucket);
            const auto bucket = find_unique_bucket(key, nmspace);
            EMH_NEW(
                std::move(key),
                std::move(EMH_VAL(old_pairs, src_bucket)),
                std::move(EMH_NMSPACE(old_pairs, src_bucket)),
                bucket,
                stateMapVersion);
            if (is_triviall_destructable()) old_pairs[src_bucket].~PairT();
        }

#if EMH_REHASH_LOG
        if (_num_filled > EMH_REHASH_LOG) {
            auto mbucket = bucket_main();
            char buff[255] = {0};
            sprintf_s(
                buff,
                "    _num_filled/collision/main/K.V/pack/ = %u/%.2lf%%(%.2lf%%)/%s.%s/%zd",
                _num_filled,
                200.0f * (_num_filled - mbucket) / _mask,
                100.0f * mbucket / _mask,
                typeid(KeyT).name(),
                typeid(ValueT).name(),
                sizeof(_pairs[0]));
#ifdef EMH_LOG
            static size_t ihashs = 0;
            EMH_LOG << "rhash_nums = " << ihashs++ << "|" << __FUNCTION__ << "|" << buff << endl;
#else
            puts(buff);
#endif
        }
#endif

        free(old_pairs);
        if (old_num_filled != _num_filled) {
            throw std::runtime_error("CopyOnWriteStateMap rehash failed");
        }
    }

private:
    // Can we fit another element?
    inline bool check_expand_need()
    {
        return reserve(_num_filled);
    }

    void clear_bucket(size_type bucket)
    {
        EMH_CLS(bucket);
        _num_filled--;
        if (is_triviall_destructable()) _pairs[bucket].~PairT();
    }

    /**
     * Release everything one entry owns: refcounted Object*, cloned pointer keys, inner HashMaps.
     * Shared by clear() and the destructor so both free an entry exactly the same way. Non-pointer
     * key/value/namespace types own nothing and fall through.
     *
     * The entry must already be unreachable from the table when this is called.
     */
    void releaseEntry(const KeyT& key, const ValueT& value, const N& nmspace)
    {
        if constexpr (std::is_same_v<KeyT, Object*>) {
            // put() stores a null clone for a null key, so the guard is load-bearing.
            if (key != nullptr) {
                static_cast<Object*>(key)->putRefCount();
            }
        } else if constexpr (std::is_pointer_v<KeyT>) {
            delete key;
        }
        if constexpr (std::is_same_v<ValueT, Object*>) {
            if (value != nullptr) {
                static_cast<Object*>(value)->putRefCount();
            }
        } else if constexpr (cowstatemap_detail::IsEmhashMapPtrType<ValueT>::value) {
            // For inner HashMap values: clean up Object* entries before deleting
            cowstatemap_detail::cleanupInnerMapAndDelete(value);
        } else if constexpr (std::is_pointer_v<ValueT>) {
            delete value;
        }
        if constexpr (std::is_pointer_v<N>) {
            delete nmspace;
        }
    }

    /**
     * Clean up saved key/value refs AFTER the entry has been erased from the table.
     * This handles refcounting for Object* and ownership for cloned pointer keys.
     * For inner HashMap values (e.g. emhash7::HashMap<Object*, Object*>*), only
     * deletes the HashMap structure — inner entry refcounting is HeapMapState's
     * responsibility (its clear()/remove() already handle that before calling
     * stateTable->remove()). cleanupInnerMapAndDelete is only used in the
     * destructor for final shutdown cleanup.
     */
    void cleanupSavedRefs(const KeyT& savedKey, const ValueT& savedValue)
    {
        if constexpr (std::is_same_v<KeyT, Object*>) {
            if (savedKey != nullptr) {
                static_cast<Object*>(savedKey)->putRefCount();
            }
        } else if constexpr (NeedClone<KeyT>::value && std::is_pointer_v<KeyT>) {
            delete savedKey;
        }
        if constexpr (std::is_same_v<ValueT, Object*>) {
            if (savedValue != nullptr) {
                static_cast<Object*>(savedValue)->putRefCount();
            }
        } else if constexpr (cowstatemap_detail::IsRowListMapPtrType<ValueT>::value) {
            cowstatemap_detail::cleanupInnerMapAndDelete(savedValue);
        } else if constexpr (std::is_pointer_v<ValueT>) {
            delete savedValue;
        }
    }

    // Find the bucket with this key, or return bucket size
    size_type find_filled_hash(const KeyT& key, const N& nmspace, const size_t key_hash) const
    {
        const auto bucket = key_hash & _mask;
        if (EMH_EMPTY(bucket)) return _num_buckets;

        auto next_bucket = bucket;
        while (true) {
            if (_eq(key, EMH_KEY(_pairs, next_bucket), nmspace, EMH_NMSPACE(_pairs, next_bucket))) return next_bucket;

            const auto nbucket = EMH_BUCKET(_pairs, next_bucket);
            if (nbucket == next_bucket) break;
            next_bucket = nbucket;
        }

        return _num_buckets;
    }

    // Find the bucket with this key, or return bucket size
    size_type find_filled_bucket(const KeyT& key, const N& nmspace) const
    {
        const auto bucket = hash_key(key, nmspace) & _mask;
        if (EMH_EMPTY(bucket)) return _num_buckets;

        auto next_bucket = bucket;

        while (true) {
            if (_eq(key, EMH_KEY(_pairs, next_bucket), nmspace, EMH_NMSPACE(_pairs, next_bucket))) return next_bucket;

            const auto nbucket = EMH_BUCKET(_pairs, next_bucket);
            if (nbucket == next_bucket) return _num_buckets;
            next_bucket = nbucket;
        }

        return 0;
    }

    // kick out bucket and find empty to occpuy
    // it will break the orgin link and relnik again.
    // before: main_bucket-->prev_bucket --> bucket   --> next_bucket
    // atfer : main_bucket-->prev_bucket --> (removed)--> new_bucket--> next_bucket
    size_type kickout_bucket(const size_type kmain, const size_type kbucket)
    {
        const auto next_bucket = EMH_BUCKET(_pairs, kbucket);
        const auto new_bucket = find_empty_bucket(next_bucket, kbucket);
        const auto prev_bucket = find_prev_bucket(kmain, kbucket);
        new (_pairs + new_bucket) PairT(std::move(_pairs[kbucket]));
        if (is_triviall_destructable()) _pairs[kbucket].~PairT();

        if (next_bucket == kbucket) EMH_BUCKET(_pairs, new_bucket) = new_bucket;
        EMH_BUCKET(_pairs, prev_bucket) = new_bucket;

        EMH_SET(new_bucket);
        return kbucket;
    }

    /*
    ** inserts a new key into a hash table; first check whether key's main
    ** bucket/position is free. If not, check whether colliding node/bucket is in its main
    ** position or not: if it is not, move colliding bucket to an empty place and
    ** put new key in its main position; otherwise (colliding bucket is in its main
    ** position), new key goes to an empty position. ***/

    size_type find_or_allocate(const KeyT& key, const N& nmspace, bool& isempty)
    {
        const auto bucket = hash_key(key, nmspace) & _mask;
        const auto& bucket_key = EMH_KEY(_pairs, bucket);
        const auto& bucket_nmspace = EMH_NMSPACE(_pairs, bucket);

        if (EMH_EMPTY(bucket)) {
            isempty = true;
            return bucket;
        } else if (_eq(key, bucket_key, nmspace, bucket_nmspace)) {
            isempty = false;
            return bucket;
        }

        isempty = true;
        auto next_bucket = EMH_BUCKET(_pairs, bucket);
        // check current bucket_key is in main bucket or not
        const auto kmain_bucket = hash_key(bucket_key, bucket_nmspace) & _mask;
        if (kmain_bucket != bucket)
            return kickout_bucket(kmain_bucket, bucket);
        else if (next_bucket == bucket)
            return EMH_BUCKET(_pairs, next_bucket) = find_empty_bucket(next_bucket, bucket);

#if EMH_LRU_SET
        auto prev_bucket = bucket;
#endif
        // find next linked bucket and check key, if lru is set then swap current key with prev_bucket
        while (true) {
            if (EMH_UNLIKELY(_eq(key, EMH_KEY(_pairs, next_bucket), nmspace, EMH_NMSPACE(_pairs, next_bucket)))) {
                isempty = false;
#if EMH_LRU_SET
                EMH_PKV(_pairs, next_bucket).swap(EMH_PKV(_pairs, prev_bucket));
                return prev_bucket;
#else
                return next_bucket;
#endif
            }

#if EMH_LRU_SET
            prev_bucket = next_bucket;
#endif

            const auto nbucket = EMH_BUCKET(_pairs, next_bucket);
            if (nbucket == next_bucket) break;
            next_bucket = nbucket;
        }

        const auto new_bucket = find_empty_bucket(next_bucket, bucket); // : find_empty_bucket(next_bucket);
        return EMH_BUCKET(_pairs, next_bucket) = new_bucket;
    }

    // key is not in this map. Find a place to put it.
    size_type find_empty_bucket(const size_type bucket_from, const size_type main_bucket)
    {
#if EMH_ITER_SAFE
        const auto boset = bucket_from % 8;
        auto* const align = (uint8_t*)_bitmask + bucket_from / 8;
        (void)main_bucket;
        size_t bmask;
        memcpy_s(&bmask, align + 0, sizeof(bmask));
        bmask >>= boset; // bmask |= ((size_t)align[8] << (SIZE_BIT - boset));
        if (EMH_LIKELY(bmask != 0)) return bucket_from + CTZ(bmask);
#else
        const auto boset = bucket_from % 8;
        auto* const align = (uint8_t*)_bitmask + bucket_from / 8;
        (void)bucket_from;
        const size_t bmask = (*(size_t*)(align) >> boset); // & 0xF0F0F0F0FF0FF0FFull;//
        if (EMH_LIKELY(bmask != 0)) return bucket_from + CTZ(bmask);
#endif

        const auto qmask = _mask / SIZE_BIT;

        // auto next_bucket = (bucket_from + 0 * SIZE_BIT) & qmask;
        auto& last = EMH_BUCKET(_pairs, _num_buckets);
        for (;;) {
            last &= qmask;
            const auto bmask2 = *((size_t*)_bitmask + last);
            if (bmask2 != 0) return last * SIZE_BIT + CTZ(bmask2);
#if 1
            const auto next1 = (qmask / 2 + last) & qmask;
            const auto bmask1 = *((size_t*)_bitmask + next1);
            if (bmask1 != 0) {
                last = next1;
                return next1 * SIZE_BIT + CTZ(bmask1);
            }
            last += 1;
#endif
        }
    }

    size_type find_last_bucket(size_type main_bucket) const
    {
        auto next_bucket = EMH_BUCKET(_pairs, main_bucket);
        if (next_bucket == main_bucket) return main_bucket;

        while (true) {
            const auto nbucket = EMH_BUCKET(_pairs, next_bucket);
            if (nbucket == next_bucket) return next_bucket;
            next_bucket = nbucket;
        }
    }

    size_type find_prev_bucket(size_type main_bucket, const size_type bucket) const
    {
        auto next_bucket = EMH_BUCKET(_pairs, main_bucket);
        if (next_bucket == bucket) return main_bucket;

        while (true) {
            const auto nbucket = EMH_BUCKET(_pairs, next_bucket);
            if (nbucket == bucket) return next_bucket;
            next_bucket = nbucket;
        }
    }

    size_type find_unique_bucket(const KeyT& key, const N& nmspace)
    {
        const size_type bucket = hash_key(key, nmspace) & _mask;
        if (EMH_EMPTY(bucket)) return bucket;

        // check current bucket_key is in main bucket or not
        const auto kmain_bucket = hash_key(EMH_KEY(_pairs, bucket), EMH_NMSPACE(_pairs, bucket)) & _mask;
        if (EMH_UNLIKELY(kmain_bucket != bucket)) return kickout_bucket(kmain_bucket, bucket);

        auto next_bucket = EMH_BUCKET(_pairs, bucket);
        if (next_bucket != bucket) next_bucket = find_last_bucket(next_bucket);

        // find a new empty and link it to tail
        return EMH_BUCKET(_pairs, next_bucket) = find_empty_bucket(next_bucket, bucket);
    }

    // todo: Don't use EMH_INT_HASH and EMH_IDENTITY_HASH now
    template <typename UType, typename std::enable_if<std::is_integral<UType>::value, size_type>::type = 0>
    inline size_type hash_key(const UType key, const N nmspace) const
    {
#if EMH_INT_HASH
        return hash64(key);
#elif EMH_IDENTITY_HASH
        return key + (key >> 24);
#else
        return (size_type)_hasher(key, nmspace);
#endif
    }
    // todo: Don't use EMH_WY_HASH now
    template <typename UType, typename std::enable_if<std::is_same<UType, std::string>::value, size_type>::type = 0>
    inline size_type hash_key(const UType& key, const N nmspace) const
    {
#if EMH_WY_HASH
        return wyhash(key.data(), key.size(), 0);
#else
        return (size_type)_hasher(key, nmspace);
#endif
    }

    template <
        typename UType,
        typename std::
            enable_if<!std::is_integral<UType>::value && !std::is_same<UType, std::string>::value, size_type>::type = 0>
    inline size_type hash_key(const UType& key, const N& nmspace) const
    {
        return (size_type)_hasher(key, nmspace);
    }

private:
    uint8_t* _bitmask;
    PairT* _pairs;
    HashT _hasher;
    EqT _eq;
    size_type _mask;
    size_type _num_buckets;

    size_type _num_filled;
    uint32_t _mlf;

private:
    int stateMapVersion = 0;
    TypeSerializer* stateSerializer;

private:
    static constexpr uint32_t BIT_PACK = sizeof(uint64_t);
    static constexpr uint32_t MASK_BIT = sizeof(_bitmask[0]) * 8;
    static constexpr uint32_t SIZE_BIT = sizeof(size_t) * 8;
    static constexpr uint32_t EPACK_SIZE = sizeof(PairT) >= sizeof(size_t) == 0 ? 1 : 2; // > 1
};
} // namespace omnistream
// namespace emhash7
#if __cplusplus >= 201103L
#endif

#endif
