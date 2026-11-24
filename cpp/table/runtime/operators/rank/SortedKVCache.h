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

#ifndef OMNISTREAM_KVCACHE_H
#define OMNISTREAM_KVCACHE_H
#include <iostream>
#include <unordered_map>
#include <list>
#include <memory>

template <typename K, typename V>
class SortedKVCache {
public:
    explicit SortedKVCache() = default;
    explicit SortedKVCache(size_t cap) : capacity(cap)
    {
    }
    ~SortedKVCache()
    {
        for (auto& item : cacheList) {
            if constexpr (std::is_pointer<K>::value) {
                delete item.first;
            }
        }
    }

    // Tell the cache whether it is the sole owner of stored V pointers.
    // Only safe to set true when the caller guarantees no one else references
    // the pointers (e.g. rocksdb backend which stores a byte-copy, not the ptr).
    void setOwnsValues(bool owns)
    {
        ownsValues = owns;
    }

    V get(const K& key)
    {
        if (cacheMap.find(key) == cacheMap.end()) {
            return nullptr;
        }

        cacheList.splice(cacheList.begin(), cacheList, cacheMap[key]);
        return cacheMap[key]->second;
    }

    bool hasKey(const K& key)
    {
        return cacheMap.find(key) != cacheMap.end();
    }

    void put(const K& key, const V& value)
    {
        if (cacheMap.find(key) != cacheMap.end()) {
            cacheList.splice(cacheList.begin(), cacheList, cacheMap[key]);
            if constexpr (std::is_pointer_v<V>) {
                oldValues.push_back(cacheMap[key]->second);
            }
            cacheMap[key]->second = value;
            if constexpr (std::is_pointer<K>::value) {
                delete key;
            }
        } else {
            if (cacheList.size() == capacity) {
                K lruKey = cacheList.back().first;
                V lruVal = cacheList.back().second;
                cacheMap.erase(lruKey);
                cacheList.pop_back();
                if constexpr (std::is_pointer_v<K>) {
                    delete lruKey;
                }
                // Only free the evicted value when the cache is its sole owner.
                // For the heap state backend, the state still holds the same ptr.
                if (ownsValues) {
                    if constexpr (std::is_pointer<V>::value) {
                        delete lruVal;
                    }
                }
            }
            // Insert new element at front
            cacheList.push_front({key, value});
            cacheMap[key] = cacheList.begin();
        }
    }

    void clearOldValues()
    {
        for (auto& value : oldValues) {
            if constexpr (std::is_pointer<V>::value) {
                delete value;
            }
        }
        oldValues.clear();
    }

private:
    size_t capacity = 1024;
    bool ownsValues = false;              // if true, cache is sole owner of V pointers and frees them on eviction
    std::list<std::pair<K, V>> cacheList; // Stores key-value pairs
    std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> cacheMap;
    std::vector<V> oldValues;
};

#endif
