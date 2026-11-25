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

#include <list>
#include <optional>
#include <emhash7.hpp>

template <typename K, typename V, typename KHash = std::hash<K>, typename KEqual = std::equal_to<K>>
class LRUMap {
public:
    LRUMap()
    {
        cacheMap_.reserve(capacity_);
    };

    explicit LRUMap(size_t cap) : capacity_(cap)
    {
        if (capacity_ > 0) {
            cacheMap_.reserve(capacity_);
        }
    }

    ~LRUMap()
    {
        if constexpr (std::is_pointer_v<V>) {
            for (auto it = cacheMap_.begin(); it != cacheMap_.end(); ++it) {
                delete it->second.second;
            }
        }
    }

    std::optional<V> get(const K& key)
    {
        auto mapIt = cacheMap_.find(key);
        if (mapIt == cacheMap_.end()) {
            return std::nullopt;
        }
        cacheList_.splice(cacheList_.begin(), cacheList_, mapIt->second.first);
        return mapIt->second.second;
    }

    void put(const K& key, const V& value)
    {
        auto mapIt = cacheMap_.find(key);
        if (mapIt != cacheMap_.end()) {
            cacheList_.splice(cacheList_.begin(), cacheList_, mapIt->second.first);
            mapIt->second.second = value;
        } else {
            if (cacheList_.size() >= capacity_) {
                // delete least recently used element in the end
                auto& lruKey = cacheList_.back();
                auto lruMapIt = cacheMap_.find(lruKey);
                if (lruMapIt != cacheMap_.end()) {
                    if constexpr (std::is_pointer_v<V>) {
                        delete lruMapIt->second.second;
                    }
                    cacheMap_.erase(lruMapIt);
                } else {
                    THROW_RUNTIME_ERROR("LRU key not found in cache map");
                }
                cacheList_.pop_back();
            }
            // insert new element at front
            cacheList_.push_front(key);
            cacheMap_[key] = {cacheList_.begin(), value};
        }
    }

private:
    size_t capacity_ = 1024;
    std::list<K> cacheList_;
    emhash7::HashMap<K, std::pair<typename std::list<K>::iterator, V>, KHash, KEqual> cacheMap_;
};
