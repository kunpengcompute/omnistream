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

#include <cstddef>
#include <functional>
#include <list>
#include <utility>

#include <emhash7.hpp>

/*
 * 按权重控制容量的 LRU cache。maxWeight 为 0 时禁用缓存；get 命中会刷新最近使用顺序。
 * put 支持 copyable 和 move-only value，成功时返回缓存内 value 指针；超重或禁用时返回 nullptr。
 * 拒绝新 key 时不插入且不影响其它 entry；拒绝已有 key 时删除旧 entry，避免读到过期值；拒绝路径
 * 不会 consume 调用方传入的 move-only value。返回指针只在对应 entry 留在 cache 且 cache 未通过
 * put/erase/clear 等后续修改影响该 entry 时有效。
 */
template <typename K, typename V, typename KHash = std::hash<K>, typename KEqual = std::equal_to<K>>
class WeightedLruCache {
public:
    explicit WeightedLruCache(size_t maxWeight) : maxWeight_(maxWeight)
    {
    }

    WeightedLruCache(const WeightedLruCache&) = delete;
    WeightedLruCache& operator=(const WeightedLruCache&) = delete;

    V* get(const K& key)
    {
        auto mapIt = entries_.find(key);
        if (mapIt == entries_.end()) {
            return nullptr;
        }

        promote(mapIt->second);
        return &mapIt->second->value;
    }

    template <typename Value>
    V* put(const K& key, Value&& value, size_t weight)
    {
        if (maxWeight_ == 0 || weight > maxWeight_) {
            erase(key);
            return nullptr;
        }

        auto mapIt = entries_.find(key);
        if (mapIt != entries_.end()) {
            auto nodeIt = mapIt->second;
            nodeIt->value = std::forward<Value>(value);
            currentWeight_ -= nodeIt->weight;
            nodeIt->weight = weight;
            currentWeight_ += weight;
            promote(nodeIt);
            evictIfNeeded();
            return &nodeIt->value;
        }

        nodes_.push_front(Node{key, std::forward<Value>(value), weight});
        entries_[nodes_.front().key] = nodes_.begin();
        currentWeight_ += weight;
        evictIfNeeded();
        return &nodes_.front().value;
    }

    bool erase(const K& key)
    {
        auto mapIt = entries_.find(key);
        if (mapIt == entries_.end()) {
            return false;
        }

        currentWeight_ -= mapIt->second->weight;
        nodes_.erase(mapIt->second);
        entries_.erase(mapIt);
        return true;
    }

    void clear()
    {
        nodes_.clear();
        entries_.clear();
        currentWeight_ = 0;
    }

    size_t currentWeight() const
    {
        return currentWeight_;
    }

    size_t maxWeight() const
    {
        return maxWeight_;
    }

    size_t size() const
    {
        return entries_.size();
    }

private:
    struct Node {
        K key;
        V value;
        size_t weight;
    };

    using ListIterator = typename std::list<Node>::iterator;

    void promote(ListIterator nodeIt)
    {
        nodes_.splice(nodes_.begin(), nodes_, nodeIt);
    }

    void evictIfNeeded()
    {
        while (currentWeight_ > maxWeight_ && !nodes_.empty()) {
            auto lruIt = --nodes_.end();
            currentWeight_ -= lruIt->weight;
            entries_.erase(lruIt->key);
            nodes_.pop_back();
        }
    }

    size_t maxWeight_;
    size_t currentWeight_ = 0;
    std::list<Node> nodes_;
    emhash7::HashMap<K, ListIterator, KHash, KEqual> entries_;
};
