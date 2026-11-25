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

#ifndef OMNISTREAM_VALUESTATECACHE_H
#define OMNISTREAM_VALUESTATECACHE_H

/**
 * Falcon hot state cache interface, which provide API for upper-level flink app to access falcon cache. The
 * FalconKey-FalconValue state can be read, added, updated and retrieved from falcon cache. Only when falcon
 * cache misses, falcon cache will then access RocksDB.
 *
 * <p> Besides, we also expose some API for falcon cache to flush all dirty Key-Value states to RocksDB or flush the
 * coldest state to RocksDB and then remove it from falcon cache.
 *
 * <p> Each falcon cache has its own size limit, which can be got or set by the exposed API.
 *
 * <p> In Flink, each columnFamily will instantiate a State. In our Falcon system, each State hold a
 * falcon cache, which only store states under this columnFamily. Thus, input parameters does not contain columnFamily.
 *
 * @param [K] Type of ValueState's key
 * @param [N] Type of ValueState's namespace
 * @param [V] Type of ValueState's value
 */
template <typename K, typename N, typename V>
class ValueStateCache {
public:
    /**
     * Returns the current value of the given key and namespace (i.e., a FalconKey)
     * <p> Case1: if FalconKey has already been in falcon cache, directly get value from falcon cache;
     * <p> Case2: if falcon cache miss, get value from RocksDB and insert (value, isDirty=false) into cache;
     * <p> If we successfully get FalconValue, mark the state as hot state (done by LinkedHashMap).
     *
     * @param key key of the state to be read
     * @param namespace namespace of the state to be read
     * @return value corresponding to the given key and namespace
     */
    virtual V get(const K& key, const N& ns) = 0;

    /**
     * Write the given key-value pair into falcon cache, compose key and namespace as FalconKey, compose value and
     * isDirty flag as FalconValue.
     * <p> Case1: if FalconKey has already been in falcon cache, directly update FalconValue in cache and mark as dirty
     * <p> Case2: if falcon cache miss, write key-value pair to RocksDB, then insert FalconKey-FalconValue pair into
     * cache and mark as clean
     * <p> After the state has been written, mark the state as hot state.
     *
     * @param key key of the state to be written
     * @param namespace namespace of the state to be written
     * @param value value of the state to be written
     */
    virtual void put(const K& key, const N& ns, const V& value) = 0;

    /**
     * Remove the given key and namespace (FalconKey) from falcon cache and RocksDB
     *
     * @param key key of the state to be removed
     * @param namespace namespace of the state to be removed
     */
    virtual void remove(const K& key, const N& ns) = 0;

    /**
     * Flush all the dirty state from falcon cache to RocksDB.
     */
    virtual void flush() = 0;

    /**
     * Clear falcon cache and free object memory.
     */
    virtual void clearAll() = 0;

    /**
     * When cache size reach upper size limit, flush the coldest state to RocksDB if needed and then remove it from
     * falcon cache. Note that we defensively check whether cache size exceeds upper size limit a lot, if so, flush and
     * clear falcon cache.
     */
    virtual void removeEldestState() = 0;

    /**
     * Get cache size limit.
     *
     * @return cache size limit
     */
    [[nodiscard]] virtual int getSizeLimit() const = 0;

    /**
     * Update cache size limit.
     *
     * @param newSizeLimit the new size limit of the cache.
     */
    virtual void updateSizeLimit(int newSizeLimit) = 0;

    virtual ~ValueStateCache() = default;
};

#endif // OMNISTREAM_VALUESTATECACHE_H
