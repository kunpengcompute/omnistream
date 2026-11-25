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

#include <thread>
#include "core/utils/key_type_traits.h"
#include "../../table/data/RowData.h"
#include "../../core/utils/MathUtils.h"
#include "data/binary/BinaryRowData.h"

template <typename K>
class KeyGroupRangeAssignment {
public:
    // Prevent instantiation
    KeyGroupRangeAssignment() = delete;

    static inline int assignToKeyGroup(const K& key, int maxParallelism)
    {
        if constexpr (KeyTypeTraits<K>::isRowKey || KeyTypeTraits<K>::isSharedRowKey) {
            // std::hash<RowData*> uses a simpler hasher. But here we need to keep it same as java
            int hashCode = key ? key->hashCode() : 0;
            return MathUtils::murmurHash(hashCode) % maxParallelism;
        }
        if constexpr (std::is_same_v<K, Object*>) {
            int hashCode = key ? ((Object*)key)->hashCode() : 0;
            int res = MathUtils::murmurHash(hashCode) % maxParallelism;
            return res;
        }
        int hashCode = std::hash<K>{}(key);
        LOG("hashkey hashCode -> " << hashCode);
        return MathUtils::murmurHash(hashCode) % maxParallelism;
    }

    static inline int computeOperatorIndexForKeyGroup(int maxParallelism, int parallelism, int keyGroupId)
    {
        return keyGroupId * parallelism / maxParallelism;
    }

    static inline int assignKeyToParallelOperator(const K& key, int maxParallelism, int parallelism)
    {
        int keyGroup = assignToKeyGroup(key, maxParallelism);
        auto result = computeOperatorIndexForKeyGroup(maxParallelism, parallelism, keyGroup);
        return result;
    }
};
