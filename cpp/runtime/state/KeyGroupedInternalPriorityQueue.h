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

#include <unordered_set>
#include <cstdint>

#include "InternalPriorityQueue.h"
#include "core/utils/type_traits_ext.h"

template <typename T>
class KeyGroupedInternalPriorityQueue : virtual public InternalPriorityQueue<T> {
public:
    static_assert(is_shared_ptr_v<T>, "T should be shared ptr.");
    using InnerType = typename T::element_type;
    using DedupSet = std::unordered_set<T, typename InnerType::SharedPtrHash, typename InnerType::SharedPtrEqual>;
    virtual std::shared_ptr<DedupSet> getSubsetForKeyGroup(int32_t keyGroupId) = 0;
};
