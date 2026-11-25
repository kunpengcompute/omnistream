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

#ifndef OMNISTREAM_OPERATORSTATESTORE_H
#define OMNISTREAM_OPERATORSTATESTORE_H

#include <unordered_set>
#include <set>
#include <string>
#include <stdexcept>

#include "core/include/common.h"

#include "BroadcastState.h"
#include "ListState.h"
#include "MapStateDescriptor.h"
#include "ListStateDescriptor.h"

class OperatorStateStore {
public:
    virtual ~OperatorStateStore() = default;

    template <typename K, typename V>
    std::shared_ptr<BroadcastState<K, V>> getBroadcastState(MapStateDescriptor<K, V>* stateDescriptor)
    {
        /**
         * template functions cannot be virtual.
         * but subclasses can override it by defining the same signature.
         * if subclass does not override it, an exception will be thrown.
         */
        NOT_IMPL_EXCEPTION;
    }

    template <typename S>
    std::shared_ptr<ListState<S>> getListState(ListStateDescriptor<S>* stateDescriptor)
    {
        /**
         * template functions cannot be virtual.
         * but subclasses can override it by defining the same signature.
         * if subclass does not override it, an exception will be thrown.
         */
        NOT_IMPL_EXCEPTION;
    }

    template <typename S>
    std::shared_ptr<ListState<S>> getUnionListState(ListStateDescriptor<S>* stateDescriptor)
    {
        /**
         * template functions cannot be virtual.
         * but subclasses can override it by defining the same signature.
         * if subclass does not override it, an exception will be thrown.
         */
        NOT_IMPL_EXCEPTION;
    }

    virtual std::unordered_set<std::string> getRegisteredStateNames() = 0;

    virtual std::unordered_set<std::string> getRegisteredBroadcastStateNames() = 0;
};

#endif // OMNISTREAM_OPERATORSTATESTORE_H
