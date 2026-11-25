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

#ifndef FLINK_TNEL_TTLSTATEFACTORY_H
#define FLINK_TNEL_TTLSTATEFACTORY_H
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "runtime/state/KeyedStateBackend.h"

/**
 * K: The type of the keys.
 * N: The data type that the serializer serializes.
 * S: The type of the State objects created from this {@code StateDescriptor}.
 * V: The type of the value of the state object described by this state descriptor.
 * */
template <typename K, typename N, typename S, typename V>
class TtlStateFactory {
public:
    // Function def does not match java, simplified to be passed to next class
    // stateBackend input should be KeyedStateBackend instead
    static S* createStateAndWrapWithTtlIfEnabled(
        TypeSerializer* namespaceSerialzer, StateDescriptor* stateDesc, KeyedStateBackend<K>* stateBackend)
    {
        return (S*)stateBackend->createOrUpdateInternalState(namespaceSerialzer, stateDesc);
    };
};

#endif // FLINK_TNEL_TTLSTATEFACTORY_H
