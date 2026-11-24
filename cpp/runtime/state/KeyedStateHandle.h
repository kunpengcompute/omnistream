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

#ifndef OMNISTREAM_KEYEDSTATEHANDLE_H
#define OMNISTREAM_KEYEDSTATEHANDLE_H
#include <memory>
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/CompositeStateHandle.h"
#include "runtime/state/StateHandleID.h"
#include <memory>

class KeyedStateHandle : public CompositeStateHandle {
public:
    virtual KeyGroupRange GetKeyGroupRange() const = 0;
    virtual std::shared_ptr<KeyedStateHandle> GetIntersection(const KeyGroupRange& keyGroupRange) const = 0;
    virtual StateHandleID GetStateHandleId() const = 0;
    virtual std::string ToString() const = 0;
};
#endif // OMNISTREAM_KEYEDSTATEHANDLE_H
