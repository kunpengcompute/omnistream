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

#ifndef FLINK_TNEL_REGISTEREDSTATEMETAINFOBASE_H
#define FLINK_TNEL_REGISTEREDSTATEMETAINFOBASE_H

#include <string>
#include "core/include/common.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
/** Base class for all registered state in state backends. */
class RegisteredStateMetaInfoBase {
public:
    explicit RegisteredStateMetaInfoBase(const std::string& name) : name(name)
    {
    }

    virtual ~RegisteredStateMetaInfoBase() = default;

    [[nodiscard]] const std::string& getName() const
    {
        return name;
    }
    static std::unique_ptr<RegisteredStateMetaInfoBase> fromMetaInfoSnapshot(const StateMetaInfoSnapshot& snapshot);

    virtual std::shared_ptr<StateMetaInfoSnapshot> snapshot()
    {
        NOT_IMPL_EXCEPTION;
    };

protected:
    /** The name of the state */
    const std::string name;
};

#endif // FLINK_TNEL_REGISTEREDSTATEMETAINFOBASE_H
