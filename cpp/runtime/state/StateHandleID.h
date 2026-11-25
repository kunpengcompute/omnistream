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

#ifndef FLINK_TNEL_STATEHANDLEID_H
#define FLINK_TNEL_STATEHANDLEID_H
#include <string>
#include "runtime/state/UUID.h"

class StateHandleID {
public:
    explicit StateHandleID(const std::string& keyString) : keyString_(keyString)
    {
    }

    const std::string& getKeyString() const
    {
        return keyString_;
    }

    bool operator==(const StateHandleID& other) const
    {
        return keyString_ == other.keyString_;
    }

    bool operator!=(const StateHandleID& other) const
    {
        return !(*this == other);
    }

    bool operator<(const StateHandleID& other) const
    {
        return keyString_ < other.keyString_;
    }

    std::size_t hashCode() const
    {
        return std::hash<std::string>()(keyString_);
    }

    std::string ToString() const
    {
        return "{\"keyString\": \"" + keyString_ + "\"}";
    }

    static StateHandleID randomStateHandleId()
    {
        return StateHandleID(UUID::randomUUID().ToString());
    }

private:
    std::string keyString_;
};

#endif // FLINK_TNEL_STATEHANDLEID_H
