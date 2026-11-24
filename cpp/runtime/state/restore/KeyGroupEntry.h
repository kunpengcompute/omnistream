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

#ifndef OMNISTREAM_KEYGROUPENTRY_H
#define OMNISTREAM_KEYGROUPENTRY_H
#include <cstdint>
#include <vector>
#include <memory>

class KeyGroupEntry {
public:
    using ByteArray = std::vector<int8_t>;

    KeyGroupEntry(int kvStateId, ByteArray key, ByteArray value)
        : kvStateId_(kvStateId),
          key_(std::move(key)),
          value_(std::move(value))
    {
    }

    int getKvStateId() const noexcept
    {
        return kvStateId_;
    }

    const ByteArray& getKey() const noexcept
    {
        return key_;
    }

    const ByteArray& getValue() const noexcept
    {
        return value_;
    }

    ByteArray&& moveKey() noexcept
    {
        return std::move(key_);
    }

    ByteArray&& moveValue() noexcept
    {
        return std::move(value_);
    }

private:
    int kvStateId_;
    ByteArray key_;
    ByteArray value_;
};

#endif // OMNISTREAM_KEYGROUPENTRY_H
