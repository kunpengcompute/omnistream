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
#include <cstdint>

/**
 * Non-owning read-only view over a contiguous byte range.
 *
 * Replaces std::vector<int8_t> copies in savepoint iterator hot paths with
 * zero-copy views. The underlying buffer must remain valid for the lifetime
 * of the view. Intended for use in single-entry scope where buffer stability
 * is guaranteed (e.g. RocksDB iterator key/value slices, serialized blobs
 * held in members that outlive the view).
 */
class ByteView {
public:
    ByteView() : data_(nullptr), length_(0)
    {
    }
    ~ByteView() = default;

    template <typename T>
    ByteView(const T* data, size_t length)
        : data_(reinterpret_cast<const uint8_t*>(data)),
          length_(data == nullptr ? 0 : length * sizeof(T))
    {
    }

    ByteView(const ByteView& other) = default;
    ByteView(ByteView&& other) noexcept = default;

    ByteView& operator=(const ByteView& other) = default;
    ByteView& operator=(ByteView&& other) noexcept = default;

    template <typename T>
    static ByteView fromBuffer(const T* buffer, size_t length)
    {
        return {buffer, length};
    }

    const uint8_t* data() const
    {
        return data_;
    }
    size_t size() const
    {
        return length_;
    }
    bool empty() const
    {
        return length_ == 0;
    }

    const uint8_t& operator[](size_t index) const
    {
        return data_[index];
    }
    const uint8_t* begin() const
    {
        return data_;
    }
    const uint8_t* end() const
    {
        return data_ == nullptr ? nullptr : data_ + length_;
    }

private:
    const uint8_t* data_;
    size_t length_;
};
