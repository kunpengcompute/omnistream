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
#include <vector>

namespace omnistream::utils {

/**
 * 使用标准 FNV-1a 64-bit 算法的无状态 hash functor。
 *
 * uint64_t 重载按低字节到高字节的固定顺序处理完整 8-byte 数值，结果不依赖主机
 * 内存布局；pointer 和 vector 重载按 data[0] 到 data[size - 1] 的存储顺序处理
 * 任意长度字节序列。pointer 重载允许 size 为 0 时 data 为 nullptr；size 大于 0
 * 时调用方必须保证 data 有效。返回值用于 hash table 选桶，不用于持久化、校验和
 * 或密码学用途。
 */
struct Fnv1a64Hash {
    constexpr std::size_t operator()(uint64_t value) const noexcept
    {
        uint64_t hash = OFFSET_BASIS;
        for (std::size_t index = 0; index < sizeof(value); ++index) {
            hash = updateByte(hash, static_cast<uint8_t>(value & OCTET_MASK));
            value >>= BITS_PER_OCTET;
        }
        return static_cast<std::size_t>(hash);
    }

    constexpr std::size_t operator()(const int8_t* data, std::size_t size) const noexcept
    {
        uint64_t hash = OFFSET_BASIS;
        for (std::size_t index = 0; index < size; ++index) {
            hash = updateByte(hash, static_cast<uint8_t>(data[index]));
        }
        return static_cast<std::size_t>(hash);
    }

    std::size_t operator()(const std::vector<int8_t>& value) const noexcept
    {
        return (*this)(value.data(), value.size());
    }

private:
    static constexpr uint64_t OFFSET_BASIS = 14695981039346656037ULL;
    static constexpr uint64_t PRIME = 1099511628211ULL;
    static constexpr uint64_t OCTET_MASK = 0xffU;
    static constexpr uint32_t BITS_PER_OCTET = 8U;

    static constexpr uint64_t updateByte(uint64_t hash, uint8_t value) noexcept
    {
        return (hash ^ value) * PRIME;
    }
};

} // namespace omnistream::utils
