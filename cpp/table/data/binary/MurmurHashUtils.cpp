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

#include "MurmurHashUtils.h"
#include <stdexcept>
#include <libboundscheck/include/securec.h>

int MurmurHashUtils::hashUnsafeBytesByWords(const void* base, std::size_t offset, int lengthInBytes)
{
    return hashUnsafeBytesByWords(base, offset, lengthInBytes, DEFAULT_SEED);
}

int MurmurHashUtils::hashUnsafeBytes(const void* base, std::size_t offset, int lengthInBytes)
{
    return hashUnsafeBytes(base, offset, lengthInBytes, DEFAULT_SEED);
}

int MurmurHashUtils::hashBytesByWords(const uint8_t* segment, int offset, int lengthInBytes)
{
    return hashBytesByWords(segment, offset, lengthInBytes, DEFAULT_SEED);
}

int MurmurHashUtils::hashBytes(const uint8_t* segment, int offset, int lengthInBytes)
{
    return hashBytes(segment, offset, lengthInBytes, DEFAULT_SEED);
}

int MurmurHashUtils::hashUnsafeBytesByWords(const void* base, std::size_t offset, int lengthInBytes, int seed)
{
    int h1 = hashUnsafeBytesByInt(base, offset, lengthInBytes, seed);
    return fmix(h1, lengthInBytes);
}

int MurmurHashUtils::hashBytesByWords(const uint8_t* segment, int offset, int lengthInBytes, int seed)
{
    int h1 = hashBytesByInt(segment, offset, lengthInBytes, seed);
    return fmix(h1, lengthInBytes);
}

int MurmurHashUtils::hashBytes(const uint8_t* segment, int offset, int lengthInBytes, int seed)
{
    int lengthAligned = lengthInBytes - lengthInBytes % 4;
    int h1 = hashBytesByInt(segment, offset, lengthAligned, seed);
    for (int i = lengthAligned; i < lengthInBytes; i++) {
        int k1 = mixK1(segment[offset + i]);
        h1 = mixH1(h1, k1);
    }
    return fmix(h1, lengthInBytes);
}

int MurmurHashUtils::hashUnsafeBytes(const void* base, std::size_t offset, int lengthInBytes, int seed)
{
    if (lengthInBytes < 0) {
        throw std::runtime_error("Length in bytes cannot be negative.");
    }

    int lengthAligned = lengthInBytes - lengthInBytes % 4;
    int h1 = hashUnsafeBytesByInt(base, offset, lengthAligned, seed);
    for (int i = lengthAligned; i < lengthInBytes; i++) {
        int halfWord = static_cast<const uint8_t*>(base)[offset + static_cast<std::size_t>(i)];
        int k1 = mixK1(halfWord);
        h1 = mixH1(h1, k1);
    }
    return fmix(h1, lengthInBytes);
}

int MurmurHashUtils::hashUnsafeBytesByInt(const void* base, std::size_t offset, int lengthInBytes, int seed)
{
    if (lengthInBytes % WORD_SIZE != 0) {
        throw std::runtime_error("Length in bytes is not aligned to 4 bytes.");
    }
    int h1 = seed;
    for (int i = 0; i < lengthInBytes; i += 4) {
        int halfWord;
        memcpy_s(&halfWord, sizeof(int), static_cast<const uint8_t*>(base) + offset + i, sizeof(int));
        int k1 = mixK1(halfWord);
        h1 = mixH1(h1, k1);
    }
    return h1;
}

int MurmurHashUtils::hashBytesByInt(const uint8_t* segment, int offset, int lengthInBytes, int seed)
{
    if (lengthInBytes % WORD_SIZE != 0) {
        throw std::runtime_error("Length in bytes is not aligned to 4 bytes.");
    }
    int h1 = seed;
    for (int i = 0; i < lengthInBytes; i += 4) {
        int halfWord;
        memcpy_s(&halfWord, sizeof(int), segment + offset + i, sizeof(int));
        int k1 = mixK1(halfWord);
        h1 = mixH1(h1, k1);
    }
    return h1;
}

int MurmurHashUtils::mixK1(int k1)
{
    k1 *= C1;
    unsigned int uk = static_cast<unsigned int>(k1);
    uk = (uk << 15) | (uk >> (32 - 15));
    k1 = static_cast<int>(uk);
    k1 *= C2;
    return k1;
}

int MurmurHashUtils::mixH1(int h1, int k1)
{
    unsigned int uh = static_cast<unsigned int>(h1);
    uh ^= static_cast<unsigned int>(k1);
    uh = (uh << 13) | (uh >> (32 - 13));
    uh = uh * 5 + 0xe6546b64;
    return static_cast<int>(uh);
}

int MurmurHashUtils::fmix(int h1, int length)
{
    unsigned int uh = static_cast<unsigned int>(h1);
    uh ^= static_cast<unsigned int>(length);
    return fmix(static_cast<int>(uh));
}

int MurmurHashUtils::fmix(int h)
{
    unsigned int uh = static_cast<unsigned int>(h);
    uh ^= static_cast<unsigned int>(uh) >> 16;
    uh *= 0x85ebca6b;
    uh ^= static_cast<unsigned int>(uh) >> 13;
    uh *= 0xc2b2ae35;
    uh ^= static_cast<unsigned int>(uh) >> 16;
    return static_cast<int>(uh);
}

std::uint64_t MurmurHashUtils::fmix(std::uint64_t h)
{
    h ^= static_cast<unsigned long long>(h) >> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= static_cast<unsigned long long>(h) >> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= static_cast<unsigned long long>(h) >> 33;
    return h;
}
