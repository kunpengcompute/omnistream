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

#ifndef MURMURHASHUTILS_H
#define MURMURHASHUTILS_H

#include <cstdint>
#include <cassert>
#include <cstring>

class MurmurHashUtils {
public:
    static const int DEFAULT_SEED = 42;
    static const int WORD_SIZE = 4;

    static int hashUnsafeBytesByWords(const void* base, std::size_t offset, int lengthInBytes);
    static int hashUnsafeBytes(const void* base, std::size_t offset, int lengthInBytes);
    static int hashBytesByWords(const uint8_t* segment, int offset, int lengthInBytes);
    static int hashBytes(const uint8_t* segment, int offset, int lengthInBytes);

private:
    static const int C1 = 0xcc9e2d51;
    static const int C2 = 0x1b873593;

    MurmurHashUtils() = delete;

    static int hashUnsafeBytesByWords(const void* base, std::size_t offset, int lengthInBytes, int seed);
    static int hashBytesByWords(const uint8_t* segment, int offset, int lengthInBytes, int seed);
    static int hashBytes(const uint8_t* segment, int offset, int lengthInBytes, int seed);
    static int hashUnsafeBytes(const void* base, std::size_t offset, int lengthInBytes, int seed);
    static int hashUnsafeBytesByInt(const void* base, std::size_t offset, int lengthInBytes, int seed);
    static int hashBytesByInt(const uint8_t* segment, int offset, int lengthInBytes, int seed);
    static int mixK1(int k1);
    static int mixH1(int h1, int k1);
    static int fmix(int h1, int length);
    static int fmix(int h);
    static std::uint64_t fmix(std::uint64_t h);
};

#endif // MURMURHASHUTILS_H
