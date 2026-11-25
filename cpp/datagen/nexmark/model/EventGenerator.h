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

#ifndef OMNISTREAM_EVENTGENERATOR_H
#define OMNISTREAM_EVENTGENERATOR_H

#include <iostream>
#include <string>
#include <chrono>
#include <random>
#include <algorithm>
// For formatting output of time_point (if needed)
#include <ctime>
#include <climits>
#include <cmath>
#include <cctype>
#include <tuple>
#include <iomanip>
#include "Event.h"
#include "../GeneratorConfig.h"

class SplittableRandom {
public:
    explicit SplittableRandom(int seed) : engine(seed)
    {
        dist_int = std::uniform_int_distribution<uint32_t>(0, INT_MAX);
        dist_long = std::uniform_int_distribution<uint64_t>(0, INT64_MAX);
        dist_double = std::uniform_real_distribution<double>(0.0, 1.0);
        dist_char = std::uniform_int_distribution<uint32_t>(0, 26);
    }
    int nextInt()
    {
        // Java SplittableRandom.nextInt() covers every signed 32-bit value, so rnd % 26 can be negative.
        return std::uniform_int_distribution<int>(INT_MIN, INT_MAX)(engine);
    }
    int nextInt(int bound)
    {
        return static_cast<int>(dist_int(engine)) % bound;
    }
    long nextLong()
    {
        return dist_long(engine);
    }
    long nextLong(long bound)
    {
        return static_cast<int64_t>(dist_long(engine)) % bound;
    }
    double nextDouble()
    {
        return dist_double(engine);
    }
    long nextPrice()
    {
        return static_cast<long>(std::lround(std::pow(10.0, nextDouble() * 6.0) * 100.0));
    }
    std::string nextStringNew(int length, char special = ' ')
    {
        std::string str(length, ' ');
        // Handle remaining characters sequentially
        for (int i = 0; i < length; ++i) {
            unsigned int index = dist_char(engine);
            str[i] = index == 13 ? special : static_cast<char>('a' + index);
        }
        return str;
    }
    // Used when there's an existing buffer
    std::string_view nextString(char* buffer, int length, char special = ' ')
    {
        for (int i = 0; i < length; ++i) {
            unsigned int index = dist_char(engine);
            buffer[i] = index == 13 ? special : static_cast<char>('a' + index);
        }
        return buffer;
    }

private:
    std::minstd_rand engine;
    std::uniform_int_distribution<uint32_t> dist_int;
    std::uniform_int_distribution<uint64_t> dist_long;
    std::uniform_real_distribution<double> dist_double;
    std::uniform_int_distribution<uint32_t> dist_char;
    char* buffer;
};

// Helper function to trim whitespace from both ends of a string
std::string trim(const std::string& s);

/** Generates strings which are used for different field in other model objects. */
class StringsGenerator {
public:
    /** Smallest random string size. */
    static const int MIN_STRING_LENGTH = 3;
    // REUSABLE_EXTRA_STRING is a static reusable string computed from nextExactString.
    static std::string REUSABLE_EXTRA_STRING;
    // Return a random string of up to {@code maxLength}.
    static std::string nextString(SplittableRandom& random, int maxLength)
    {
        return nextString(random, maxLength, ' ');
    }
    static std::string nextString(SplittableRandom& random, int maxLength, char special);
    template <bool useSpecial>
    static void fillWithRandomLower(SplittableRandom& random, char* buffer, int length, char special = ' ');

    /** Return a random string of exactly {@code length}. */
    static std::string_view nextExactString(SplittableRandom& random, size_t length, char* buffer);

    /**
     * Return a random {@code string} such that {@code currentSize + string.length()} is on average
     * {@code averageSize}.
     */
    static std::string_view nextExtra(SplittableRandom& random, int currentSize, int desiredAverageSize, char* buffer);
};

template <bool useSpecial>
void StringsGenerator::fillWithRandomLower(SplittableRandom& random, char* buffer, int length, char special)
{
    int rnd = 0;
    int n = 0; // number of random characters left in rnd
    for (int i = 0; i < length; ++i) {
        if (n == 0) {
            rnd = random.nextInt();
            n = 6; // log_26(2^31)
        }
        if constexpr (useSpecial) {
            int index = rnd % 26;
            buffer[i] = (index == 13 ? special : static_cast<char>('a' + index));
        } else {
            buffer[i] = static_cast<char>('a' + rnd % 26);
        }
        rnd /= 26;
        n--;
    }
}

#endif // OMNISTREAM_EVENTGENERATOR_H
