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

#ifndef MT_CHECK_BIGINTEGER_H
#define MT_CHECK_BIGINTEGER_H
#include <vector>
#include <algorithm>
#include <iostream>

#include "Object.h"
#include "String.h"
#include "basictypes/Long.h"

class BigInteger : public Object {
public:
    BigInteger() = default;
    BigInteger(String* val);
    BigInteger(const std::string& val);
    BigInteger(const std::string& val, int radix);
    BigInteger(int64_t val);
    BigInteger(const std::vector<int>& val);
    BigInteger(const std::vector<int>& magnitude, int signum);

    static BigInteger* valueOf(int64_t val);
    static BigInteger* valueOf(std::vector<int>& val);

    int bitLength() const;
    std::vector<unsigned char> toByteArray();
    void setByteArray(uint8_t* buffer, int capacity, int offset, int length);

    int hashCode();
    bool equals(Object* obj);
    std::string toString(int radix = 10) const;
    Object* clone();

    BigInteger& operator=(const BigInteger& other);
    bool operator==(const BigInteger& other) const;
    bool operator!=(const BigInteger& other) const;

    int64_t sizeInBytes() const override
    {
        return static_cast<int64_t>(sizeof(BigInteger) + mag.size() * sizeof(int));
    }

    std::vector<int> mag;
    int signum; // -1 for negative, 0 for zero, or 1 for positive.

    static const int MAX_MAG_LENGTH = 100000;
    static const int SCHOENHAGE_BASE_CONVERSION_THRESHOLD = 20;
    static const int CHARACTER_MIN_RADIX = 2;
    static const int CHARACTER_MAX_RADIX = 36;

private:
    std::vector<int> makePositive(const std::vector<int>& val);
    std::vector<int> makePositive(const uint8_t* start, const int length);
    std::vector<int> trustedStripLeadingZeroInts(const std::vector<int>& val);
    std::vector<int> stripLeadingZeroBytes(const uint8_t* start, const int length);
    void checkRange();
    void checkRange() const;
    void destructiveMulAdd(std::vector<int>& magnitude, int superRadix, int groupVal);
    int getInt(int n);
    int firstNonzeroIntNum();
    int bitLengthForInt(int value) const;
    int bitCount(int value) const;

    int firstNonzeroIntNumIndex = 0;

    static constexpr int64_t bitsPerDigit[] = {0,    0,    1024, 1624, 2048, 2378, 2648, 2875, 3072, 3247,
                                               3402, 3543, 3672, 3790, 3899, 4001, 4096, 4186, 4271, 4350,
                                               4426, 4498, 4567, 4633, 4696, 4756, 4814, 4870, 4923, 4975,
                                               5025, 5074, 5120, 5166, 5210, 5253, 5295};

    static constexpr int digitsPerInt[] = {0, 0, 30, 19, 15, 13, 11, 11, 10, 9, 9, 8, 8, 8, 8, 7, 7, 7, 7,
                                           7, 7, 7,  6,  6,  6,  6,  6,  6,  6, 6, 6, 6, 6, 6, 6, 6, 5};

    static constexpr int intRadix[] = {
        0,          0,          0x40000000, 0x4546b3db, 0x40000000, 0x48c27395, 0x159fd800, 0x75db9c97,
        0x40000000, 0x17179149, 0x3b9aca00, 0xcc6db61,  0x19a10000, 0x309f1021, 0x57f6c100, 0xa2f1b6f,
        0x10000000, 0x18754571, 0x247dbc80, 0x3547667b, 0x4c4b4000, 0x6b5a6e1d, 0x6c20a40,  0x8d2d931,
        0xb640000,  0xe8d4a51,  0x1269ae40, 0x17179149, 0x1cb91000, 0x23744899, 0x2b73a840, 0x34e63b41,
        0x40000000, 0x4cfa3cc1, 0x5c13d840, 0x6d91b519, 0x39aa400};

    static constexpr int64_t LONG_MASK = 0xffffffffL;
};

extern const BigInteger ZERO;
extern const BigInteger ONE;
extern const BigInteger TEN;

#endif // MT_CHECK_BIGINTEGER_H
