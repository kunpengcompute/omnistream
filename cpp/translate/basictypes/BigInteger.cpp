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

#include "basictypes/BigInteger.h"

void BigInteger::checkRange() const
{
    if (mag.size() >= MAX_MAG_LENGTH) {
        throw std::runtime_error("BigInteger::checkRange error");
        return;
    }
}

void BigInteger::destructiveMulAdd(std::vector<int>& x, int y, int z)
{
    int64_t ylong = static_cast<int64_t>(y) & LONG_MASK;
    int64_t zlong = static_cast<int64_t>(z) & LONG_MASK;
    int len = x.size();

    // 乘法部分
    int64_t product = 0;
    int64_t carry = 0;
    for (int i = len - 1; i >= 0; --i) {
        product = ylong * (static_cast<int64_t>(x[i]) & LONG_MASK) + carry;
        x[i] = static_cast<int>(product);
        carry = product >> 32; // 逻辑右移，相当于Java中的>>>
    }

    // 加法部分
    int64_t sum = (static_cast<int64_t>(x[len - 1]) & LONG_MASK) + zlong;
    x[len - 1] = static_cast<int>(sum);
    carry = sum >> 32;

    for (int i = len - 2; i >= 0; --i) {
        sum = (static_cast<int64_t>(x[i]) & LONG_MASK) + carry;
        x[i] = static_cast<int>(sum);
        carry = sum >> 32;
    }
    return;
}

int BigInteger::hashCode()
{
    int64_t hashCode = 0;
    for (size_t i = 0; i < mag.size(); i++) {
        hashCode = static_cast<int>(31 * hashCode + (static_cast<int64_t>(mag[i]) & LONG_MASK));
    }
    return static_cast<int>(hashCode) * signum;
}

bool BigInteger::equals(Object* obj)
{
    if (obj == nullptr) {
        return false;
    }
    const auto* bigInteger = reinterpret_cast<BigInteger*>(obj);
    if (bigInteger == this) {
        return true;
    }

    if (bigInteger->signum != this->signum) {
        return false;
    }

    if (bigInteger->mag.size() != this->mag.size()) {
        return false;
    }

    for (size_t i = 0; i < this->mag.size(); ++i) {
        if (bigInteger->mag[i] != this->mag[i]) {
            return false;
        }
    }
    return true;
}

Object* BigInteger::clone()
{
    return new BigInteger(this->mag, this->signum);
}

std::string BigInteger::toString(int radix) const
{
    if (signum == 0) return "0";

    // Copy magnitude since we need to modify it
    std::vector<uint32_t> magCopy;
    magCopy.reserve(mag.size());
    for (int num : mag) {
        magCopy.push_back(static_cast<uint32_t>(num));
    }

    std::string result;

    // Repeatedly divide by 10
    while (!magCopy.empty()) {
        uint32_t remainder = 0;
        std::vector<uint32_t> quotient;

        for (uint32_t word : magCopy) {
            uint64_t value = ((uint64_t)remainder << 32) | word;
            quotient.push_back((uint32_t)(value / 10));
            remainder = value % 10;
        }

        result += (char)('0' + remainder);
        size_t first_non_zero = 0;
        while (first_non_zero < quotient.size() && quotient[first_non_zero] == 0) {
            ++first_non_zero;
        }
        magCopy.assign(quotient.begin() + first_non_zero, quotient.end());
    }

    // Reverse string since digits are collected backwards
    std::reverse(result.begin(), result.end());

    if (signum < 0) {
        result.insert(result.begin(), '-');
    }

    return result;
}

BigInteger::BigInteger(const std::string& val, int radix)
{
    int cursor = 0;
    int len = val.length();

    if (radix < CHARACTER_MIN_RADIX || radix > CHARACTER_MAX_RADIX) {
        throw std::runtime_error("Radix out of range");
    }
    if (len == 0) {
        throw std::runtime_error("Zero length BigInteger");
    }

    // 处理符号
    int sign = 1;
    size_t index1 = val.find('-');
    size_t index2 = val.find('+');
    if (index1 != std::string::npos) {
        if (index1 != 0 || index2 != std::string::npos) {
            throw std::runtime_error("Illegal embedded sign character");
        }
        sign = -1;
        cursor = 1;
    } else if (index2 != std::string::npos) {
        if (index2 != 0) {
            throw std::runtime_error("Illegal embedded sign character");
        }
        sign = 1;
        cursor = 1;
    }

    if (cursor == len) {
        throw std::runtime_error("Zero length BigInteger");
    }

    // 跳过前导零
    while (cursor < len && isdigit(val[cursor]) && val[cursor] == '0') {
        cursor++;
    }

    if (cursor == len) {
        signum = 0;
        mag = std::vector<int>();
        return;
    }

    int numDigits = len - cursor;
    signum = sign;

    // 计算所需的位数
    int64_t numBits = ((numDigits * bitsPerDigit[radix]) >> 10) + 1;
    if (numBits + 31 >= (1L << 32)) {
        throw std::runtime_error("BigInteger overflow");
    }
    int numWords = (numBits + 31) >> 5;

    std::vector<int> magnitude(numWords, 0);

    // 处理第一个数字组
    int firstGroupLen = numDigits % digitsPerInt[radix]; // 假设每组最多10位
    if (firstGroupLen == 0) {
        firstGroupLen = digitsPerInt[radix];
    }
    std::string group = val.substr(cursor, firstGroupLen);
    cursor += firstGroupLen;
    magnitude[numWords - 1] = stoi(group, nullptr, radix);
    if (magnitude[numWords - 1] < 0) {
        throw std::runtime_error("Illegal digit");
    }

    // 处理剩余的数字组
    int superRadix = intRadix[radix];
    int groupVal = 0;
    while (cursor < len) {
        group = val.substr(cursor, digitsPerInt[radix]);
        cursor += digitsPerInt[radix];
        groupVal = stoi(group, nullptr, radix);
        if (groupVal < 0) {
            throw std::runtime_error("Illegal digit");
        }
        destructiveMulAdd(magnitude, superRadix, groupVal);
    }

    // 去除前导零
    mag = trustedStripLeadingZeroInts(magnitude);
    if (mag.size() >= MAX_MAG_LENGTH) {
        checkRange();
    }
    return;
}

BigInteger::BigInteger(const std::string& val) : BigInteger(val, 10)
{
}

BigInteger::BigInteger(String* val)
{
    if (val == nullptr) {
        throw std::runtime_error("String* is nullptr");
        return;
    }

    BigInteger(val->toString(), 10);
    return;
}

BigInteger::BigInteger(int64_t val)
{
    if (val == 0) {
        signum = 0;
        mag = std::vector<int>();
        return;
    }

    if (val < 0) {
        val = -val;
        signum = -1;
    } else {
        signum = 1;
    }

    int highWord = static_cast<int>(val >> 32);
    int lowWord = static_cast<int>(val);

    if (highWord == 0) {
        mag = std::vector<int>(1, static_cast<int>(lowWord));
    } else {
        mag = std::vector<int>(2);
        mag[0] = static_cast<int>(highWord);
        mag[1] = static_cast<int>(lowWord);
    }
}

std::vector<int> BigInteger::makePositive(const std::vector<int>& val)
{
    // 实现makePositive逻辑
    std::vector<int> result(val);
    // 补码转换逻辑
    bool carry = true;
    for (int i = result.size() - 1; i >= 0 && carry; --i) {
        result[i] = ~result[i];
        carry = (result[i] & 1) ? false : true;
        result[i] += 1;
    }
    if (carry) {
        result.insert(result.begin(), 1);
    }
    return result;
}

std::vector<int> BigInteger::trustedStripLeadingZeroInts(const std::vector<int>& val)
{
    int keep = 0;
    int vlen = val.size();
    // 找到第一个非零元素的位置
    while (keep < vlen && val[keep] == 0) {
        keep++;
    }
    if (keep == 0) {
        return val; // 没有前导零，返回原数组
    } else {
        // 创建新的数组并复制从keep位置开始的元素
        return std::vector<int>(val.begin() + keep, val.end());
    }
}

void BigInteger::checkRange()
{
    // 实现checkRange逻辑
    if (mag.size() > MAX_MAG_LENGTH) {
        throw std::runtime_error("BigInteger out of range");
    }
}

BigInteger::BigInteger(const std::vector<int>& val)
{
    if (val.empty()) {
        throw std::runtime_error("Zero length BigInteger");
    }

    if (val[0] < 0) {
        mag = makePositive(val);
        signum = -1;
    } else {
        mag = trustedStripLeadingZeroInts(val);
        signum = (mag.empty() ? 0 : 1);
    }

    if (!mag.empty() && mag.size() >= MAX_MAG_LENGTH) {
        checkRange();
    }
}

BigInteger::BigInteger(const std::vector<int>& magnitude, int signum)
    : mag(magnitude),
      signum(magnitude.empty() ? 0 : signum)
{
    if (!mag.empty() && mag.size() >= MAX_MAG_LENGTH) {
        checkRange();
    }
}

BigInteger* BigInteger::valueOf(int64_t val)
{
    return new BigInteger(val);
}

BigInteger* BigInteger::valueOf(std::vector<int>& val)
{
    if (val.size() == 0) {
        return new BigInteger((int64_t)0);
    }

    if (val[0] > 0) {
        return new BigInteger(val, 1);
    }

    return new BigInteger(val);
}

int BigInteger::bitCount(int value) const
{
    int count = 0;
    while (value) {
        count += value & 1;
        value >>= 1;
    }
    return count;
}

int BigInteger::bitLengthForInt(int value) const
{
    if (value == 0) {
        return 0;
    }
    int count = 0;
    while (value >>= 1) {
        count++;
    }
    return count + 1;
}

int BigInteger::bitLength() const
{
    int len = mag.size();
    if (len == 0) {
        return 0;
    }

    int magBitLength = ((len - 1) << 5) + bitLengthForInt(mag[0]);

    if (signum < 0) {
        // 检查是否为2的幂
        bool pow2 = (bitCount(mag[0]) == 1);
        for (size_t i = 1; i < mag.size() && pow2; ++i) {
            pow2 = (mag[i] == 0);
        }

        if (pow2) {
            magBitLength -= 1;
        }
    }

    return magBitLength;
}

int BigInteger::firstNonzeroIntNum()
{
    int fn = firstNonzeroIntNumIndex - 2;
    if (fn == -2) { // 未初始化
        fn = 0;

        int mlen = mag.size();
        int i;
        for (i = mlen - 1; i >= 0 && mag[i] == 0; --i); // 查找第一个非零整数

        fn = (mlen - i - 1);
        firstNonzeroIntNumIndex = fn + 2;
    }
    return fn;
}

int BigInteger::getInt(int n)
{
    if (n < 0) {
        return 0;
    }
    if (n >= static_cast<int>(mag.size())) {
        return signum < 0 ? -1 : 0; // 假设signInt()返回符号位的值
    }

    int magInt = mag[mag.size() - n - 1];
    if (signum >= 0) {
        return magInt;
    }

    if (n <= firstNonzeroIntNum()) {
        return -magInt;
    }

    return ~magInt;
}

std::vector<unsigned char> BigInteger::toByteArray()
{
    if (mag.empty()) {
        return std::vector<unsigned char>();
    }

    int byteLen = (bitLength() / 8) + 1;
    std::vector<unsigned char> byteArray(byteLen, 0x00);

    int intIndex = 0;
    int bytesCopied = 4; // 每个int有4个字节
    int nextInt = 0;

    for (int i = byteLen - 1; i >= 0; --i) {
        if (bytesCopied == 4) {
            nextInt = getInt(intIndex++);
            bytesCopied = 1;
        } else {
            nextInt >>= 8;
            bytesCopied++;
        }
        byteArray[i] = static_cast<unsigned char>(nextInt & 0xFF);
    }
    return byteArray;
}

void BigInteger::setByteArray(uint8_t* buffer, int capacity, int offset, int length)
{
    if (buffer == nullptr || offset + length > capacity) {
        throw std::runtime_error("BigInteger::setByteArray error");
        return;
    }

    auto first = buffer + offset;
    if (first[0] < 0) {
        mag = makePositive(first, length);
        signum = -1;
    } else {
        mag = stripLeadingZeroBytes(first, length);
        signum = (mag.size() == 0 ? 0 : 1);
    }
    if (mag.size() >= MAX_MAG_LENGTH) {
        checkRange();
    }
    return;
}

std::vector<int> BigInteger::stripLeadingZeroBytes(const uint8_t* a, const int byteLength)
{
    if (byteLength == 0) {
        return std::vector<int>();
    }

    size_t keep = 0;
    while (keep < byteLength && a[keep] == 0) {
        ++keep;
    }

    if (keep == byteLength) {
        return std::vector<int>();
    }

    size_t intLength = (byteLength - keep + 3) / 4;
    std::vector<int> result(intLength, 0);

    size_t b = byteLength - 1;
    for (size_t i = intLength - 1; i < intLength; --i) {
        result[i] = a[b--] & 0xFF;
        size_t bytesRemaining = b - keep + 1;
        size_t bytesToTransfer = std::min<size_t>(3, bytesRemaining);
        for (size_t j = 8; j <= (bytesToTransfer << 3); j += 8) {
            if (b >= keep) {
                result[i] |= (a[b--] & 0xFF) << j;
            } else {
                break;
            }
        }
    }

    return result;
}

std::vector<int> BigInteger::makePositive(const uint8_t* a, const int byteLength)
{
    int keep = 0;

    while (keep < byteLength && a[keep] == -1) {
        keep++;
    }

    int k = keep;
    while (k < byteLength && a[k] == 0) {
        k++;
    }

    int extraByte = (k == byteLength) ? 1 : 0;
    int intLength = (byteLength - keep + extraByte + 3) / 4;

    std::vector<int> result(intLength, 0);

    int b = byteLength - 1;
    for (int i = intLength - 1; i >= 0; i--) {
        int numBytesToTransfer = std::min(3, b - keep + 1);
        if (numBytesToTransfer < 0) {
            numBytesToTransfer = 0;
        }

        int value = 0;
        for (int j = 0; j < numBytesToTransfer; j++) {
            value |= (static_cast<int>(a[b - j] & 0xFF)) << (8 * j);
        }

        int maskShift = 8 * (3 - numBytesToTransfer);
        int mask = 0xFFFFFFFF << maskShift;
        result[i] = (~value) & mask;
    }

    for (int i = intLength - 1; i >= 0; i--) {
        result[i] = (result[i] + 1) & 0xFFFFFFFF;
        if (result[i] != 0) {
            break;
        }
    }

    while (result.size() > 1 && result.front() == 0) {
        result.erase(result.begin());
    }
    return result;
}

BigInteger& BigInteger::operator=(const BigInteger& other)
{
    if (this != &other) {
        this->mag = other.mag;
        this->signum = other.signum;
    }
    return *this;
}

bool BigInteger::operator==(const BigInteger& other) const
{
    if (this->signum == other.signum &&
        std::equal(this->mag.begin(), this->mag.end(), other.mag.begin(), other.mag.end())) {
        return true;
    }
    return false;
}

bool BigInteger::operator!=(const BigInteger& other) const
{
    if (this->signum != other.signum ||
        !std::equal(this->mag.begin(), this->mag.end(), other.mag.begin(), other.mag.end())) {
        return true;
    }
    return false;
}

const BigInteger ZERO = BigInteger(static_cast<int64_t>(0));
const BigInteger ONE = BigInteger(static_cast<int64_t>(1));
const BigInteger TEN = BigInteger(static_cast<int64_t>(10));
