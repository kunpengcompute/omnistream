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

#ifndef FLINK_TNEL_STRINGDATA_H
#define FLINK_TNEL_STRINGDATA_H

#include <string>
#include <vector>
#include <cstdint> // For uint8_t

class StringData {
public:
    virtual uint8_t* toBytes() = 0;
    virtual std::u32string* toString() = 0;
    virtual std::string* ToUtF8String() = 0;

    static StringData* fromString(std::u32string* str);

    static StringData* fromBytes(uint8_t* bytes, int len);

    static StringData* fromBytes(uint8_t* bytes, int offset, int numBytes);
};

#endif // FLINK_TNEL_STRINGDATA_H
