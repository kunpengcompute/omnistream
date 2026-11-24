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

#ifndef FLINK_TNEL_BINARYSTRINGDATA_H
#define FLINK_TNEL_BINARYSTRINGDATA_H

#include <string>
#include <vector>
#include <cstdint> // For uint8_t
#include <stdexcept>
#include "../StringData.h"
#include "StringUtf8Utils.h"
#include "LazyBinaryFormat.h"

class BinaryStringData : public StringData, public LazyBinaryFormat<std::u32string> {
public:
    // Constructors
    BinaryStringData() = default;
    explicit BinaryStringData(std::u32string* str) : LazyBinaryFormat<std::u32string>(str)
    {
        ensureMaterialized();
    };
    // BinaryStringData(MemorySegment *segments[], int offset, int sizeInBytes) : LazyBinaryFormat(segments, offset,
    // sizeInBytes) {}; BinaryStringData(MemorySegment *segments[], int offset, int sizeInBytes, std::u32string* str) :
    // LazyBinaryFormat<std::u32string>(segments, offset, sizeInBytes, str) {};

    explicit BinaryStringData(uint8_t* bytes, int offset, int sizeInBytes)
        : LazyBinaryFormat(bytes, offset, sizeInBytes)
    {
    }

    explicit BinaryStringData(std::string_view& str)
        : BinaryStringData(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(str.data())), 0, str.size())
    {
    }

    // Construction from data
    static BinaryStringData* fromString(std::u32string* str);
    static BinaryStringData* fromBytes(uint8_t* bytes, int len);
    static BinaryStringData* fromBytes(uint8_t* bytes, int offset, int len);

    static BinaryStringData* EMPTY_UTF8;

    // Getters
    std::u32string* toString() override;
    std::string* ToUtF8String() override;
    uint8_t* toBytes() override;

    // Util
    void ensureMaterialized();
    BinarySection* materialize(TypeSerializer*) override;
};

#endif // FLINK_TNEL_BINARYSTRINGDATA_H
