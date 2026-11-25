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

#include "BinaryStringData.h"

BinaryStringData* BinaryStringData::EMPTY_UTF8 = fromBytes(StringUtf8Utils::encodeUTF8(new std::u32string(U"")), 0);

void BinaryStringData::ensureMaterialized()
{
    if (materialized == false) {
        binarySection = materialize(nullptr);
        materialized = true;
    }
}

BinarySection* BinaryStringData::materialize(TypeSerializer* S)
{
    if (S != nullptr) {
        THROW_LOGIC_EXCEPTION("BinaryStringData does not support custom serializers");
    }

    uint8_t* bytes = StringUtf8Utils::encodeUTF8(object);
    int size = StringUtf8Utils::computeUTF8Length(object);

    return new BinarySection(bytes, 0, size);
}

std::u32string* BinaryStringData::toString()
{
    if (object == nullptr) {
        object = StringUtf8Utils::decodeUTF8(toBytes(), 0, getSizeInBytes());
    }
    if (object->empty()) {
        object = StringUtf8Utils::decodeUTF8(toBytes(), 0, getSizeInBytes());
    }
    return object;
}

std::string* BinaryStringData::ToUtF8String()
{
    if (!materialized) {
        // Object is already materialized (as std::u32string), encode to UTF-8 and construct std::string
        uint8_t* utf8Bytes = StringUtf8Utils::encodeUTF8(object);
        int utf8Len = StringUtf8Utils::computeUTF8Length(object);
        return new std::string(static_cast<const char*>(static_cast<const void*>(utf8Bytes)), utf8Len);
    }
    // Raw UTF-8 bytes already available — construct std::string directly
    return new std::string(static_cast<const char*>(static_cast<const void*>(toBytes())), getSizeInBytes());
}

uint8_t* BinaryStringData::toBytes()
{
    ensureMaterialized();
    return getSegment() + getOffset();
}

BinaryStringData* BinaryStringData::fromString(std::u32string* str)
{
    if (str->empty()) {
        return nullptr;
    } else {
        return new BinaryStringData(str);
    }
}

BinaryStringData* BinaryStringData::fromBytes(uint8_t* bytes, int len)
{
    return fromBytes(bytes, 0, len);
}

BinaryStringData* BinaryStringData::fromBytes(uint8_t* bytes, int offset, int len)
{
    return new BinaryStringData(bytes, offset, len);
}
