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

#include "StringUtf8Utils.h"
#include "../../../core/include/common.h"

std::u32string* StringUtf8Utils::decodeUTF8(uint8_t* bytes, int offset, int len)
{
    std::u32string* result = new std::u32string();
    int i = offset;

    while (i < offset + len) {
        char32_t codepoint = 0;
        unsigned char byte = static_cast<unsigned char>(bytes[i]);

        if ((byte & 0x80) == 0x00) {
            // 1-byte sequence
            codepoint = byte;
            ++i;
        } else if ((byte & 0xE0) == 0xC0) {
            // 2-byte sequence
            if (i + 1 >= offset + len) THROW_LOGIC_EXCEPTION("Invalid UTF-8 sequence: unexpected end of input");
            codepoint = (byte & 0x1F) << 6;
            codepoint |= (bytes[i + 1] & 0x3F);
            i += 2;
        } else if ((byte & 0xF0) == 0xE0) {
            // 3-byte sequence
            if (i + 2 >= offset + len) THROW_LOGIC_EXCEPTION("Invalid UTF-8 sequence: unexpected end of input");
            codepoint = (byte & 0x0F) << 12;
            codepoint |= (bytes[i + 1] & 0x3F) << 6;
            codepoint |= (bytes[i + 2] & 0x3F);
            i += 3;
        } else if ((byte & 0xF8) == 0xF0) {
            // 4-byte sequence
            if (i + 3 >= offset + len) THROW_LOGIC_EXCEPTION("Invalid UTF-8 sequence: unexpected end of input");
            codepoint = (byte & 0x07) << 18;
            codepoint |= (bytes[i + 1] & 0x3F) << 12;
            codepoint |= (bytes[i + 2] & 0x3F) << 6;
            codepoint |= (bytes[i + 3] & 0x3F);
            i += 4;
        } else {
            // Invalid byte sequence
            THROW_LOGIC_EXCEPTION("Invalid UTF-8 sequence");
        }
        result->push_back(codepoint);
    }
    return result;
}

// Encode UTF-8 functions

int StringUtf8Utils::computeUTF8Length(const std::u32string* str)
{
    int length = 0;
    for (char32_t codepoint : *str) {
        if (codepoint <= 0x7F) {
            length += 1;
        } else if (codepoint <= 0x7FF) {
            length += 2;
        } else if (codepoint <= 0xFFFF) {
            length += 3;
        } else if (codepoint <= 0x10FFFF) {
            length += 4;
        } else {
            THROW_LOGIC_EXCEPTION("Invalid Unicode code point");
        }
    }
    return length;
}

uint8_t* StringUtf8Utils::encodeUTF8(const std::u32string* str)
{
    int length = computeUTF8Length(str);
    uint8_t* bytes = new uint8_t[length];
    uint8_t* ptr = bytes;

    for (char32_t codepoint : *str) {
        if (codepoint <= 0x7F) {
            *ptr++ = static_cast<uint8_t>(codepoint);
        } else if (codepoint <= 0x7FF) {
            *ptr++ = static_cast<uint8_t>(0xC0 | (codepoint >> 6));
            *ptr++ = static_cast<uint8_t>(0x80 | (codepoint & 0x3F));
        } else if (codepoint <= 0xFFFF) {
            *ptr++ = static_cast<uint8_t>(0xE0 | (codepoint >> 12));
            *ptr++ = static_cast<uint8_t>(0x80 | ((codepoint >> 6) & 0x3F));
            *ptr++ = static_cast<uint8_t>(0x80 | (codepoint & 0x3F));
        } else if (codepoint <= 0x10FFFF) {
            *ptr++ = static_cast<uint8_t>(0xF0 | (codepoint >> 18));
            *ptr++ = static_cast<uint8_t>(0x80 | ((codepoint >> 12) & 0x3F));
            *ptr++ = static_cast<uint8_t>(0x80 | ((codepoint >> 6) & 0x3F));
            *ptr++ = static_cast<uint8_t>(0x80 | (codepoint & 0x3F));
        } else {
            THROW_LOGIC_EXCEPTION("Invalid Unicode code point");
        }
    }

    return bytes;
}
