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

#include <iostream>
#include <locale>
#include <codecvt>
#include "common.h"

std::string u32string_to_std_string(const std::u32string& u32str)
{
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> utf32_converter;
    return utf32_converter.to_bytes(u32str);
}

std::string uint32_to_hex_string(uint32_t value)
{
    std::stringstream ss;
    ss << std::hex << value;
    return ss.str();
}

std::string bytesToHex(uint8_t* _bytes__, size_t _size__)
{
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (size_t i = 0; i < _size__; ++i) {
        ss << std::setw(2) << static_cast<int>(_bytes__[i]);
    }
    return ss.str();
}

std::vector<uint8_t> Base64_decode(const std::string& encoded_string)
{
    static const std::string base64_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                            "abcdefghijklmnopqrstuvwxyz"
                                            "0123456789+/";

    size_t in_len = encoded_string.size();
    if (in_len % 4 != 0) {
        throw std::runtime_error("Input data size is not a multiple of 4");
    }

    int padding = 0;
    if (in_len >= 2) {
        if (encoded_string[in_len - 1] == '=') {
            padding++;
        }
        if (encoded_string[in_len - 2] == '=') {
            padding++;
        }
    }

    std::vector<uint8_t> ret;
    ret.reserve((in_len / 4) * 3);

    for (size_t i = 0; i < in_len; i += 4) {
        uint8_t indices[4];
        for (size_t j = 0; j < 4; j++) {
            char c = encoded_string[i + j];
            if (c == '=') {
                indices[j] = 0;
            } else {
                size_t pos = base64_chars.find(c);
                indices[j] = static_cast<uint8_t>(pos);
            }
        }
        ret.push_back((indices[0] << 2) | (indices[1] >> 4));
        if (encoded_string[i + 2] != '=') ret.push_back(((indices[1] & 0xF) << 4) | (indices[2] >> 2));
        if (encoded_string[i + 3] != '=') ret.push_back(((indices[2] & 0x3) << 6) | indices[3]);
    }

    return ret;
}

std::string Base64_encode(const std::vector<uint8_t>& input)
{
    static const std::string base64_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                            "abcdefghijklmnopqrstuvwxyz"
                                            "0123456789+/";
    std::string ret;
    int i = 0;
    int j = 0;
    uint8_t char_array_3[3];
    uint8_t char_array_4[4];
    size_t in_len = input.size();

    ret.reserve((in_len / 3 + (in_len % 3 > 0)) * 4);
    while (in_len--) {
        char_array_3[i++] = input[j++];
        if (i == 3) {
            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
            char_array_4[3] = char_array_3[2] & 0x3f;

            for (i = 0; (i < 4); i++) {
                ret += base64_chars[char_array_4[i]];
            }
            i = 0;
        }
    }

    if (i) {
        for (j = i; j < 3; j++) {
            char_array_3[j] = '\0';
        }

        char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
        char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
        char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);

        for (j = 0; (j < i + 1); j++) {
            ret += base64_chars[char_array_4[j]];
        }
        while ((i++ < 3)) {
            ret += '=';
        }
    }
    return ret;
}

std::u32string to_u32string(std::string_view sv)
{
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> converter;
    return converter.from_bytes(sv.data(), sv.data() + sv.size());
}

std::string getFilenameFromPath(const char* filePath)
{
    std::string path(filePath);
    size_t lastSlash = path.find_last_of("/\\");
    if (lastSlash == std::string::npos) {
        return path; // No slash found, so it's just the filename
    } else {
        return path.substr(lastSlash + 1);
    }
}

void GErrorLog(std::string msg)
{
    const int fixedStringLen = 80;
    auto now = std::chrono::system_clock::now();
    time_t currentTime = std::chrono::system_clock::to_time_t(now);
    char timeBuffer[fixedStringLen];
    strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&currentTime));
    std::cout << "[" << "ERROR" << "] [ #" << std::this_thread::get_id() << "] " << timeBuffer << "  :" << ">>>>"
              << (msg) << std::endl;
    std::cout.flush();
}
