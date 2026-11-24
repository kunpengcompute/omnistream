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

#ifndef OMNISTREAM_MEMORYSIZE_H
#define OMNISTREAM_MEMORYSIZE_H
#include "string"
#include "core/include/common.h"
#include <algorithm>

class MemorySize {
public:
    static u_int64_t parseBytes(std::string text)
    {
        auto trimmed = trim(text);
        size_t len = trimmed.length();
        size_t pos = 0;
        char current;
        while (pos < len && (current = trimmed.at(pos)) >= '0' && current <= '9') {
            pos++;
        }
        auto number = trimmed.substr(0, pos);
        auto unitStr = trim(trimmed.substr(pos));
        std::transform(unitStr.begin(), unitStr.end(), unitStr.begin(), ::tolower);
        if (number.length() == 0) {
            THROW_RUNTIME_ERROR("text does not start with a number");
        }
        uint64_t value = static_cast<uint64_t>(std::stol(number));
        auto multiplier = parseUnit(unitStr);
        auto result = value * multiplier;
        if (result / multiplier != value) {
            THROW_RUNTIME_ERROR(
                "The value '" + text + "' cannot be re represented as 64bit number of bytes (numeric overflow).");
        }
        return result;
    }

    static u_int64_t parseUnit(std::string unit)
    {
        if (unit.size() == 0) {
            return 1;
        }
        u_int64_t multiplier;
        if (unit == "b" || unit == "bytes") {
            multiplier = 1;
        } else if (unit == "k" || unit == "kb" || unit == "kibibytes") {
            multiplier = 1024;
        } else if (unit == "m" || unit == "mb" || unit == "mebibytes") {
            multiplier = 1024 * 1024;
        } else if (unit == "g" || unit == "gb" || unit == "gibibytes") {
            multiplier = 1024 * 1024 * 1024;
        } else {
            THROW_RUNTIME_ERROR("Memory size unit '" + unit + "' does not match any of the recognized units.");
        }
        return multiplier;
    }

    // 去除字符串前导空白
    static std::string ltrim(const std::string& s)
    {
        std::string str = s;
        auto it = std::find_if(str.begin(), str.end(), [](unsigned char c) { return !std::isspace(c); });
        str.erase(str.begin(), it);
        return str;
    }

    // 去除字符串尾部空白
    static std::string rtrim(const std::string& s)
    {
        std::string str = s;
        auto it = std::find_if(str.rbegin(), str.rend(), [](unsigned char c) { return !std::isspace(c); });
        str.erase(it.base(), str.end());
        return str;
    }

    // 去除前后空格
    static std::string trim(const std::string& s)
    {
        return ltrim(rtrim(s));
    }
};

#endif // OMNISTREAM_MEMORYSIZE_H
