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

#include "basictypes/StringBuilder.h"

StringBuilder::StringBuilder()
{
    value.reserve(INIT_SIZE);
}

StringBuilder::StringBuilder(std::string str)
{
    value.reserve(str.length() + 16);
    this->append(str);
}

StringBuilder::StringBuilder(int size)
{
    value.reserve(size);
}

StringBuilder::~StringBuilder()
{
    value.clear();
}

StringBuilder* StringBuilder::append(String* input)
{
    if (input == nullptr) {
        return append("null");
    }
    const auto val = input->getValue();
    if (val.empty()) return this; // No need to append empty data

    // Directly expand the vector and append the content
    value.insert(value.end(), val.begin(), val.end());
    return this;
}

StringBuilder* StringBuilder::append(int32_t input)
{
    std::string num2Str = std::to_string(input);
    if (num2Str.empty()) return this; // No need to append empty data

    // Directly expand the vector and append the content
    value.insert(value.end(), num2Str.begin(), num2Str.end());
    return this;
}

StringBuilder* StringBuilder::append(int64_t input)
{
    std::string num2Str = std::to_string(input);
    if (num2Str.empty()) return this; // No need to append empty data

    // Directly expand the vector and append the content
    value.insert(value.end(), num2Str.begin(), num2Str.end());
    return this;
}

int32_t StringBuilder::length()
{
    return value.size();
}

StringBuilder* StringBuilder::deleteCharAt(int32_t idx)
{
    if (idx >= 0 && idx < value.size()) {
        value.erase(value.begin() + idx);
    } else {
        THROW_LOGIC_EXCEPTION("index of bounds");
    }
    return this;
}

StringBuilder* StringBuilder::append(Object* input)
{
    if (input == nullptr) {
        return append("null");
    }
    auto value = input->toString();
    return this->append(value);
}

StringBuilder* StringBuilder::append(const std::string& input)
{
    if (input.empty()) return this; // No need to append empty data

    // Directly expand the vector and append the content
    value.insert(value.end(), input.begin(), input.end());
    return this;
}

// std::unique_ptr<String> StringBuilder::toStringUniquePtr() {
//    return std::make_unique<String>(value.data(), value.size());
// }

std::string StringBuilder::toString()
{
    return std::string(value.data(), value.size());
}
