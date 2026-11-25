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

#ifndef FLINK_TNEL_STRINGCONSTANT_H
#define FLINK_TNEL_STRINGCONSTANT_H
#include "Object.h"
#include "String.h"
#include <iostream>

class StringConstant : public Object {
public:
    using StringMap = std::unordered_map<std::string, String*>;
    StringMap stringMap;

    static StringConstant& getInstance();
    String* get(const std::string& str);

private:
    explicit StringConstant();
    ~StringConstant();

    StringConstant(const StringConstant&) = delete;
    StringConstant& operator=(const StringConstant&) = delete;
};
#endif // FLINK_TNEL_STRINGCONSTANT_H
