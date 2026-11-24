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

#ifndef FLINK_TNEL_STRINGBUILDER_H
#define FLINK_TNEL_STRINGBUILDER_H

#include "basictypes/String.h"

class StringBuilder : public Object {
public:
    StringBuilder();

    StringBuilder(std::string str);

    StringBuilder(int size);

    ~StringBuilder();

    // todo returnType is different from java
    StringBuilder* append(String* input);

    StringBuilder* append(int32_t input);

    StringBuilder* append(int64_t input);

    StringBuilder* append(Object* input);

    // this contains copy op of std::string
    StringBuilder* append(const std::string& input);

    //    std::unique_ptr<String> toStringUniquePtr();

    std::string toString();

    int32_t length();

    StringBuilder* deleteCharAt(int32_t idx);
    // expand other append(), such as append(int value)
    static constexpr int INIT_SIZE = 16;

private:
    std::vector<char> value;
};

#endif // FLINK_TNEL_STRINGBUILDER_H
