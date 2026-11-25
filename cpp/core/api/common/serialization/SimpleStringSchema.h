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

#ifndef CPP_SIMPLESTRINGSCHEMA_H
#define CPP_SIMPLESTRINGSCHEMA_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include <locale>
#include <codecvt>
#include "DeserializationSchema.h"
#include "SerializationSchema.h"
#include "basictypes/StringView.h"

class SimpleStringSchema : public DeserializationSchema, public SerializationSchema {
public:
    // 默认构造函数，使用 UTF-8 编码
    SimpleStringSchema();
    ~SimpleStringSchema() override;
    // 带字符集参数的构造函数
    explicit SimpleStringSchema(const std::string& charsetName);
    // 获取当前使用的字符集名称
    std::string_view getCharset() const;

    // 反序列化方法
    Object* deserialize(const uint8_t* message, size_t length) override;
    // 判断是否为流结束
    bool isEndOfStream(const void* nextElement) override;

    std::vector<uint8_t> serialize(Object* element) override;

private:
    std::string_view charsetName;
    String* buffer;
};

#endif // CPP_SIMPLESTRINGSCHEMA_H
