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

#ifndef OMNISTREAM_JSONSPLITFUNCTION_H
#define OMNISTREAM_JSONSPLITFUNCTION_H

#include "NativeTableFunction.h"
#include <nlohmann/json.hpp>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

/**
 * 对标 Java 侧的 JsonSplit TableFunction。
 * 输入一个 JSON 数组字符串，输出数组中每个元素的字符串表示。
 *
 * 例如：
 *   输入: '["Room_101", "Room_102"]'
 *   输出: ["Room_101", "Room_102"]
 *
 *   输入: 'InvalidJson'
 *   输出: []  (空，解析失败静默跳过，与 Java 侧行为一致)
 */
class JsonSplitFunction : public NativeTableFunction {
public:
    std::vector<std::string> eval(const std::string& input) override
    {
        // INFO_RELEASE("JsonSplitFunction eval start, inputLen=" << input.size())
        std::vector<std::string> results;
        if (input.empty()) {
            return results;
        }
        rapidjson::Document doc;
        doc.Parse(input.data(), input.size());
        if (doc.HasParseError() || !doc.IsArray()) {
            return results;
        }
        results.reserve(doc.Size());
        for (rapidjson::SizeType i = 0; i < doc.Size(); ++i) {
            const auto& element = doc[i];
            if (element.IsString()) {
                results.emplace_back(element.GetString(), element.GetStringLength());
            } else {
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                element.Accept(writer);
                results.emplace_back(buffer.GetString(), buffer.GetSize());
            }
        }
        return results;
    }

    std::string name() const override
    {
        return "JsonSplitFunction";
    }
};

#endif // OMNISTREAM_JSONSPLITFUNCTION_H
