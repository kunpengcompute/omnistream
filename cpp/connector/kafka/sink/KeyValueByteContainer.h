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

#ifndef FLINK_BENCHMARK_KEYVALUEBYTECONTAINER_H
#define FLINK_BENCHMARK_KEYVALUEBYTECONTAINER_H

#include <vector>
#include <memory>
#include <stdexcept>
#include <cstdint>
#include <libboundscheck/include/securec.h>

class KeyValueByteContainer {
public:
    // 公有成员变量（按需求可直接访问）
    char* key;
    char* value;
    size_t keyLen;
    size_t valueLen;

    // 默认构造函数
    KeyValueByteContainer() = default;

    // 参数化构造函数（使用移动语义优化性能）
    KeyValueByteContainer(char* keyData, std::string_view valueData) : key(keyData)
    {
        valueLen = valueData.size();
        value = reinterpret_cast<char*>(malloc(valueLen)); // free by rdkafka
        if (!value) {
            throw std::runtime_error("allocate memory failed!");
        }
        memcpy_s(value, valueLen, valueData.data(), valueLen);
    }

    ~KeyValueByteContainer()
    {
    }

    // （可选）获取数据大小的便捷方法
    [[nodiscard]] size_t keySize() const noexcept
    {
        return strlen(value);
    }

    [[nodiscard]] size_t valueSize() const noexcept
    {
        return valueLen;
    }
};

#endif // FLINK_BENCHMARK_KEYVALUEBYTECONTAINER_H
