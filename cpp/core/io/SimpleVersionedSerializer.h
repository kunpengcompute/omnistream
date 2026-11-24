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

#ifndef OMNISTREAM_SIMPLEVERSIONEDSERIALIZER_H
#define OMNISTREAM_SIMPLEVERSIONEDSERIALIZER_H

#include <vector>
#include <stdexcept>
#include <memory>

template <typename E>
class SimpleVersionedSerializer {
public:
    // 虚析构函数，确保正确释放派生类对象
    virtual ~SimpleVersionedSerializer() = default;

    // 获取序列化版本
    virtual int getVersion() const = 0;

    // 序列化对象
    virtual std::vector<uint8_t> serialize(const E& obj) = 0;

    // 反序列化对象
    virtual E* deserialize(int version, std::vector<uint8_t>& serialized) = 0;
};

#endif // OMNISTREAM_SIMPLEVERSIONEDSERIALIZER_H
