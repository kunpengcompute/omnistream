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

#ifndef CPP_SERIALIZATIONSCHEMA_H
#define CPP_SERIALIZATIONSCHEMA_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include "basictypes/Object.h"

class SerializationSchema {
public:
    // 虚析构函数，确保正确释放派生类对象
    virtual ~SerializationSchema() = default;

    virtual std::vector<uint8_t> serialize(Object* element) = 0;
};

#endif // CPP_SERIALIZATIONSCHEMA_H
