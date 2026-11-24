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

#ifndef OMNISTREAM_CLASSREGISTRY_H
#define OMNISTREAM_CLASSREGISTRY_H
#include <unordered_map>
#include <string>
#include <functional>
#include "Class.h"
#include "emhash7.hpp"

class ClassRegistry {
public:
    static ClassRegistry& instance();

    ~ClassRegistry();

    void registerClass(const std::string& name, Class* clazz);

    Class* getClass(const std::string& name);

    Class* newClass(const std::string& name);

    bool hasRegistry(const std::string& name);

private:
    std::unordered_map<std::string, Class*> classes_;
};
#endif // OMNISTREAM_CLASSREGISTRY_H
