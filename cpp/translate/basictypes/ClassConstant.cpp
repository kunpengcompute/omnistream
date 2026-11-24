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

#include "basictypes/ClassConstant.h"
ClassConstant& ClassConstant::getInstance()
{
    thread_local static ClassConstant* instance = new ClassConstant(); // 初始化时创建 16 个对象
    return *instance;
}

ClassConstant::ClassConstant() = default;

ClassConstant::~ClassConstant()
{
    for (auto& pair : classMap) {
        delete pair.second;
    }
    classMap.clear();
};

Class* ClassConstant::get(const std::string& str)
{
    auto it = classMap.find(str);
    if (it != classMap.end()) {
        return it->second;
    } else {
        Class* newClass = new Class(str);
        classMap[str] = newClass;
        return newClass;
    }
}
