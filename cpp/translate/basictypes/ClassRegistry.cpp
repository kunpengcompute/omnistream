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

#include "basictypes/ClassRegistry.h"

#include "core/typeinfo/typeconstants.h"
#include "basictypes/String.h"
#include "runtime/state/VoidNamespace.h"

ClassRegistry& ClassRegistry::instance()
{
    static ClassRegistry registry;
    return registry;
}

ClassRegistry::~ClassRegistry()
{
    for (auto it = classes_.begin(); it != classes_.end(); ++it) {
        it->second->putRefCount();
    }
    classes_.clear();
}

void ClassRegistry::registerClass(const std::string& name, Class* clazz)
{
    classes_[name] = clazz;
}

Class* ClassRegistry::getClass(const std::string& name)
{
    auto it = classes_.find(name);
    if (it != classes_.end()) {
        return it->second;
    }
    Class* newClass = new Class(name);
    classes_[name] = newClass;
    return newClass;
}

Class* ClassRegistry::newClass(const std::string& name)
{
    if (strcasecmp(name.c_str(), TYPE_NAME_STRING) == 0 || name == TYPE_NAME_STRING_CLASS ||
        name == TYPE_NAME_STRING_CLASS_LINE) {
        return new Class([]() -> Object* { return new String(); });
    } else if (
        strcasecmp(name.c_str(), TYPE_NAME_VOID_NAMESPACE) == 0 || name == TYPE_NAME_VOID_NAMESPACE_CLASS ||
        name == TYPE_NAME_VOID_NAMESPACE_CLASS_LINE) {
        return new Class([]() -> Object* { return new VoidNamespace(); });
    } else {
        return new Class(name);
    }
}

bool ClassRegistry::hasRegistry(const std::string& name)
{
    auto it = classes_.find(name);
    if (it != classes_.end()) {
        return true;
    }
    return false;
}
