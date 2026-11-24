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

#include "basictypes/Class.h"
#include <utility>

Class::Class() = default;

Class::Class(Creator creator) : creator_(std::move(creator))
{
}

Object* Class::newInstance() const
{
    return creator_();
}

void Class::addField(const std::string& name, Class::Getter getter, Class::Setter setter)
{
    getters_[name] = std::move(getter);
    setters_[name] = std::move(setter);
}

void Class::set(Object* obj, const std::string& field, Object* value) const
{
    auto it = setters_.find(field);
    if (it != setters_.end()) {
        it->second(obj, value);
    }
}

Object* Class::get(Object* obj, const std::string& field) const
{
    auto it = getters_.find(field);
    if (it == getters_.end()) {
        throw std::runtime_error("Field not found: " + field);
    }
    return it->second(obj);
}

Class::Class(const std::string& str) : name(str)
{
}

Class::Class(std::string&& str) noexcept : name(std::move(str))
{
}

std::string_view Class::getName()
{
    return name;
}

ClassLoader* Class::getClassLoader()
{
    return new ClassLoader();
};

Class::~Class() = default;
