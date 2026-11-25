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

#ifndef FLINK_TNEL_CLASS_H
#define FLINK_TNEL_CLASS_H
#include "Object.h"
#include "java_lang_ClassLoader.h"
#include "emhash7.hpp"

class Class : public Object {
public:
    using Creator = std::function<Object*()>;
    using Getter = std::function<Object*(Object*)>;
    using Setter = std::function<void(Object*, Object*)>;

    std::string name; // such as org.example.PVMVLogType; java.lang.Long

    std::map<std::string, std::string> fieldTypes_;

    explicit Class(Creator creator);

    Object* newInstance() const;

    void addField(const std::string& name, Getter getter, Setter setter);

    void set(Object* obj, const std::string& field, Object* value) const;

    Object* get(Object* obj, const std::string& field) const;

    Class();

    explicit Class(const std::string& str);

    explicit Class(std::string&& str) noexcept;

    std::string_view getName();

    ClassLoader* getClassLoader();

    ~Class() override;

private:
    Creator creator_;
    emhash7::HashMap<std::string, Getter> getters_;
    emhash7::HashMap<std::string, Setter> setters_;
};
#endif // FLINK_TNEL_CLASS_H
