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

#ifndef OMNISTREAM_REFLECTMACROS_H
#define OMNISTREAM_REFLECTMACROS_H
#include "ClassRegistry.h"
#include "Class.h"
#include "Integer.h"

#define DECLARE_REFLECT_CLASS(className) \
public:                                  \
    static Class* getClass();            \
                                         \
private:                                 \
    static Class* clazz_;

#define DEFINE_REFLECT_CLASS_BEGIN(className) \
    Class* className::getClass()              \
    {                                         \
        return clazz_;                        \
    }                                         \
    Class* className::clazz_ = []() { \
    Class* c = new Class([]()->Object* { return new className(); });
#define DEFINE_REFLECT_CLASS_END(className)                 \
    c->name = #className;                                   \
    ClassRegistry::instance().registerClass(#className, c); \
    return c;                                               \
    }                                                       \
    ();

#define REGISTER_PTR_FIELD(cls, fieldName, fieldType, typeStr)                     \
    c->fieldTypes_[#fieldName] = typeStr;                                          \
    c->addField(                                                                   \
        #fieldName,                                                                \
        [](Object* obj) -> Object* {                                               \
            if (likely(static_cast<cls*>(obj)->fieldName != nullptr)) {            \
                static_cast<cls*>(obj)->fieldName->getRefCount();                  \
            }                                                                      \
            return static_cast<cls*>(obj)->fieldName;                              \
        },                                                                         \
        [](Object* obj, Object* value) {                                           \
            if (unlikely(static_cast<cls*>(obj)->fieldName != value)) {            \
                if (likely(static_cast<fieldType>(value) != nullptr)) {            \
                    value->getRefCount();                                          \
                }                                                                  \
                if (likely(static_cast<cls*>(obj)->fieldName != nullptr)) {        \
                    static_cast<cls*>(obj)->fieldName->putRefCount();              \
                }                                                                  \
                static_cast<cls*>(obj)->fieldName = static_cast<fieldType>(value); \
            }                                                                      \
        });

#define REGISTER_PRIMITIVE_FIELD(cls, fieldName, WrapperType, typeStr)                             \
    c->fieldTypes_[#fieldName] = typeStr;                                                          \
    c->addField(                                                                                   \
        #fieldName,                                                                                \
        [](Object* obj) -> Object* { return new WrapperType(static_cast<cls*>(obj)->fieldName); }, \
        [](Object* obj, Object* value) {                                                           \
            static_cast<cls*>(obj)->fieldName = static_cast<WrapperType*>(value)->value;           \
        });
#endif // OMNISTREAM_REFLECTMACROS_H
