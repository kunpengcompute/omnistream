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

#include "BasicTypeInfo.h"

BasicTypeInfo* BasicTypeInfo::getBasicTypeInfo(const std::string typeName_)
{
    if (typeName_ == TYPE_NAME_STRING_SERIALIZER) {
        return new BasicTypeInfo(typeName_);
    } else if (typeName_ == TYPE_NAME_BIGINT_SERIALIZER) {
        return new BasicTypeInfo(typeName_);
    } else if (typeName_ == TYPE_NAME_LONG_SERIALIZER) {
        return new BasicTypeInfo(typeName_);
    } else if (typeName_ == TYPE_NAME_DOUBLE_SERIALIZER) {
        return new BasicTypeInfo(typeName_);
    } else if (typeName_ == TYPE_NAME_INT_SERIALIZER) {
        return new BasicTypeInfo(typeName_);
    } else if (typeName_ == TYPE_NAME_VOID_SERIALIZER) {
        return new BasicTypeInfo(typeName_);
    }
    return nullptr;
}

BasicTypeInfo* BasicTypeInfo::getBasicTypeInfoByClass(std::string className)
{
    if (className == "java_lang_String" || className == "String") {
        return new BasicTypeInfo(TYPE_NAME_STRING_SERIALIZER);
    } else if (className == "java_math_BigInteger" || className == "BigInteger") {
        return new BasicTypeInfo(TYPE_NAME_BIGINT_SERIALIZER);
    } else if (className == "java_lang_Long" || className == "long") {
        return new BasicTypeInfo(TYPE_NAME_LONG_SERIALIZER);
    } else if (className == "java_lang_Integer" || className == "integer") {
        return new BasicTypeInfo(TYPE_NAME_INT_SERIALIZER);
    } else if (className == "java_lang_Double" || className == "double") {
        return new BasicTypeInfo(TYPE_NAME_DOUBLE_SERIALIZER);
    }
    return nullptr;
}

// Typeinfo is Object.Multithreading can easily lead to reference technology exceptions.
thread_local auto Unique_Basici_String = std::make_unique<BasicTypeInfo>(TYPE_NAME_STRING_SERIALIZER);
thread_local BasicTypeInfo* BasicTypeInfo::STRING_TYPE_INFO = Unique_Basici_String.get();
thread_local auto Unique_Basici_BigInt = std::make_unique<BasicTypeInfo>(TYPE_NAME_BIGINT_SERIALIZER);
thread_local BasicTypeInfo* BasicTypeInfo::BIG_INT_TYPE_INFO = Unique_Basici_BigInt.get();
thread_local auto Unique_Basici_Long = std::make_unique<BasicTypeInfo>(TYPE_NAME_LONG_SERIALIZER);
thread_local BasicTypeInfo* BasicTypeInfo::LONG_TYPE_INFO = Unique_Basici_Long.get();
thread_local auto Unique_Basici_Int = std::make_unique<BasicTypeInfo>(TYPE_NAME_INT_SERIALIZER);
thread_local BasicTypeInfo* BasicTypeInfo::INT_TYPE_INFO = Unique_Basici_Int.get();
thread_local auto Unique_Basici_Double = std::make_unique<BasicTypeInfo>(TYPE_NAME_DOUBLE_SERIALIZER);
thread_local BasicTypeInfo* BasicTypeInfo::DOUBLE_TYPE_INFO = Unique_Basici_Double.get();
