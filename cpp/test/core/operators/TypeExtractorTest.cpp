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

#include <gtest/gtest.h>
#include "core/typeinfo/TypeExtractor.h"
#include "core/typeinfo/typeconstants.h"

TEST(TypeExtractorTest, BaseClassTest)
{
    auto basicTypeInformation = TypeExtractor::CreateTypeInfo(ClassRegistry::instance().getClass("java_lang_String"));
    ASSERT_EQ(basicTypeInformation->name(), TYPE_NAME_STRING_SERIALIZER);
    basicTypeInformation = TypeExtractor::CreateTypeInfo(ClassRegistry::instance().getClass("java_lang_Double"));
    ASSERT_EQ(basicTypeInformation->name(), TYPE_NAME_DOUBLE_SERIALIZER);
    basicTypeInformation = TypeExtractor::CreateTypeInfo(ClassRegistry::instance().getClass("java_lang_Long"));
    ASSERT_EQ(basicTypeInformation->name(), TYPE_NAME_LONG_SERIALIZER);
    basicTypeInformation = TypeExtractor::CreateTypeInfo(ClassRegistry::instance().getClass("java_math_BigInteger"));
    ASSERT_EQ(basicTypeInformation->name(), TYPE_NAME_BIGINT_SERIALIZER);
}

TEST(TypeExtractorTest, MapTest)
{
    auto typeInformation = TypeExtractor::CreateTypeInfo(
        ClassRegistry::instance().getClass("java_util_Map<java_lang_String,java_lang_String>"));

    MapTypeInfo* derivedObj = dynamic_cast<MapTypeInfo*>(typeInformation);
    ASSERT_NE(derivedObj, nullptr);

    ASSERT_EQ(
        derivedObj->name(), std::string(TYPE_NAME_STRING_SERIALIZER) + " " + std::string(TYPE_NAME_STRING_SERIALIZER));
}

TEST(TypeExtractorTest, PojoTest)
{
    auto typeInformation = TypeExtractor::CreateTypeInfo(ClassRegistry::instance().getClass("User"));

    PojoTypeInfo* derivedObj = dynamic_cast<PojoTypeInfo*>(typeInformation);
    ASSERT_NE(derivedObj, nullptr);

    auto fieldInfo1 = derivedObj->getTypeAt(0);
    ASSERT_EQ(fieldInfo1->name(), TYPE_NAME_LONG_SERIALIZER);

    auto fieldInfo2 = derivedObj->getTypeAt(1);
    ASSERT_EQ(fieldInfo2->name(), TYPE_NAME_STRING_SERIALIZER);
}
