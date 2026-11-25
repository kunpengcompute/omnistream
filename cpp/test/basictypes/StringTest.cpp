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
#include "basictypes/String.h"
#include "basictypes/Array.h"
#include "core/operators/test_utils/Mocks.h"
#include <string>

using namespace std;

TEST(StringTest, DefaultConstructor)
{
    String str;
    EXPECT_EQ(str.getSize(), 0);
}

TEST(StringTest, CharDataSizeConstructor)
{
    char data[] = "hello";
    String str(data, 5);
    EXPECT_STREQ(str.getValue().data(), "hello");
    EXPECT_EQ(str.getSize(), 5);
}

TEST(StringTest, ConstCharDataSizeConstructor)
{
    const char data[] = "hello";
    String str(data, 5);
    EXPECT_STREQ(str.getValue().data(), "hello");
    EXPECT_EQ(str.getSize(), 5);
}

TEST(StringTest, StdStringConstructor)
{
    String str("hello");
    EXPECT_STREQ(str.getValue().data(), "hello");
    EXPECT_EQ(str.getSize(), 5);
}

TEST(StringTest, StringPointerConstructor)
{
    String* original = new String("hello");
    String str(original);
    EXPECT_STREQ(str.getValue().data(), "hello");
    EXPECT_EQ(str.getSize(), 5);
    // original should be cleared
    delete original;
}

TEST(StringTest, Destructor)
{
    String* str = new String("hello");
    delete str;
}

TEST(StringTest, GetValue)
{
    String str("hello");
    EXPECT_STREQ(str.getValue().data(), "hello");
}

TEST(StringTest, GetData)
{
    String str("hello");
    EXPECT_STREQ(str.getData(), "hello");
}

TEST(StringTest, GetSize)
{
    String str("hello");
    EXPECT_EQ(str.getSize(), 5);
}

TEST(StringTest, SetValueStdString)
{
    String str;
    str.setValue(string("hello"));
    EXPECT_STREQ(str.getValue().data(), "hello");
}

TEST(StringTest, SetData)
{
    String str;
    char* data = static_cast<char*>(malloc(6));
    std::string s = "hello";
    strcpy_s(data, 6, s.c_str());
    str.setData(data);
    std::string res(str.getValue().data(), s.size());
    EXPECT_EQ(res, "hello");
    free(data);
}

TEST(StringTest, SetSize)
{
    String str("hello world");
    str.setSize(5);
    EXPECT_EQ(str.getSize(), 5);
    EXPECT_EQ(str.getValue(), "hello");
}

TEST(StringTest, HashCode)
{
    String str("hello");
    int hash = str.hashCode();
    EXPECT_NE(hash, 0);
}

TEST(StringTest, Equals)
{
    String str1("hello");
    String str2("hello");
    String str3("world");

    EXPECT_TRUE(str1.equals(&str2));
    EXPECT_FALSE(str1.equals(&str3));
}

TEST(StringTest, ToString)
{
    String str("hello");
    EXPECT_STREQ(str.toString().c_str(), "hello");
}

TEST(StringTest, Clone)
{
    String str("hello");
    Object* cloned = str.clone();
    String* clonedStr = dynamic_cast<String*>(cloned);

    EXPECT_NE(cloned, nullptr);
    EXPECT_STREQ(clonedStr->getValue().data(), "hello");
    delete clonedStr;
}

TEST(StringTest, Split)
{
    String str("hello.world.test");
    Array* result = str.split(".");

    EXPECT_EQ(result->size(), 3);
    EXPECT_EQ(result->get(0)->toString(), "hello");
    EXPECT_EQ(result->get(1)->toString(), "world");
    EXPECT_EQ(result->get(2)->toString(), "test");
    delete result;
}

TEST(StringTest, ValueOf_Null)
{
    auto result = String::valueOf(nullptr);
    EXPECT_EQ(result, nullptr);
}

TEST(StringTest, ValueOf_Object)
{
    MockStringObject obj("hello");
    auto result = String::valueOf(&obj);
    EXPECT_EQ(result->getValue(), "hello");
}
