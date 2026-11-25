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
#include "basictypes/Long.h"
#include "basictypes/String.h"
#include "core/operators/test_utils/Mocks.h"

using namespace std;

TEST(LongTest, ValueConstructor)
{
    Long longObj(12345);
    EXPECT_EQ(longObj.getValue(), 12345);
}

TEST(LongTest, Destructor)
{
    Long* longObj = new Long(12345);
    delete longObj;
}

TEST(LongTest, GetValue)
{
    Long longObj(12345);
    EXPECT_EQ(longObj.getValue(), 12345);
}

TEST(LongTest, SetValue)
{
    Long longObj;
    longObj.setValue(54321);
    EXPECT_EQ(longObj.getValue(), 54321);
}

TEST(LongTest, HashCode)
{
    Long longObj(12345);
    int hash = longObj.hashCode();
    EXPECT_EQ(hash, static_cast<int>(12345 ^ (static_cast<uint64_t>(12345) >> 32)));
}

TEST(LongTest, Equals_SameValue)
{
    Long longObj1(12345);
    Long longObj2(12345);

    EXPECT_TRUE(longObj1.equals(&longObj2));
}

TEST(LongTest, Equals_DifferentValue)
{
    Long longObj1(12345);
    Long longObj2(54321);

    EXPECT_FALSE(longObj1.equals(&longObj2));
}

TEST(LongTest, Equals_DifferentType)
{
    Long longObj(12345);
    MockLongObject obj(12345);

    EXPECT_TRUE(longObj.equals(&obj));
}

TEST(LongTest, ToString)
{
    Long longObj(12345);
    EXPECT_EQ(longObj.toString(), "12345");
}

TEST(LongTest, Clone)
{
    Long longObj(12345);
    Object* cloned = longObj.clone();
    Long* clonedLong = dynamic_cast<Long*>(cloned);

    EXPECT_NE(cloned, &longObj);
    EXPECT_EQ(clonedLong->getValue(), longObj.getValue());

    delete clonedLong;
}

TEST(LongTest, ValueOf_String)
{
    std::string s = "12345";
    String str(s);
    Long* longObj = Long::valueOf(&str);
    EXPECT_EQ(longObj->getValue(), 12345);

    delete longObj;
}

TEST(LongTest, ValueOf_String_Invalid)
{
    String str("abcde");
    EXPECT_THROW(Long::valueOf(&str), std::out_of_range);
}

TEST(LongTest, ValueOf_Int64)
{
    Long* longObj = Long::valueOf(12345);
    EXPECT_EQ(longObj->getValue(), 12345);

    delete longObj;
}
