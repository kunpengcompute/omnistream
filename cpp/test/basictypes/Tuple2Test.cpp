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
#include "basictypes/Tuple2.h"
#include "basictypes/String.h"

using namespace std;

TEST(Tuple2Test, Constructor_Default)
{
    Tuple2 tuple;
    EXPECT_EQ(tuple.GetF0(), nullptr);
    EXPECT_EQ(tuple.GetF1(), nullptr);
}

TEST(Tuple2Test, Constructor_WithObjects)
{
    String obj1("f0");
    String obj2("f1");

    Tuple2 tuple(&obj1, &obj2);

    EXPECT_EQ(tuple.GetF0(), &obj1);
    EXPECT_EQ(tuple.GetF1(), &obj2);
}

TEST(Tuple2Test, Destructor_NotClone)
{
    String obj1("f0");
    String obj2("f1");

    Tuple2 tuple(&obj1, &obj2);
    // Since isClone is false, the objects should not be deleted
    EXPECT_FALSE(tuple.isCloned());
}

TEST(Tuple2Test, SetF0)
{
    String obj1("f0");
    String obj2("f1");

    Tuple2 tuple(&obj1, &obj2);
    String newObj("newF0");
    tuple.SetF0(&newObj);

    EXPECT_EQ(tuple.GetF0(), &newObj);
    EXPECT_EQ(obj1.getRefCountNumber(), 1);
    EXPECT_EQ(newObj.getRefCountNumber(), 2);
}

TEST(Tuple2Test, GetF0)
{
    String obj1("f0");
    String obj2("f1");

    Tuple2 tuple(&obj1, &obj2);
    EXPECT_EQ(tuple.GetF0(), &obj1);
}

TEST(Tuple2Test, SetF1)
{
    String obj1("f0");
    String obj2("f1");

    Tuple2 tuple(&obj1, &obj2);
    String newObj("newF1");
    tuple.SetF1(&newObj);

    EXPECT_EQ(tuple.GetF1(), &newObj);
    EXPECT_EQ(obj2.getRefCountNumber(), 1);
    EXPECT_EQ(newObj.getRefCountNumber(), 2);
}

TEST(Tuple2Test, GetF1)
{
    String obj1("f0");
    String obj2("f1");

    Tuple2 tuple(&obj1, &obj2);
    EXPECT_EQ(tuple.GetF1(), &obj2);
}

TEST(Tuple2Test, HashCode)
{
    String obj1("f0");
    String obj2("f1");

    Tuple2 tuple(&obj1, &obj2);
    int autualHash = tuple.hashCode();
    int expectedHash = obj1.hashCode() * 31 + obj2.hashCode();
    EXPECT_EQ(autualHash, expectedHash);
}

TEST(Tuple2Test, Equals_SameValues)
{
    String obj1("f0");
    String obj2("f1");

    Tuple2 tuple1(&obj1, &obj2);
    Tuple2 tuple2(&obj1, &obj2);

    EXPECT_TRUE(tuple1.equals(&tuple2));
}

TEST(Tuple2Test, Equals_DifferentValues)
{
    String obj1("f0");
    String obj2("f1");
    String obj3("f2");

    Tuple2 tuple1(&obj1, &obj2);
    Tuple2 tuple2(&obj1, &obj3);

    EXPECT_FALSE(tuple1.equals(&tuple2));
}

TEST(Tuple2Test, Clone)
{
    String obj1("f0");
    String obj2("f1");

    Tuple2 tuple(&obj1, &obj2);
    Object* cloned = tuple.clone();
    Tuple2* clonedTuple = dynamic_cast<Tuple2*>(cloned);

    EXPECT_NE(cloned, &tuple);
    EXPECT_EQ(clonedTuple->GetF0()->toString(), tuple.GetF0()->toString());
    EXPECT_EQ(clonedTuple->GetF1()->toString(), tuple.GetF1()->toString());

    delete clonedTuple;
}

TEST(Tuple2Test, StaticOf)
{
    String* obj1 = new String("f0");
    String* obj2 = new String("f1");
    Tuple2* tuple = Tuple2::of(obj1, obj2);

    // Since Tuple2::of sets isClone to true, the objects should be deleted
    EXPECT_EQ(tuple->GetF0(), obj1);
    EXPECT_EQ(tuple->GetF1(), obj2);

    tuple->putRefCount();
    obj1->putRefCount();
    obj2->putRefCount();
}
