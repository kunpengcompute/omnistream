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
#include "runtime/state/VoidNamespace.h"

TEST(NamespaceTest, VoidNamespace)
{
    VoidNamespace vn1;
    VoidNamespace vn2;

    std::hash<VoidNamespace> myHash;
    std::size_t hashValue = myHash(vn1);
    EXPECT_EQ(hashValue, 99);

    std::equal_to<VoidNamespace> myEqual;
    bool isEqual = myEqual(vn1, vn2);

    EXPECT_EQ(isEqual, true);
}
