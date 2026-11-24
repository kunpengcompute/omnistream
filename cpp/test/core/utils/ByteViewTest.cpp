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
#include <cstdint>
#include <cstring>
#include "core/utils/ByteView.h"

/**
 * 默认构造应产生空视图：size=0, data=nullptr。
 */
TEST(ByteViewTest, DefaultConstructorEmpty)
{
    ByteView view;
    EXPECT_EQ(view.size(), 0);
    EXPECT_TRUE(view.empty());
    EXPECT_EQ(view.data(), nullptr);
    EXPECT_EQ(view.begin(), nullptr);
    EXPECT_EQ(view.end(), nullptr);
}

/**
 * uint8_t 数组模板构造，按元素个数计算 size，data 非空。
 */
TEST(ByteViewTest, TemplateConstructorFromUint8)
{
    const uint8_t buf[] = {0x01, 0x02, 0x03};
    ByteView view(buf, 3);
    EXPECT_EQ(view.size(), 3);
    EXPECT_FALSE(view.empty());
    EXPECT_NE(view.data(), nullptr);
    EXPECT_EQ(view[0], 0x01);
    EXPECT_EQ(view[1], 0x02);
    EXPECT_EQ(view[2], 0x03);
}

/**
 * int8_t 数组模板构造，负值按 uint8_t 转义正确。
 */
TEST(ByteViewTest, TemplateConstructorFromInt8)
{
    const int8_t buf[] = {-1, 0, 1};
    ByteView view(buf, 3);
    EXPECT_EQ(view.size(), 3);
    EXPECT_EQ(view[0], static_cast<uint8_t>(-1));
    EXPECT_EQ(view[2], static_cast<uint8_t>(1));
}

/**
 * nullptr 入参应返回空视图，size=0, data=nullptr。
 */
TEST(ByteViewTest, NullptrYieldsEmpty)
{
    ByteView view(static_cast<const uint8_t*>(nullptr), 10);
    EXPECT_EQ(view.size(), 0);
    EXPECT_TRUE(view.empty());
    EXPECT_EQ(view.data(), nullptr);
}

/**
 * 拷贝构造/赋值：视图数据与源视图一致，且 data 指针指向同一底层缓冲区。
 */
TEST(ByteViewTest, CopySemantics)
{
    const uint8_t buf[] = {0xAA, 0xBB};
    ByteView original(buf, 2);

    // 拷贝构造
    ByteView copy(original);
    EXPECT_EQ(copy.size(), 2);
    EXPECT_EQ(copy[0], 0xAA);
    EXPECT_EQ(copy[1], 0xBB);
    EXPECT_EQ(copy.data(), original.data());

    // 拷贝赋值
    const uint8_t buf2[] = {0x22, 0x33};
    ByteView view(buf, 1);
    ByteView other(buf2, 2);
    view = other;
    EXPECT_EQ(view.size(), 2);
    EXPECT_EQ(view[0], 0x22);
    EXPECT_EQ(view.data(), other.data());
}

/**
 * 移动构造/赋值：新视图持有源视图的数据指针。
 * ByteView 为非拥有型视图，=default 移动为拷贝式搬移，
 * 源对象保留有效状态（与 std::string_view 语义一致），无需清空。
 */
TEST(ByteViewTest, MoveSemantics)
{
    const uint8_t buf[] = {0xCC, 0xDD};
    ByteView original(buf, 2);

    // 移动构造
    ByteView moved(std::move(original));
    EXPECT_EQ(moved.size(), 2);
    EXPECT_EQ(moved[0], 0xCC);
    EXPECT_EQ(moved[1], 0xDD);

    // 移动赋值
    const uint8_t buf2[] = {0x44, 0x55};
    ByteView other(buf2, 2);
    ByteView view(buf, 1);
    view = std::move(other);
    EXPECT_EQ(view.size(), 2);
    EXPECT_EQ(view[0], 0x44);
}

/**
 * begin/end 迭代器跨度与视图 size 一致。
 */
TEST(ByteViewTest, BeginEndIterator)
{
    const uint8_t buf[] = {0x10, 0x20, 0x30};
    ByteView view(buf, 3);
    EXPECT_EQ(*view.begin(), 0x10);
    const uint8_t* end = view.end();
    EXPECT_NE(end, nullptr);
    EXPECT_EQ(*(end - 1), 0x30);
    EXPECT_EQ(std::distance(view.begin(), view.end()), static_cast<std::ptrdiff_t>(3));
}

/**
 * fromBuffer 静态工厂方法产生的视图与原 buffer 内容一致，且 data 指针指向同一底层缓冲区。
 */
TEST(ByteViewTest, FromBufferStaticMethod)
{
    const int8_t buf[] = {7, 8, 9};
    ByteView view = ByteView::fromBuffer(buf, 3);
    EXPECT_EQ(view.size(), 3);
    EXPECT_EQ(view[0], 7);
    EXPECT_EQ(view[2], 9);
    EXPECT_EQ(view.data(), reinterpret_cast<const uint8_t*>(buf));
}

/**
 * 模板构造器按 sizeof(T) 计算字节长度，而非元素个数。
 */
TEST(ByteViewTest, SizeofCalculation)
{
    const int32_t buf[] = {0x01020304};
    ByteView view(buf, 1);
    EXPECT_EQ(view.size(), sizeof(int32_t));
}

/**
 * length=0 构造的视图 size 为 0，empty() 为 true。
 */
TEST(ByteViewTest, ZeroLengthView)
{
    const uint8_t buf[] = {0x42};
    ByteView view(buf, 0);
    EXPECT_EQ(view.size(), 0);
    EXPECT_TRUE(view.empty());
}
