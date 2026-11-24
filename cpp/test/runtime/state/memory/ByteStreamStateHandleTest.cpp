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
#include "runtime/state/memory/ByteStreamStateHandle.h"

// Test Seeking and getting position
TEST(ByteStreamStateHandleTest, StreamSeekAndPos)
{
    std::vector<uint8_t> data = {34, 25, 22, 66, 88, 54};
    ByteStreamStateHandle handle("name", data);

    for (int i = static_cast<int>(data.size()); i >= 0; --i) {
        auto in = handle.OpenInputStream();
        in->Seek(i);
        EXPECT_EQ(i, in->GetPos());

        if (i < static_cast<int>(data.size())) {
            EXPECT_EQ(data[i], in->Read());
            EXPECT_EQ(i + 1, in->GetPos());
        } else {
            EXPECT_EQ(-1, in->Read());
            EXPECT_EQ(i, in->GetPos());
        }
    }

    auto in = handle.OpenInputStream();
    in->Seek(data.size());

    EXPECT_EQ(-1, in->Read());
    EXPECT_EQ(-1, in->Read());
    EXPECT_EQ(-1, in->Read());

    EXPECT_EQ(data.size(), in->GetPos());
}

// Test Seeking out of bounds
TEST(ByteStreamStateHandleTest, StreamSeekOutOfBounds)
{
    int len = 10;
    ByteStreamStateHandle handle("name", std::vector<uint8_t>(len));

    auto in = handle.OpenInputStream();
    EXPECT_THROW(in->Seek(-2), std::ios_base::failure);

    in = handle.OpenInputStream();
    EXPECT_THROW(in->Seek(len + 1), std::ios_base::failure);

    in = handle.OpenInputStream();
    EXPECT_THROW(in->Seek(static_cast<long long>(INT32_MAX) + 100), std::ios_base::failure);
}

// Test bulk Read behavior
TEST(ByteStreamStateHandleTest, BulkRead)
{
    std::vector<uint8_t> data = {34, 25, 22, 66};
    ByteStreamStateHandle handle("name", data);
    int targetLen = 8;

    for (int start = 0; start < static_cast<int>(data.size()); ++start) {
        for (int num = 0; num < targetLen; ++num) {
            auto in = handle.OpenInputStream();
            in->Seek(start);

            std::vector<uint8_t> target(targetLen, 0);
            int Read = in->Read(target, targetLen - num, num);

            int expectedRead = std::min(num, static_cast<int>(data.size()) - start);
            EXPECT_EQ(expectedRead, Read);

            for (int i = 0; i < Read; ++i) {
                EXPECT_EQ(data[start + i], static_cast<uint8_t>(target[targetLen - num + i]));
            }

            int newPos = start + Read;
            EXPECT_EQ(newPos, in->GetPos());
            int expectedNextByte = (newPos < static_cast<int>(data.size())) ? data[newPos] : -1;
            EXPECT_EQ(expectedNextByte, in->Read());
        }
    }
}

// Test invalid Read ranges throw exceptions
TEST(ByteStreamStateHandleTest, BulkReadIndexOutOfBounds)
{
    ByteStreamStateHandle handle("name", std::vector<uint8_t>(10));

    auto check_invalid = [&](int off, int len) {
        auto in = handle.OpenInputStream();
        std::vector<uint8_t> buffer(10);
        EXPECT_THROW(in->Read(buffer, off, len), std::out_of_range);
    };

    check_invalid(-1, 5);        // negative offset
    check_invalid(10, 5);        // offset overflow
    check_invalid(0, -2);        // negative length
    check_invalid(5, 6);         // overflow beyond buffer
    check_invalid(5, INT32_MAX); // integer overflow
}
