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
#include "table/data/RowData.h"
#include "table/data/JoinedRowData.h"
#include "table/data/binary/BinaryRowData.h"

#include <iostream>

TEST(JoinedRowDataTest, ConstructorTest)
{
    JoinedRowData joinedRow(nullptr, nullptr);
    EXPECT_EQ(joinedRow.getRowKind(), RowKind::INSERT);
}

TEST(JoinedRowDataTest, GetRowKindTest)
{
    JoinedRowData row1(RowKind::INSERT, nullptr, nullptr);
    JoinedRowData row2(RowKind::DELETE, nullptr, nullptr);

    EXPECT_EQ(row1.getRowKind(), RowKind::INSERT);
    EXPECT_EQ(row2.getRowKind(), RowKind::DELETE);
}

TEST(JoinedRowDataTest, SetRowKindTest)
{
    JoinedRowData row1(RowKind::INSERT, nullptr, nullptr);
    JoinedRowData row2(RowKind::DELETE, nullptr, nullptr);

    RowKind updateAfterRowKind = RowKind::UPDATE_AFTER;
    row1.setRowKind(updateAfterRowKind);
    row2.setRowKind(updateAfterRowKind);

    EXPECT_EQ(row1.getRowKind(), RowKind::UPDATE_AFTER);
    EXPECT_EQ(row2.getRowKind(), RowKind::UPDATE_AFTER);
}

TEST(JoinedRowDataTest, GetArityTest)
{
    BinaryRowData row1(5);
    BinaryRowData row2(4);
    JoinedRowData joinedRow(&row1, &row2);

    EXPECT_EQ(joinedRow.getArity(), 9);
}

TEST(JoinedRowDataTest, ReplaceTest)
{
    JoinedRowData joinedRow(RowKind::INSERT, nullptr, nullptr);
    BinaryRowData row1(5);
    BinaryRowData row2(4);
    joinedRow.replace(&row1, &row2);

    EXPECT_EQ(joinedRow.getArity(), 9);
}

TEST(JoinedRowDataTest, SetCompactTimestamp)
{
    BinaryRowData* row1 = BinaryRowData::createBinaryRowDataWithMem(1);
    BinaryRowData* row2 = BinaryRowData::createBinaryRowDataWithMem(1);
    JoinedRowData joinedRow(row1, row2);
    TimestampData timestamp1 = TimestampData::fromEpochMillis(123L, 456);
    TimestampData timestamp2 = TimestampData::fromEpochMillis(321L, 654);
    int precision = 3;

    joinedRow.setTimestamp(0, timestamp1, precision);
    joinedRow.setTimestamp(1, timestamp2, precision);

    EXPECT_EQ(*(joinedRow.getLong(0)), 123L);
    EXPECT_EQ(*(joinedRow.getLong(1)), 321L);
}

TEST(JoinedRowDataTest, SetNonCompactTimestamp)
{
    BinaryRowData* row1 = BinaryRowData::createBinaryRowDataWithMem(2);
    BinaryRowData* row2 = BinaryRowData::createBinaryRowDataWithMem(2);
    JoinedRowData joinedRow(row1, row2);
    TimestampData timestamp1 = TimestampData::fromEpochMillis(123L, 456);
    TimestampData timestamp2 = TimestampData::fromEpochMillis(321L, 654);
    int precision = 4;

    joinedRow.setTimestamp(0, timestamp1, precision);
    joinedRow.setTimestamp(2, timestamp2, precision);

    EXPECT_EQ(*(joinedRow.getLong(0)), 123L);
    EXPECT_EQ(*(joinedRow.getInt(1)), 456);

    EXPECT_EQ(*(joinedRow.getLong(2)), 321L);
    EXPECT_EQ(*(joinedRow.getInt(3)), 654);
}

TEST(JoinedRowDataTest, SetNonCompactTimestampOutOfBoundThrowingException)
{
    BinaryRowData* row1 = BinaryRowData::createBinaryRowDataWithMem(1);
    BinaryRowData* row2 = BinaryRowData::createBinaryRowDataWithMem(1);
    JoinedRowData joinedRow(row1, row2);
    TimestampData timestamp1 = TimestampData::fromEpochMillis(123L, 456);
    TimestampData timestamp2 = TimestampData::fromEpochMillis(321L, 654);
    int precision = 4;

    EXPECT_THROW(joinedRow.setTimestamp(0, timestamp1, precision), std::logic_error);
    EXPECT_THROW(joinedRow.setTimestamp(1, timestamp2, precision), std::logic_error);
}

TEST(JoinedRowDataTest, GetCompactTimestamp)
{
    BinaryRowData* row1 = BinaryRowData::createBinaryRowDataWithMem(1);
    BinaryRowData* row2 = BinaryRowData::createBinaryRowDataWithMem(1);
    JoinedRowData joinedRow(row1, row2);
    joinedRow.setLong(0, 123L);
    joinedRow.setLong(1, 321L);
    int precision = 3;
    TimestampData timestamp1 = joinedRow.getTimestamp(0);
    TimestampData timestamp2 = joinedRow.getTimestamp(1);

    EXPECT_EQ(timestamp1.getMillisecond(), 123L);
    EXPECT_EQ(timestamp2.getMillisecond(), 321L);
}

TEST(JoinedRowDataTest, GetNonCompactTimestamp)
{
    BinaryRowData* row1 = BinaryRowData::createBinaryRowDataWithMem(2);
    BinaryRowData* row2 = BinaryRowData::createBinaryRowDataWithMem(2);
    JoinedRowData joinedRow(row1, row2);
    joinedRow.setLong(0, 123L);
    joinedRow.setInt(1, 456);
    joinedRow.setLong(2, 321L);
    joinedRow.setInt(3, 654);
    int precision = 4;
    TimestampData timestamp1 = joinedRow.getTimestampPrecise(0);
    TimestampData timestamp2 = joinedRow.getTimestampPrecise(2);

    EXPECT_EQ(timestamp1.getMillisecond(), 123L);
    EXPECT_EQ(timestamp1.getNanoOfMillisecond(), 456);
    EXPECT_EQ(timestamp2.getMillisecond(), 321L);
    EXPECT_EQ(timestamp2.getNanoOfMillisecond(), 654);
}
