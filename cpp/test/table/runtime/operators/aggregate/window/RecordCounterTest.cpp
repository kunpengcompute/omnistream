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
#include "table/runtime/operators/aggregate/window/RecordCounter.h"
#include "table/data/JoinedRowData.h"

TEST(RecordCounterTest, AccumulationRecordCounterTest)
{
    JoinedRowData joinedRowData(nullptr, nullptr);
    auto accumulationRecordCounter = RecordCounter::of(-1);

    EXPECT_TRUE(accumulationRecordCounter->recordCountIsZero(nullptr));
    EXPECT_FALSE(accumulationRecordCounter->recordCountIsZero(&joinedRowData));
}

TEST(RecordCounterTest, RetractionRecordCounterTest)
{
    JoinedRowData joinedRowData(nullptr, nullptr);
    auto retractionRecordCounter = RecordCounter::of(0);

    EXPECT_TRUE(retractionRecordCounter->recordCountIsZero(nullptr));
    // EXPECT_FALSE(retractionRecordCounter->recordCountIsZero(&joinedRowData));
}
