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

#include <memory>
#include <vector>

#include "table/runtime/operators/window/assigners/TumblingWindowAssigner.h"

namespace {

void AssertWindowsEq(const std::vector<TimeWindow>& actual, const std::vector<TimeWindow>& expected)
{
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i], expected[i]) << "at index " << i;
    }
}

} // namespace

TEST(TumblingWindowAssignerTest, WindowAssignmentMatchesFlink)
{
    TumblingWindowAssigner assigner(5000, 0, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, 0L), {TimeWindow(0, 5000)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 4999L), {TimeWindow(0, 5000)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 5000L), {TimeWindow(5000, 10000)});
}

TEST(TumblingWindowAssignerTest, WindowAssignmentWithOffsetMatchesFlink)
{
    TumblingWindowAssigner assigner(5000, 100, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, 100L), {TimeWindow(100, 5100)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 5099L), {TimeWindow(100, 5100)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 5100L), {TimeWindow(5100, 10100)});
}

TEST(TumblingWindowAssignerTest, WindowAssignmentForNegativeTimestampMatchesFlink)
{
    TumblingWindowAssigner assigner(5000, 0, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, -1L), {TimeWindow(-5000, 0)});
    AssertWindowsEq(assigner.assignWindows(nullptr, -5000L), {TimeWindow(-5000, 0)});
    AssertWindowsEq(assigner.assignWindows(nullptr, -5001L), {TimeWindow(-10000, -5000)});
}

TEST(TumblingWindowAssignerTest, WindowAssignmentWithOffsetForNegativeTimestampMatchesFlink)
{
    TumblingWindowAssigner assigner(5000, 100, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, 99L), {TimeWindow(-4900, 100)});
    AssertWindowsEq(assigner.assignWindows(nullptr, -4900L), {TimeWindow(-4900, 100)});
    AssertWindowsEq(assigner.assignWindows(nullptr, -4901L), {TimeWindow(-9900, -4900)});
}

TEST(TumblingWindowAssignerTest, RejectsInvalidSizeAndKeepsTimeMode)
{
    EXPECT_THROW(TumblingWindowAssigner(-1, 0, true), std::logic_error);

    TumblingWindowAssigner assigner(5000, 0, true);
    EXPECT_TRUE(assigner.isEventTime());

    std::unique_ptr<TumblingWindowAssigner> eventTime(assigner.withEventTime());
    std::unique_ptr<TumblingWindowAssigner> processingTime(assigner.withProcessingTime());
    EXPECT_TRUE(eventTime->isEventTime());
    EXPECT_FALSE(processingTime->isEventTime());
}
