/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "table/runtime/operators/window/assigners/CumulativeWindowAssigner.h"

namespace {

void AssertWindowsEq(const std::vector<TimeWindow>& actual, const std::vector<TimeWindow>& expected)
{
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i], expected[i]) << "at index " << i;
    }
}

} // namespace

TEST(CumulativeWindowAssignerTest, WindowAssignmentMatchesFlink)
{
    CumulativeWindowAssigner assigner(3000, 1000, 0, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, 20L), {TimeWindow(0, 1000), TimeWindow(0, 2000), TimeWindow(0, 3000)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 999L), {TimeWindow(0, 1000), TimeWindow(0, 2000), TimeWindow(0, 3000)});
    // On the step boundary only the already elapsed cumulative windows remain.
    AssertWindowsEq(assigner.assignWindows(nullptr, 1000L), {TimeWindow(0, 2000), TimeWindow(0, 3000)});
    AssertWindowsEq(
        assigner.assignWindows(nullptr, 3999L),
        {TimeWindow(3000, 4000), TimeWindow(3000, 5000), TimeWindow(3000, 6000)});
}

TEST(CumulativeWindowAssignerTest, WindowAssignmentWithOffsetMatchesFlink)
{
    CumulativeWindowAssigner assigner(3000, 1000, 100, true);

    AssertWindowsEq(
        assigner.assignWindows(nullptr, 100L),
        {TimeWindow(100, 1100), TimeWindow(100, 2100), TimeWindow(100, 3100)});
    AssertWindowsEq(
        assigner.assignWindows(nullptr, 1099L),
        {TimeWindow(100, 1100), TimeWindow(100, 2100), TimeWindow(100, 3100)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 1100L), {TimeWindow(100, 2100), TimeWindow(100, 3100)});
    AssertWindowsEq(
        assigner.assignWindows(nullptr, 3100L),
        {TimeWindow(3100, 4100), TimeWindow(3100, 5100), TimeWindow(3100, 6100)});
}

TEST(CumulativeWindowAssignerTest, WindowAssignmentForNegativeTimestampMatchesFlink)
{
    CumulativeWindowAssigner assigner(3000, 1000, 0, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, -1L), {TimeWindow(-3000, 0)});
    AssertWindowsEq(assigner.assignWindows(nullptr, -1000L), {TimeWindow(-3000, 0)});
    AssertWindowsEq(assigner.assignWindows(nullptr, -1001L), {TimeWindow(-3000, -1000), TimeWindow(-3000, 0)});
}

TEST(CumulativeWindowAssignerTest, WindowAssignmentWithNegativeOffsetMatchesFlink)
{
    CumulativeWindowAssigner assigner(3000, 1000, -100, true);

    AssertWindowsEq(
        assigner.assignWindows(nullptr, 99L),
        {TimeWindow(-100, 900), TimeWindow(-100, 1900), TimeWindow(-100, 2900)});
    AssertWindowsEq(
        assigner.assignWindows(nullptr, -100L),
        {TimeWindow(-100, 900), TimeWindow(-100, 1900), TimeWindow(-100, 2900)});
    AssertWindowsEq(assigner.assignWindows(nullptr, -101L), {TimeWindow(-3100, -100)});
}

TEST(CumulativeWindowAssignerTest, RejectsInvalidParametersAndKeepsTimeMode)
{
    EXPECT_THROW(CumulativeWindowAssigner(0, 1000, 0, true), std::logic_error);
    EXPECT_THROW(CumulativeWindowAssigner(-3000, 1000, 0, true), std::logic_error);
    EXPECT_THROW(CumulativeWindowAssigner(3000, 0, 0, true), std::logic_error);
    // maxSize must be an integral multiple of step.
    EXPECT_THROW(CumulativeWindowAssigner(3000, 2000, 0, true), std::logic_error);

    CumulativeWindowAssigner assigner(3000, 1000, 0, true);
    EXPECT_TRUE(assigner.isEventTime());

    std::unique_ptr<CumulativeWindowAssigner> eventTime(assigner.withEventTime());
    std::unique_ptr<CumulativeWindowAssigner> processingTime(assigner.withProcessingTime());
    EXPECT_TRUE(eventTime->isEventTime());
    EXPECT_FALSE(processingTime->isEventTime());
}
