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

#include "table/runtime/operators/window/assigners/SlidingWindowAssigner.h"

namespace {

constexpr int64_t MINUTE = 60 * 1000L;

void AssertWindowsEq(const std::vector<TimeWindow>& actual, const std::vector<TimeWindow>& expected)
{
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i], expected[i]) << "at index " << i;
    }
}

} // namespace

TEST(SlidingWindowAssignerTest, WindowAssignmentAndPanesMatchFlink)
{
    SlidingWindowAssigner assigner(5000, 1000, 0, true);

    AssertWindowsEq(
        assigner.assignWindows(nullptr, 0L),
        {TimeWindow(0, 5000),
         TimeWindow(-1000, 4000),
         TimeWindow(-2000, 3000),
         TimeWindow(-3000, 2000),
         TimeWindow(-4000, 1000)});
    AssertWindowsEq(
        assigner.assignWindows(nullptr, 4999L),
        {TimeWindow(4000, 9000),
         TimeWindow(3000, 8000),
         TimeWindow(2000, 7000),
         TimeWindow(1000, 6000),
         TimeWindow(0, 5000)});
    AssertWindowsEq(
        assigner.assignWindows(nullptr, 5000L),
        {TimeWindow(5000, 10000),
         TimeWindow(4000, 9000),
         TimeWindow(3000, 8000),
         TimeWindow(2000, 7000),
         TimeWindow(1000, 6000)});

    EXPECT_EQ(assigner.assignPane(nullptr, 0L), TimeWindow(0, 1000));
    EXPECT_EQ(assigner.assignPane(nullptr, 4999L), TimeWindow(4000, 5000));
    EXPECT_EQ(assigner.assignPane(nullptr, 5000L), TimeWindow(5000, 6000));

    AssertWindowsEq(
        assigner.splitIntoPanes(TimeWindow(0, 5000)),
        {TimeWindow(0, 1000),
         TimeWindow(1000, 2000),
         TimeWindow(2000, 3000),
         TimeWindow(3000, 4000),
         TimeWindow(4000, 5000)});
    AssertWindowsEq(
        assigner.splitIntoPanes(TimeWindow(3000, 8000)),
        {TimeWindow(3000, 4000),
         TimeWindow(4000, 5000),
         TimeWindow(5000, 6000),
         TimeWindow(6000, 7000),
         TimeWindow(7000, 8000)});

    EXPECT_EQ(assigner.getLastWindow(TimeWindow(4000, 5000)), TimeWindow(4000, 9000));
    EXPECT_EQ(assigner.getLastWindow(TimeWindow(2000, 3000)), TimeWindow(2000, 7000));
}

TEST(SlidingWindowAssignerTest, WindowAssignmentWithOffsetMatchesFlink)
{
    SlidingWindowAssigner assigner(5000, 1000, 100, true);

    AssertWindowsEq(
        assigner.assignWindows(nullptr, 100L),
        {TimeWindow(100, 5100),
         TimeWindow(-900, 4100),
         TimeWindow(-1900, 3100),
         TimeWindow(-2900, 2100),
         TimeWindow(-3900, 1100)});
    AssertWindowsEq(
        assigner.assignWindows(nullptr, 5099L),
        {TimeWindow(4100, 9100),
         TimeWindow(3100, 8100),
         TimeWindow(2100, 7100),
         TimeWindow(1100, 6100),
         TimeWindow(100, 5100)});
    AssertWindowsEq(
        assigner.assignWindows(nullptr, 5100L),
        {TimeWindow(5100, 10100),
         TimeWindow(4100, 9100),
         TimeWindow(3100, 8100),
         TimeWindow(2100, 7100),
         TimeWindow(1100, 6100)});

    EXPECT_EQ(assigner.assignPane(nullptr, 100L), TimeWindow(100, 1100));
    EXPECT_EQ(assigner.assignPane(nullptr, 5099L), TimeWindow(4100, 5100));
    EXPECT_EQ(assigner.assignPane(nullptr, 5100L), TimeWindow(5100, 6100));

    AssertWindowsEq(
        assigner.splitIntoPanes(TimeWindow(1100, 6100)),
        {TimeWindow(1100, 2100),
         TimeWindow(2100, 3100),
         TimeWindow(3100, 4100),
         TimeWindow(4100, 5100),
         TimeWindow(5100, 6100)});
    AssertWindowsEq(
        assigner.splitIntoPanes(TimeWindow(3100, 8100)),
        {TimeWindow(3100, 4100),
         TimeWindow(4100, 5100),
         TimeWindow(5100, 6100),
         TimeWindow(6100, 7100),
         TimeWindow(7100, 8100)});

    EXPECT_EQ(assigner.getLastWindow(TimeWindow(4100, 5100)), TimeWindow(4100, 9100));
    EXPECT_EQ(assigner.getLastWindow(TimeWindow(2100, 3100)), TimeWindow(2100, 7100));
}

TEST(SlidingWindowAssignerTest, NonIntegralMinuteHopWindowAssignmentMatchesFlink)
{
    SlidingWindowAssigner assigner(60 * MINUTE, 23 * MINUTE, 0, true);

    AssertWindowsEq(
        assigner.assignWindows(nullptr, 0L),
        {TimeWindow(0, 60 * MINUTE), TimeWindow(-23 * MINUTE, 37 * MINUTE), TimeWindow(-46 * MINUTE, 14 * MINUTE)});

    AssertWindowsEq(
        assigner.assignWindows(nullptr, 13 * MINUTE + 999L),
        {TimeWindow(0, 60 * MINUTE), TimeWindow(-23 * MINUTE, 37 * MINUTE), TimeWindow(-46 * MINUTE, 14 * MINUTE)});

    AssertWindowsEq(
        assigner.assignWindows(nullptr, 14 * MINUTE),
        {TimeWindow(0, 60 * MINUTE), TimeWindow(-23 * MINUTE, 37 * MINUTE)});

    AssertWindowsEq(
        assigner.assignWindows(nullptr, 59 * MINUTE + 999L),
        {TimeWindow(46 * MINUTE, 106 * MINUTE), TimeWindow(23 * MINUTE, 83 * MINUTE), TimeWindow(0, 60 * MINUTE)});

    AssertWindowsEq(
        assigner.assignWindows(nullptr, 60 * MINUTE),
        {TimeWindow(46 * MINUTE, 106 * MINUTE), TimeWindow(23 * MINUTE, 83 * MINUTE)});
}

TEST(SlidingWindowAssignerTest, NonIntegralMinuteHopPanesCoverWholeWindow)
{
    SlidingWindowAssigner assigner(60 * MINUTE, 23 * MINUTE, 0, true);

    EXPECT_EQ(assigner.assignPane(nullptr, 0L), TimeWindow(0, MINUTE));
    EXPECT_EQ(assigner.assignPane(nullptr, 59 * MINUTE + 999L), TimeWindow(59 * MINUTE, 60 * MINUTE));
    EXPECT_EQ(assigner.assignPane(nullptr, 60 * MINUTE), TimeWindow(60 * MINUTE, 61 * MINUTE));

    auto panes = assigner.splitIntoPanes(TimeWindow(23 * MINUTE, 83 * MINUTE));
    ASSERT_EQ(panes.size(), 60);
    EXPECT_EQ(panes.front(), TimeWindow(23 * MINUTE, 24 * MINUTE));
    EXPECT_EQ(panes.back(), TimeWindow(82 * MINUTE, 83 * MINUTE));

    EXPECT_EQ(assigner.getLastWindow(TimeWindow(82 * MINUTE, 83 * MINUTE)), TimeWindow(69 * MINUTE, 129 * MINUTE));
}

TEST(SlidingWindowAssignerTest, RejectsInvalidParametersAndKeepsTimeMode)
{
    EXPECT_THROW(SlidingWindowAssigner(-2000, 1000, 0, true), std::logic_error);
    EXPECT_THROW(SlidingWindowAssigner(2000, -1000, 0, true), std::logic_error);

    SlidingWindowAssigner assigner(5000, 1000, 0, true);
    EXPECT_TRUE(assigner.isEventTime());

    std::unique_ptr<SlidingWindowAssigner> eventTime(assigner.withEventTime());
    std::unique_ptr<SlidingWindowAssigner> processingTime(assigner.withProcessingTime());
    EXPECT_TRUE(eventTime->isEventTime());
    EXPECT_FALSE(processingTime->isEventTime());
}
