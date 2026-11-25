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
#include <nlohmann/json.hpp>

#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/utils/TimeWindowUtil.h"

namespace {

constexpr int64_t kShanghaiEpochMillis = 1704067205388L;
constexpr int64_t kUtcWallClockMillis = 1704096005388L;
constexpr int64_t kHourMillis = 3600000L;
constexpr int64_t kShanghaiOffsetMillis = 8L * kHourMillis;

int64_t ExpectedSliceEndWithoutShift()
{
    return TimeWindow1::getWindowStartWithOffset(kShanghaiEpochMillis, 0, kHourMillis) + kHourMillis;
}

int64_t ExpectedSliceEndWithShanghaiShift()
{
    return TimeWindow1::getWindowStartWithOffset(kUtcWallClockMillis, 0, kHourMillis) + kHourMillis;
}

nlohmann::json MakeHourlyTumbleDescription(const char* shiftTimeZone)
{
    nlohmann::json description = {
        {"window", "TUMBLE(size=[3600 s], offset=[0 s])"},
        {"windowSize", kHourMillis},
        {"windowOffset", 0},
        {"timeAttributeIndex", 0},
        {"inputTypes", {"TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"}},
        {"outputTypes", {"BIGINT"}},
        {"grouping", nlohmann::json::array()}};
    if (shiftTimeZone != nullptr) {
        description["shiftTimeZone"] = shiftTimeZone;
    }
    return description;
}

omnistream::VectorBatch* CreateSingleTimestampBatch(int64_t epochMillis)
{
    auto* batch = new omnistream::VectorBatch(1);
    batch->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, &epochMillis));
    return batch;
}

int64_t AssignSliceEnd(SliceAssigner* assigner, int64_t epochMillis)
{
    omnistream::VectorBatch* batch = CreateSingleTimestampBatch(epochMillis);
    ClockService clock;
    const int64_t sliceEnd = assigner->assignSliceEnd(batch, 0, &clock);
    delete batch;
    return sliceEnd;
}

} // namespace

TEST(ShiftTimeZoneWindowTest, ResolveShiftTimeZoneDefaultsToUtcWhenMissing)
{
    SliceAssigner* assigner = AssignerAtt::createSliceAssigner(MakeHourlyTumbleDescription(nullptr));
    ASSERT_NE(assigner, nullptr);
    EXPECT_EQ(ResolveShiftTimeZoneId(assigner), "UTC");
    delete assigner;
}

TEST(ShiftTimeZoneWindowTest, ResolveShiftTimeZoneReadsAsiaShanghaiFromJson)
{
    SliceAssigner* assigner = AssignerAtt::createSliceAssigner(MakeHourlyTumbleDescription("Asia/Shanghai"));
    ASSERT_NE(assigner, nullptr);
    EXPECT_EQ(ResolveShiftTimeZoneId(assigner), "Asia/Shanghai");
    delete assigner;
}

TEST(ShiftTimeZoneWindowTest, ResolveShiftTimeZoneExplicitUtcMatchesMissingField)
{
    SliceAssigner* withoutShift = AssignerAtt::createSliceAssigner(MakeHourlyTumbleDescription(nullptr));
    SliceAssigner* withUtc = AssignerAtt::createSliceAssigner(MakeHourlyTumbleDescription("UTC"));
    ASSERT_NE(withoutShift, nullptr);
    ASSERT_NE(withUtc, nullptr);
    EXPECT_EQ(ResolveShiftTimeZoneId(withoutShift), ResolveShiftTimeZoneId(withUtc));
    EXPECT_EQ(AssignSliceEnd(withoutShift, kShanghaiEpochMillis), AssignSliceEnd(withUtc, kShanghaiEpochMillis));
    delete withoutShift;
    delete withUtc;
}

TEST(ShiftTimeZoneWindowTest, TumbleAssignSliceEndWithoutShiftTimezone)
{
    SliceAssigner* assigner = AssignerAtt::createSliceAssigner(MakeHourlyTumbleDescription(nullptr));
    ASSERT_NE(assigner, nullptr);
    EXPECT_EQ(AssignSliceEnd(assigner, kShanghaiEpochMillis), ExpectedSliceEndWithoutShift());
    delete assigner;
}

TEST(ShiftTimeZoneWindowTest, TumbleAssignSliceEndWithAsiaShanghai)
{
    SliceAssigner* assigner = AssignerAtt::createSliceAssigner(MakeHourlyTumbleDescription("Asia/Shanghai"));
    ASSERT_NE(assigner, nullptr);
    EXPECT_EQ(AssignSliceEnd(assigner, kShanghaiEpochMillis), ExpectedSliceEndWithShanghaiShift());
    delete assigner;
}

TEST(ShiftTimeZoneWindowTest, TumbleAssignSliceEndShiftedAndUnshiftedDifferByEightHours)
{
    SliceAssigner* utcAssigner = AssignerAtt::createSliceAssigner(MakeHourlyTumbleDescription("UTC"));
    SliceAssigner* shanghaiAssigner = AssignerAtt::createSliceAssigner(MakeHourlyTumbleDescription("Asia/Shanghai"));
    ASSERT_NE(utcAssigner, nullptr);
    ASSERT_NE(shanghaiAssigner, nullptr);

    const int64_t utcSliceEnd = AssignSliceEnd(utcAssigner, kShanghaiEpochMillis);
    const int64_t shanghaiSliceEnd = AssignSliceEnd(shanghaiAssigner, kShanghaiEpochMillis);
    EXPECT_EQ(shanghaiSliceEnd - utcSliceEnd, kShanghaiOffsetMillis);

    delete utcAssigner;
    delete shanghaiAssigner;
}

TEST(ShiftTimeZoneWindowTest, IsWindowFiredWithoutShiftTimezone)
{
    const int64_t windowEnd = ExpectedSliceEndWithoutShift();
    const int64_t triggerTime = TimeWindowUtil::toEpochMillsForTimer(windowEnd - 1, "UTC");
    EXPECT_EQ(triggerTime, windowEnd - 1);
    EXPECT_FALSE(TimeWindowUtil::isWindowFired(windowEnd, triggerTime - 1, "UTC"));
    EXPECT_TRUE(TimeWindowUtil::isWindowFired(windowEnd, triggerTime, "UTC"));
}

TEST(ShiftTimeZoneWindowTest, IsWindowFiredWithAsiaShanghai)
{
    const int64_t windowEnd = ExpectedSliceEndWithShanghaiShift();
    const int64_t triggerTime = TimeWindowUtil::toEpochMillsForTimer(windowEnd - 1, "Asia/Shanghai");
    EXPECT_EQ(triggerTime, 1704070799999L);
    EXPECT_FALSE(TimeWindowUtil::isWindowFired(windowEnd, triggerTime - 1, "Asia/Shanghai"));
    EXPECT_TRUE(TimeWindowUtil::isWindowFired(windowEnd, triggerTime, "Asia/Shanghai"));
}

TEST(ShiftTimeZoneWindowTest, WindowJoinTimerRegistrationWithoutShiftTimezone)
{
    const int64_t windowEnd = 10000L;
    EXPECT_EQ(TimeWindowUtil::toEpochMillsForTimer(windowEnd - 1, "UTC"), windowEnd - 1);
}

TEST(ShiftTimeZoneWindowTest, WindowJoinTimerRegistrationWithAsiaShanghai)
{
    const int64_t windowEnd = ExpectedSliceEndWithShanghaiShift();
    EXPECT_EQ(TimeWindowUtil::toEpochMillsForTimer(windowEnd - 1, "Asia/Shanghai"), 1704070799999L);
}

TEST(ShiftTimeZoneWindowTest, WindowJoinLateDetectionWithoutShiftTimezone)
{
    const int64_t windowEnd = 10000L;
    const int64_t triggerTime = TimeWindowUtil::toEpochMillsForTimer(windowEnd - 1, "UTC");
    EXPECT_FALSE(TimeWindowUtil::isWindowFired(windowEnd, triggerTime - 1, "UTC"));
    EXPECT_TRUE(TimeWindowUtil::isWindowFired(windowEnd, triggerTime, "UTC"));
}

TEST(ShiftTimeZoneWindowTest, TimeConversionWithoutShiftTimezone)
{
    EXPECT_EQ(TimeWindowUtil::toUtcTimestampMills(kShanghaiEpochMillis, "UTC"), kShanghaiEpochMillis);
    EXPECT_EQ(TimeWindowUtil::toUtcTimestampMills(kShanghaiEpochMillis, ""), kShanghaiEpochMillis);
    EXPECT_EQ(TimeWindowUtil::toEpochMills(kShanghaiEpochMillis, "UTC"), kShanghaiEpochMillis);
    EXPECT_EQ(
        TimeWindowUtil::toCleanupTimerMills(ExpectedSliceEndWithoutShift() - 1, 0, "UTC"),
        ExpectedSliceEndWithoutShift() - 1);
}

TEST(ShiftTimeZoneWindowTest, IsWindowFiredTwoArgMatchesUtcThreeArg)
{
    const int64_t windowEnd = ExpectedSliceEndWithoutShift();
    const int64_t triggerTime = windowEnd - 1;
    EXPECT_EQ(
        TimeWindowUtil::isWindowFired(windowEnd, triggerTime - 1),
        TimeWindowUtil::isWindowFired(windowEnd, triggerTime - 1, "UTC"));
    EXPECT_EQ(
        TimeWindowUtil::isWindowFired(windowEnd, triggerTime),
        TimeWindowUtil::isWindowFired(windowEnd, triggerTime, "UTC"));
}

TEST(ShiftTimeZoneWindowTest, WindowJoinLateDetectionWithAsiaShanghai)
{
    const int64_t windowEnd = ExpectedSliceEndWithShanghaiShift();
    const int64_t triggerTime = TimeWindowUtil::toEpochMillsForTimer(windowEnd - 1, "Asia/Shanghai");
    EXPECT_FALSE(TimeWindowUtil::isWindowFired(windowEnd, triggerTime - 1, "Asia/Shanghai"));
    EXPECT_TRUE(TimeWindowUtil::isWindowFired(windowEnd, triggerTime, "Asia/Shanghai"));
}
