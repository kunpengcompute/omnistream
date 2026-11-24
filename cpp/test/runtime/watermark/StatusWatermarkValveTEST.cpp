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
#include <vector>
#include <climits>

#include "runtime/watermark/StatusWatermarkValve.h"
#include "streaming/api/watermark/Watermark.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "core/api/common/eventtime/IndexedCombinedWatermarkStatus.h"

TEST(IndexedCombinedWatermarkStatusTest, IdleInputIsExcludedAndResumesWithoutWatermarkRegression)
{
    omnistream::IndexedCombinedWatermarkStatus status(2);
    EXPECT_FALSE(status.UpdateWatermark(0, 100));
    EXPECT_TRUE(status.UpdateStatus(1, true));
    EXPECT_EQ(status.GetCombinedWatermark(), 100);
    EXPECT_FALSE(status.IsIdle());
    EXPECT_FALSE(status.UpdateStatus(0, true));
    EXPECT_TRUE(status.IsIdle());
    EXPECT_FALSE(status.UpdateStatus(1, false));
    EXPECT_FALSE(status.IsIdle());
    EXPECT_FALSE(status.UpdateWatermark(1, 50));
    EXPECT_EQ(status.GetCombinedWatermark(), 100);
    EXPECT_TRUE(status.UpdateWatermark(1, 200));
    EXPECT_EQ(status.GetCombinedWatermark(), 200);
    EXPECT_FALSE(status.UpdateStatus(0, false));
    EXPECT_FALSE(status.UpdateWatermark(1, 300));
    EXPECT_TRUE(status.UpdateWatermark(0, 250));
    EXPECT_EQ(status.GetCombinedWatermark(), 250);
    EXPECT_THROW(status.UpdateStatus(2, true), std::logic_error);
    EXPECT_THROW(status.UpdateStatus(-1, true), std::logic_error);
    EXPECT_THROW(status.UpdateWatermark(2, 300), std::logic_error);
    EXPECT_THROW(status.UpdateWatermark(-1, 300), std::logic_error);
}

class MockStatusWatermarkValveOutput {
public:
    std::vector<int64_t> emittedWatermarks;
    std::vector<WatermarkStatus*> emittedWatermarkStatuses;

    void emitWatermark(Watermark* watermark)
    {
        if (watermark != nullptr) {
            emittedWatermarks.push_back(watermark->getTimestamp());
        }
    }

    void emitWatermarkStatus(WatermarkStatus* status)
    {
        if (status != nullptr) {
            emittedWatermarkStatuses.push_back(status);
        }
    }

    int64_t getLastWatermark()
    {
        if (emittedWatermarks.empty()) {
            return INT64_MIN;
        }
        return emittedWatermarks.back();
    }

    WatermarkStatus* getLastWatermarkStatus()
    {
        if (emittedWatermarkStatuses.empty()) {
            return nullptr;
        }
        return emittedWatermarkStatuses.back();
    }
};

TEST(StatusWatermarkValveTest, ConstructorWithInvalidChannels)
{
    EXPECT_THROW(StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(0), std::logic_error);
    EXPECT_THROW(StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(-1), std::logic_error);
}

TEST(StatusWatermarkValveTest, InputWatermarkWithInvalidChannelIndex)
{
    StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(2);
    MockStatusWatermarkValveOutput output;

    Watermark wm(100);
    EXPECT_THROW(valve.inputWatermark(&wm, -1, &output), std::logic_error);
    EXPECT_THROW(valve.inputWatermark(&wm, 2, &output), std::logic_error);
}

TEST(StatusWatermarkValveTest, InputWatermarkToSingleChannel)
{
    StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(1);
    MockStatusWatermarkValveOutput output;

    Watermark wm1(100);
    valve.inputWatermark(&wm1, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), 100);

    Watermark wm2(200);
    valve.inputWatermark(&wm2, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), 200);
}

TEST(StatusWatermarkValveTest, InputWatermarkToMultipleChannels)
{
    StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(2);
    MockStatusWatermarkValveOutput output;

    Watermark wm1(100);
    valve.inputWatermark(&wm1, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), INT64_MIN);

    Watermark wm2(150);
    valve.inputWatermark(&wm2, 1, &output);
    EXPECT_EQ(output.getLastWatermark(), 100);

    Watermark wm3(200);
    valve.inputWatermark(&wm3, 1, &output);
    EXPECT_EQ(output.getLastWatermark(), 100);

    Watermark wm4(150);
    valve.inputWatermark(&wm4, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), 150);

    Watermark wm5(180);
    valve.inputWatermark(&wm5, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), 180);
}

TEST(StatusWatermarkValveTest, IdleChannelDoesNotBlockAndCanResume)
{
    StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(2);
    MockStatusWatermarkValveOutput output;
    Watermark first(100);
    valve.inputWatermark(&first, 0, &output);
    valve.inputWatermarkStatus(WatermarkStatus::idle(), 1, &output);
    EXPECT_EQ(output.getLastWatermark(), 100);
    valve.inputWatermarkStatus(WatermarkStatus::active(), 1, &output);
    Watermark resumed(150);
    valve.inputWatermark(&resumed, 1, &output);
    Watermark next(200);
    valve.inputWatermark(&next, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), 150);
    valve.inputWatermarkStatus(WatermarkStatus::idle(), 0, &output);
    valve.inputWatermarkStatus(WatermarkStatus::idle(), 1, &output);
    ASSERT_TRUE(output.getLastWatermarkStatus()->IsIdle());
    valve.inputWatermarkStatus(WatermarkStatus::active(), 1, &output);
    ASSERT_TRUE(output.getLastWatermarkStatus()->IsActive());
    Watermark finalMark(300);
    valve.inputWatermark(&finalMark, 1, &output);
    EXPECT_EQ(output.getLastWatermark(), 300);
}
