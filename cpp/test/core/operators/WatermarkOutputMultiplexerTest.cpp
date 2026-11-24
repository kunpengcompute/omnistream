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

#include "core/api/common/eventtime/WatermarkOutputMultiplexer.h"

namespace {
class RecordingWatermarkOutput : public WatermarkOutput {
public:
    void emitWatermark(Watermark* watermark) override
    {
        watermarks.push_back(watermark->getTimestamp());
    }

    void MarkIdle() override
    {
        ++idleCount;
    }

    std::vector<int64_t> watermarks;
    int idleCount = 0;
};

void emit(WatermarkOutput& output, int64_t timestamp)
{
    Watermark watermark(timestamp);
    output.emitWatermark(&watermark);
}

TEST(WatermarkOutputMultiplexerTest, DestroysRegisteredStatesExactlyOnce)
{
    RecordingWatermarkOutput output;
    for (int count : {0, 1, 3}) {
        WatermarkOutputMultiplexer multiplexer(&output);
        for (int i = 0; i < count; ++i) {
            multiplexer.RegisterNewOutput(std::to_string(i));
        }
        // Leave outputs registered, as when an unbounded source is cancelled.
    }
}

TEST(WatermarkOutputMultiplexerTest, CombinesImmediateOutputsMonotonically)
{
    RecordingWatermarkOutput output;
    WatermarkOutputMultiplexer multiplexer(&output);
    multiplexer.RegisterNewOutput("a");
    multiplexer.RegisterNewOutput("b");
    std::unique_ptr<WatermarkOutput> a(multiplexer.GetImmediateOutput("a"));
    std::unique_ptr<WatermarkOutput> b(multiplexer.GetImmediateOutput("b"));

    emit(*a, 100);
    EXPECT_TRUE(output.watermarks.empty());
    emit(*b, 200);
    emit(*a, 300);
    emit(*a, 50);
    EXPECT_EQ(output.watermarks, (std::vector<int64_t>{100, 200}));
}

TEST(WatermarkOutputMultiplexerTest, CombinesDeferredOutputsOnPeriodicEmit)
{
    RecordingWatermarkOutput output;
    WatermarkOutputMultiplexer multiplexer(&output);
    multiplexer.RegisterNewOutput("a");
    multiplexer.RegisterNewOutput("b");
    std::unique_ptr<WatermarkOutput> a(multiplexer.GetDeferredOutput("a"));
    std::unique_ptr<WatermarkOutput> b(multiplexer.GetDeferredOutput("b"));

    emit(*a, 100);
    emit(*b, 200);
    EXPECT_TRUE(output.watermarks.empty());
    multiplexer.OnPeriodicEmit();
    EXPECT_EQ(output.watermarks, (std::vector<int64_t>{100}));
}

TEST(WatermarkOutputMultiplexerTest, UnregistersAndReregistersOutputs)
{
    RecordingWatermarkOutput output;
    WatermarkOutputMultiplexer multiplexer(&output);
    multiplexer.RegisterNewOutput("a");
    multiplexer.RegisterNewOutput("b");
    auto a = std::unique_ptr<WatermarkOutput>(multiplexer.GetDeferredOutput("a"));
    auto b = std::unique_ptr<WatermarkOutput>(multiplexer.GetDeferredOutput("b"));
    emit(*a, 100);
    emit(*b, 200);
    multiplexer.OnPeriodicEmit();

    a.reset();
    EXPECT_TRUE(multiplexer.UnregisterOutput("a"));
    EXPECT_FALSE(multiplexer.UnregisterOutput("missing"));
    multiplexer.OnPeriodicEmit();
    EXPECT_EQ(output.watermarks, (std::vector<int64_t>{100, 200}));

    b.reset();
    EXPECT_TRUE(multiplexer.UnregisterOutput("b"));
    EXPECT_FALSE(multiplexer.UnregisterOutput("a"));
    multiplexer.RegisterNewOutput("a");
    a.reset(multiplexer.GetImmediateOutput("a"));
    emit(*a, 300);
    EXPECT_EQ(output.watermarks, (std::vector<int64_t>{100, 200, 300}));
}

TEST(WatermarkOutputMultiplexerTest, ExcludesIdleOutputsAndReactivatesThem)
{
    RecordingWatermarkOutput output;
    WatermarkOutputMultiplexer multiplexer(&output);
    multiplexer.RegisterNewOutput("a");
    multiplexer.RegisterNewOutput("b");
    std::unique_ptr<WatermarkOutput> a(multiplexer.GetImmediateOutput("a"));
    std::unique_ptr<WatermarkOutput> b(multiplexer.GetImmediateOutput("b"));
    emit(*a, 100);
    emit(*b, 200);
    a->MarkIdle();
    EXPECT_EQ(output.watermarks, (std::vector<int64_t>{100, 200}));
    b->MarkIdle();
    EXPECT_EQ(output.idleCount, 1);

    a->MarkActive();
    emit(*a, 300);
    EXPECT_EQ(output.watermarks, (std::vector<int64_t>{100, 200, 300}));
}
} // namespace
