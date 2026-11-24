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

#include "streaming/api/operators/source/ProgressiveTimestampsAndWatermarks.h"
#include "core/memory/DataInputDeserializer.h"
#include "streaming/runtime/streamrecord/StreamElementSerializer.h"
#include "streaming/api/operators/StreamOperatorStateHandler.h"
#include "streaming/api/operators/StreamTaskStateInitializerImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/execution/Environment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "OutputTest.h"
#include "table/runtime/operators/wmassigners/WatermarkAssignerOperator.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <gtest/gtest.h>

class IdleTwoInputTestOperator : public AbstractStreamOperator<int64_t> {
public:
    explicit IdleTwoInputTestOperator(Output* output) : AbstractStreamOperator<int64_t>(output)
    {
        combinedWatermark = new omnistream::IndexedCombinedWatermarkStatus(2);
    }
    std::string getTypeName() override
    {
        return "IdleTwoInputTestOperator";
    }
};

class IdleTwoInputTestOutput : public OutputTest {
public:
    std::vector<int> statuses;
    void emitWatermarkStatus(WatermarkStatus* status) override
    {
        statuses.push_back(status->GetStatus());
    }
};

TEST(WatermarkAssignerOperatorTest, TwoInputStatusUnblocksWatermarksAndOnlyEmitsCombinedTransitions)
{
    IdleTwoInputTestOutput output;
    IdleTwoInputTestOperator op(&output);
    Watermark first(100);
    op.ProcessWatermark1(&first);
    EXPECT_EQ(output.getWatermark()->getTimestamp(), INT64_MIN);
    op.processWatermarkStatus2(WatermarkStatus::idle());
    EXPECT_EQ(output.getWatermark()->getTimestamp(), 100);
    EXPECT_TRUE(output.statuses.empty());
    op.processWatermarkStatus1(WatermarkStatus::idle());
    ASSERT_EQ(output.statuses.size(), 1);
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::idleStatus);
    op.processWatermarkStatus2(WatermarkStatus::active());
    ASSERT_EQ(output.statuses.size(), 2);
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::activeStatus);
    Watermark lagging(50);
    op.ProcessWatermark2(&lagging);
    EXPECT_EQ(output.getWatermark()->getTimestamp(), 100);
    Watermark resumed(200);
    op.ProcessWatermark2(&resumed);
    EXPECT_EQ(output.getWatermark()->getTimestamp(), 200);
}

TEST(WatermarkAssignerOperatorTest, InitTest)
{
    int timeRowIndex = 0;
    long outOfOrderT = 4000;
    long emissionInterval = 200;
    OutputTest out = OutputTest();
    auto timerService = SystemProcessingTimeService();
    WatermarkAssignerOperator op =
        WatermarkAssignerOperator(&out, timeRowIndex, outOfOrderT, emissionInterval, &timerService);
    op.open();

    int rowCount = 5;
    auto vb1 = new omnistream::VectorBatch(rowCount);
    std::vector<long> vec1{42, 142, 242, 342, 442};
    std::vector<long> vec2{41, 42, 43, 44, 45};
    vb1->Append(omniruntime::TestUtil::CreateVector(rowCount, vec1.data()));
    vb1->Append(omniruntime::TestUtil::CreateVector(rowCount, vec2.data()));

    StreamRecord* record1 = new StreamRecord(vb1);
    op.processBatch(record1);
    op.finish();
}

class IdleTimeoutTestClock : public ProcessingTimeService {
public:
    int64_t now = 0;
    ProcessingTimeCallback* callback = nullptr;
    int64_t getCurrentProcessingTime() override
    {
        return now;
    }
    std::shared_ptr<ScheduledFutureTask> registerTimer(int64_t, ProcessingTimeCallback* target) override
    {
        callback = target;
        return nullptr;
    }
    std::shared_ptr<ScheduledFutureTask> registerTimer(int64_t, std::shared_ptr<ProcessingTimeCallback>) override
    {
        return nullptr;
    }
    std::shared_ptr<ScheduledFutureTask> scheduleWithFixedDelay(
        std::shared_ptr<ProcessingTimeCallback>, long, long) override
    {
        return nullptr;
    }
    void advance(int64_t time)
    {
        now = time;
        callback->OnProcessingTime(now);
    }
};

class IdleTimeoutTestOutput : public OutputTest {
public:
    std::vector<int> statuses;
    void emitWatermarkStatus(WatermarkStatus* status) override
    {
        statuses.push_back(status->GetStatus());
    }
    void collect(void* record) override
    {
        auto* streamRecord = static_cast<StreamRecord*>(record);
        delete static_cast<omnistream::VectorBatch*>(streamRecord->getValue());
        delete streamRecord;
    }
};

TEST(WatermarkAssignerOperatorTest, IdleTimeoutTracksEveryBatchAndResumes)
{
    for (bool split : {false, true}) {
        IdleTimeoutTestClock clock;
        IdleTimeoutTestOutput output;
        WatermarkAssignerOperator op(&output, 0, 0, 1000, &clock);
        op.setSplitWaterMark(split);
        op.open();
        auto send = [&]() {
            auto* batch = new omnistream::VectorBatch(1);
            long value = 100;
            batch->Append(omniruntime::TestUtil::CreateVector(1, &value));
            op.processBatch(new StreamRecord(batch));
        };
        clock.now = 900;
        send();
        clock.advance(1000);
        EXPECT_TRUE(output.statuses.empty());
        clock.advance(1900);
        ASSERT_EQ(output.statuses.size(), 1);
        EXPECT_EQ(output.statuses.back(), WatermarkStatus::idleStatus);
        clock.now = 2000;
        send();
        ASSERT_EQ(output.statuses.size(), 2);
        EXPECT_EQ(output.statuses.back(), WatermarkStatus::activeStatus);
        clock.advance(2999);
        EXPECT_EQ(output.statuses.size(), 2);
        clock.advance(3000);
        EXPECT_EQ(output.statuses.back(), WatermarkStatus::idleStatus);
    }
}

TEST(WatermarkAssignerOperatorTest, WatermarkStatusSerializationRoundTrip)
{
    omnistream::datastream::StreamElementSerializer serializer(nullptr);
    DataOutputSerializer output(64);
    serializer.serialize(static_cast<Object*>(WatermarkStatus::idle()), output);
    serializer.serialize(static_cast<Object*>(WatermarkStatus::active()), output);
    std::unique_ptr<std::vector<uint8_t>> bytes(output.getCopyOfBuffer());
    DataInputDeserializer input(bytes->data(), bytes->size());
    auto* idle = static_cast<StreamElement*>(serializer.deserialize(input));
    ASSERT_EQ(idle->getTag(), StreamElementTag::TAG_STREAM_STATUS);
    EXPECT_TRUE(static_cast<WatermarkStatus*>(idle)->IsIdle());
    auto* active = static_cast<StreamElement*>(serializer.deserialize(input));
    ASSERT_EQ(active->getTag(), StreamElementTag::TAG_STREAM_STATUS);
    EXPECT_TRUE(static_cast<WatermarkStatus*>(active)->IsActive());
    EXPECT_EQ(input.Available(), 0);
}

class IdleSourceTestOutput : public omnistream::OmniPushingAsyncDataInput::OmniDataOutput,
                             public TimestampsAndWatermarks::WatermarkUpdateListener {
public:
    std::vector<int> statuses;
    std::vector<int64_t> watermarks;
    void emitRecord(StreamRecord* record) override
    {
        delete static_cast<omnistream::VectorBatch*>(record->getValue());
    }
    void emitWatermark(Watermark* watermark) override
    {
        watermarks.push_back(watermark->getTimestamp());
    }
    void emitWatermarkStatus(WatermarkStatus* status) override
    {
        statuses.push_back(status->GetStatus());
    }
    void UpdateIdle(bool) override
    {
    }
    void UpdateCurrentEffectiveWatermark(long) override
    {
    }
};

TEST(WatermarkAssignerOperatorTest, KafkaSourceMainAndSplitIdleness)
{
    IdleTimeoutTestClock clock;
    IdleSourceTestOutput output;
    auto strategy = WatermarkStrategy::ForBoundedOutOfOrderness(0, 10);
    strategy->idleTimeoutMillis = 1000;
    auto logic = TimestampsAndWatermarks::CreateProgressiveEventTimeLogic(strategy, &clock, 200);
    auto* main = logic->CreateMainOutput(&output, &output);
    logic->StartPeriodicWatermarkEmits();
    auto progressive = std::static_pointer_cast<ProgressiveTimestampsAndWatermarks>(logic);
    // A reader with no assigned partition must still become idle.
    clock.now = 1000;
    progressive->callback->OnProcessingTime(clock.now);
    ASSERT_EQ(output.statuses.size(), 1);
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::idleStatus);
    auto& split = main->CreateOutputForSplit("partition-0");
    auto send = [&]() {
        auto* batch = new omnistream::VectorBatch(1);
        long value = 100;
        batch->Append(omniruntime::TestUtil::CreateVector(1, &value));
        split.Collect(batch);
    };
    send();
    ASSERT_EQ(output.statuses.size(), 2);
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::activeStatus);
    clock.now = 1900;
    send();
    clock.now = 2000;
    progressive->callback->OnProcessingTime(clock.now);
    EXPECT_EQ(output.statuses.size(), 2);
    clock.now = 2900;
    progressive->callback->OnProcessingTime(clock.now);
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::idleStatus);
    // Resuming with the same timestamp must emit ACTIVE even without a newer watermark.
    send();
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::activeStatus);
    main->ReleaseOutputForSplit("partition-0");
    delete main;
}

TEST(WatermarkAssignerOperatorTest, IdleKafkaSplitDoesNotBlockActiveSplit)
{
    IdleTimeoutTestClock clock;
    IdleSourceTestOutput output;
    auto strategy = WatermarkStrategy::ForBoundedOutOfOrderness(0, 0);
    strategy->idleTimeoutMillis = 1000;
    auto logic = TimestampsAndWatermarks::CreateProgressiveEventTimeLogic(strategy, &clock, 200);
    auto* main = logic->CreateMainOutput(&output, &output);
    auto& quietSplit = main->CreateOutputForSplit("partition-0");
    auto& activeSplit = main->CreateOutputForSplit("partition-1");
    auto send = [](SourceOutput& split, long timestamp) {
        auto* batch = new omnistream::VectorBatch(1);
        batch->Append(omniruntime::TestUtil::CreateVector(1, &timestamp));
        split.Collect(batch);
    };
    logic->StartPeriodicWatermarkEmits();
    auto progressive = std::static_pointer_cast<ProgressiveTimestampsAndWatermarks>(logic);
    send(quietSplit, 100);
    send(activeSplit, 200);
    clock.now = 900;
    send(activeSplit, 300);
    clock.now = 1000;
    progressive->callback->OnProcessingTime(clock.now);
    EXPECT_TRUE(output.statuses.empty());
    ASSERT_FALSE(output.watermarks.empty());
    EXPECT_EQ(output.watermarks.back(), 299);

    clock.now = 1900;
    progressive->callback->OnProcessingTime(clock.now);
    ASSERT_EQ(output.statuses.size(), 1);
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::idleStatus);
    clock.now = 2200;
    progressive->callback->OnProcessingTime(clock.now);
    EXPECT_EQ(output.statuses.size(), 1);

    // A lagging split resumes activity without moving the combined watermark backwards.
    send(quietSplit, 100);
    ASSERT_EQ(output.statuses.size(), 2);
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::activeStatus);
    EXPECT_EQ(output.watermarks.back(), 299);
    send(quietSplit, 400);
    EXPECT_EQ(output.watermarks.back(), 399);
    main->ReleaseOutputForSplit("partition-0");
    EXPECT_EQ(output.statuses.back(), WatermarkStatus::idleStatus);
    main->ReleaseOutputForSplit("partition-1");
    delete main;
}

TEST(WatermarkAssignerOperatorTest, KafkaIdlenessDisabledByDefault)
{
    IdleTimeoutTestClock clock;
    IdleSourceTestOutput output;
    auto strategy = WatermarkStrategy::ForBoundedOutOfOrderness(0, 0);
    auto logic = TimestampsAndWatermarks::CreateProgressiveEventTimeLogic(strategy, &clock, 200);
    auto* main = logic->CreateMainOutput(&output, &output);
    main->CreateOutputForSplit("partition-0");
    logic->StartPeriodicWatermarkEmits();
    clock.now = 60000;
    auto progressive = std::static_pointer_cast<ProgressiveTimestampsAndWatermarks>(logic);
    progressive->callback->OnProcessingTime(clock.now);
    EXPECT_TRUE(output.statuses.empty());
    main->ReleaseOutputForSplit("partition-0");
    delete main;
}
