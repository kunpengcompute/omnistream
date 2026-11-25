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

#include "runtime/metrics/SimpleCounter.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "streaming/api/operators/CountingOutput.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/utils/VectorBatchSerializationUtils.h"

namespace {
class RecordingOutput : public WatermarkGaugeExposingOutput {
public:
    void collect(void* record) override
    {
        collectedRecord = record;
        collectCount++;
    }

    void close() override
    {
        closed = true;
    }

    void emitWatermark(Watermark* watermark) override
    {
        lastWatermark = watermark;
    }

    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        lastWatermarkStatus = watermarkStatus;
    }

    void* collectedRecord = nullptr;
    int collectCount = 0;
    bool closed = false;
    Watermark* lastWatermark = nullptr;
    WatermarkStatus* lastWatermarkStatus = nullptr;
};

struct CountingOutputFixture {
    CountingOutputFixture()
        : metricGroup(std::make_shared<omnistream::TaskMetricGroup>()),
          recordsCounter(std::make_shared<omnistream::SimpleCounter>()),
          bytesCounter(std::make_shared<omnistream::SimpleCounter>())
    {
        operatorConfig.setName(operatorName);
        metricGroup->AddInternalOperatorIOMetric(operatorName, "numRecordsOut", recordsCounter);
        metricGroup->AddInternalOperatorIOMetric(operatorName, "numBytesOut", bytesCounter);
    }

    static constexpr const char* operatorName = "Calc";
    RecordingOutput downstream;
    std::shared_ptr<omnistream::TaskMetricGroup> metricGroup;
    std::shared_ptr<omnistream::SimpleCounter> recordsCounter;
    std::shared_ptr<omnistream::SimpleCounter> bytesCounter;
    omnistream::OperatorPOD operatorConfig;
};
} // namespace

TEST(CountingOutputTest, CollectIncrementsVectorBatchRowsAndForwardsRecord)
{
    CountingOutputFixture fixture;
    CountingOutput output(&fixture.downstream, fixture.metricGroup, fixture.operatorConfig);
    omnistream::VectorBatch batch(3);
    StreamRecord record(&batch);

    output.collect(&record);

    EXPECT_EQ(fixture.recordsCounter->GetCount(), 3);
    EXPECT_EQ(fixture.downstream.collectCount, 1);
    EXPECT_EQ(fixture.downstream.collectedRecord, &record);
}

TEST(CountingOutputTest, CollectAccumulatesRowsAcrossBatches)
{
    CountingOutputFixture fixture;
    CountingOutput output(&fixture.downstream, fixture.metricGroup, fixture.operatorConfig);
    omnistream::VectorBatch firstBatch(2);
    omnistream::VectorBatch secondBatch(5);
    StreamRecord firstRecord(&firstBatch);
    StreamRecord secondRecord(&secondBatch);

    output.collect(&firstRecord);
    output.collect(&secondRecord);

    EXPECT_EQ(fixture.recordsCounter->GetCount(), 7);
    EXPECT_EQ(fixture.downstream.collectCount, 2);
}

TEST(CountingOutputTest, MissingCounterStillForwardsRecord)
{
    RecordingOutput downstream;
    auto metricGroup = std::make_shared<omnistream::TaskMetricGroup>();
    omnistream::OperatorPOD operatorConfig;
    operatorConfig.setName("Calc");
    CountingOutput output(&downstream, metricGroup, operatorConfig);
    omnistream::VectorBatch batch(1);
    StreamRecord record(&batch);

    EXPECT_NO_THROW(output.collect(&record));
    EXPECT_EQ(downstream.collectCount, 1);
    EXPECT_EQ(downstream.collectedRecord, &record);
}

TEST(CountingOutputTest, WatermarkDoesNotIncrementRecords)
{
    CountingOutputFixture fixture;
    CountingOutput output(&fixture.downstream, fixture.metricGroup, fixture.operatorConfig);
    Watermark watermark(123);

    output.emitWatermark(&watermark);

    EXPECT_EQ(fixture.recordsCounter->GetCount(), 0);
    EXPECT_EQ(fixture.downstream.lastWatermark, &watermark);
}

TEST(CountingOutputTest, NullMetricGroupStillForwardsRecord)
{
    RecordingOutput downstream;
    std::shared_ptr<omnistream::TaskMetricGroup> metricGroup;
    omnistream::OperatorPOD operatorConfig;
    operatorConfig.setName("Calc");
    CountingOutput output(&downstream, metricGroup, operatorConfig);
    omnistream::VectorBatch batch(1);
    StreamRecord record(&batch);

    EXPECT_NO_THROW(output.collect(&record));
    EXPECT_EQ(downstream.collectCount, 1);
    EXPECT_EQ(downstream.collectedRecord, &record);
}

TEST(CountingOutputTest, CloseAndWatermarkStatusAreForwarded)
{
    CountingOutputFixture fixture;
    CountingOutput output(&fixture.downstream, fixture.metricGroup, fixture.operatorConfig);
    WatermarkStatus status(WatermarkStatus::idleStatus);

    output.emitWatermarkStatus(&status);
    output.close();

    EXPECT_EQ(fixture.downstream.lastWatermarkStatus, &status);
    EXPECT_TRUE(fixture.downstream.closed);
    EXPECT_EQ(fixture.recordsCounter->GetCount(), 0);
}

TEST(BytesCountingOutputTest, CollectIncrementsSerializableSizeAndForwardsRecord)
{
    CountingOutputFixture fixture;
    BytesCountingOutput output(&fixture.downstream, fixture.metricGroup, fixture.operatorConfig);
    omnistream::VectorBatch batch(3);
    StreamRecord record(&batch);
    auto expectedBytes = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&batch);

    output.collect(&record);

    EXPECT_EQ(fixture.bytesCounter->GetCount(), expectedBytes);
    EXPECT_EQ(fixture.downstream.collectCount, 1);
    EXPECT_EQ(fixture.downstream.collectedRecord, &record);
}

TEST(BytesCountingOutputTest, CollectAccumulatesAcrossBatches)
{
    CountingOutputFixture fixture;
    BytesCountingOutput output(&fixture.downstream, fixture.metricGroup, fixture.operatorConfig);
    omnistream::VectorBatch firstBatch(2);
    omnistream::VectorBatch secondBatch(5);
    StreamRecord firstRecord(&firstBatch);
    StreamRecord secondRecord(&secondBatch);
    auto expectedBytes = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&firstBatch) +
                         omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&secondBatch);

    output.collect(&firstRecord);
    output.collect(&secondRecord);

    EXPECT_EQ(fixture.bytesCounter->GetCount(), expectedBytes);
    EXPECT_EQ(fixture.downstream.collectCount, 2);
}

TEST(BytesCountingOutputTest, MissingCounterStillForwardsRecord)
{
    RecordingOutput downstream;
    auto metricGroup = std::make_shared<omnistream::TaskMetricGroup>();
    omnistream::OperatorPOD operatorConfig;
    operatorConfig.setName("Calc");
    BytesCountingOutput output(&downstream, metricGroup, operatorConfig);
    omnistream::VectorBatch batch(1);
    StreamRecord record(&batch);

    EXPECT_NO_THROW(output.collect(&record));
    EXPECT_EQ(downstream.collectCount, 1);
    EXPECT_EQ(downstream.collectedRecord, &record);
}

TEST(BytesCountingOutputTest, NullMetricGroupStillForwardsRecord)
{
    RecordingOutput downstream;
    std::shared_ptr<omnistream::TaskMetricGroup> metricGroup;
    omnistream::OperatorPOD operatorConfig;
    operatorConfig.setName("Calc");
    BytesCountingOutput output(&downstream, metricGroup, operatorConfig);
    omnistream::VectorBatch batch(1);
    StreamRecord record(&batch);

    EXPECT_NO_THROW(output.collect(&record));
    EXPECT_EQ(downstream.collectCount, 1);
    EXPECT_EQ(downstream.collectedRecord, &record);
}

TEST(BytesCountingOutputTest, WatermarkCloseAndStatusAreForwardedWithoutIncrement)
{
    CountingOutputFixture fixture;
    BytesCountingOutput output(&fixture.downstream, fixture.metricGroup, fixture.operatorConfig);
    Watermark watermark(123);
    WatermarkStatus status(WatermarkStatus::idleStatus);

    output.emitWatermark(&watermark);
    output.emitWatermarkStatus(&status);
    output.close();

    EXPECT_EQ(fixture.bytesCounter->GetCount(), 0);
    EXPECT_EQ(fixture.downstream.lastWatermark, &watermark);
    EXPECT_EQ(fixture.downstream.lastWatermarkStatus, &status);
    EXPECT_TRUE(fixture.downstream.closed);
}
