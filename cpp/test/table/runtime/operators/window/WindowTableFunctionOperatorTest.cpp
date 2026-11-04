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
#include <nlohmann/json.hpp>

#include <algorithm>
#include <string>
#include <vector>

#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "core/graph/OperatorConfig.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "streaming/api/watermark/Watermark.h"
#include "streaming/runtime/io/RecordWriterOutput.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "table/data/RowKind.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/TableOperatorConstants.h"
#include "table/runtime/operators/window/tvf/WindowTableFunctionOperator.h"
#include "table/utils/TimeWindowUtil.h"

namespace {

const std::vector<std::string> kInputTypes = {"VARCHAR", "INT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"};

std::vector<std::string> MakeEventTimeOutputTypes()
{
    return {"VARCHAR",
            "INT",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)"};
}

std::vector<std::string> MakeProcessingTimeOutputTypes()
{
    return {"VARCHAR",
            "INT",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"};
}

nlohmann::json MakeDescription(
    const std::string& window,
    const nlohmann::json& windowParams,
    const std::string& timeType,
    int64_t timeAttributeIndex,
    const std::string& shiftTimeZone,
    const std::vector<std::string>& inputTypes,
    const std::vector<std::string>& outputTypes)
{
    nlohmann::json desc;
    desc["window"] = window;
    const std::string windowType = window.substr(0, window.find('('));
    desc["windowType"] = windowType;
    desc["timeType"] = timeType;
    desc["timeAttributeIndex"] = timeAttributeIndex;
    desc["shiftTimeZone"] = shiftTimeZone;
    desc["inputTypes"] = inputTypes;
    desc["outputTypes"] = outputTypes;
    for (auto it = windowParams.begin(); it != windowParams.end(); ++it) {
        desc[it.key()] = it.value();
    }
    return desc;
}

nlohmann::json MakeTumbleDescription(
    const std::string& timeType,
    int64_t timeAttributeIndex,
    const std::string& shiftTimeZone,
    const std::vector<std::string>& outputTypes,
    int64_t size = 3000,
    int64_t offset = 0)
{
    return MakeDescription("TUMBLE(size=[3 s], offset=[0 s])",
        {{"windowSize", size}, {"windowOffset", offset}},
        timeType,
        timeAttributeIndex,
        shiftTimeZone,
        kInputTypes,
        outputTypes);
}

nlohmann::json MakeHopDescription(
    const std::string& timeType,
    int64_t timeAttributeIndex,
    const std::string& shiftTimeZone,
    const std::vector<std::string>& outputTypes)
{
    return MakeDescription("HOP(size=[3 s], slide=[1 s], offset=[0 s])",
        {{"windowSize", 3000}, {"windowSlide", 1000}, {"windowOffset", 0}},
        timeType,
        timeAttributeIndex,
        shiftTimeZone,
        kInputTypes,
        outputTypes);
}

nlohmann::json MakeCumulateDescription(
    const std::string& timeType,
    int64_t timeAttributeIndex,
    const std::string& shiftTimeZone,
    const std::vector<std::string>& outputTypes)
{
    return MakeDescription("CUMULATE(maxSize=[3 s], step=[1 s], offset=[0 s])",
        {{"maxSize", 3000}, {"step", 1000}, {"windowOffset", 0}},
        timeType,
        timeAttributeIndex,
        shiftTimeZone,
        kInputTypes,
        outputTypes);
}

// Deterministic ProcessingTimeService with a settable current time.
class MockProcessingTimeService : public ProcessingTimeService {
public:
    explicit MockProcessingTimeService(int64_t currentTime = 0) : currentTime_(currentTime)
    {
    }

    void setCurrentTime(int64_t currentTime)
    {
        currentTime_ = currentTime;
    }

    int64_t getCurrentProcessingTime() override
    {
        return currentTime_;
    }

    ScheduledFutureTask* registerTimer(int64_t timestamp, ProcessingTimeCallback* target) override
    {
        return nullptr;
    }

    ScheduledFutureTask* scheduleWithFixedDelay(
        ProcessingTimeCallback* callback, long initialDelay, long period) override
    {
        return nullptr;
    }

private:
    int64_t currentTime_;
};

// Collecting output that derives from RecordWriterOutput so it can be passed to
// StreamOperatorFactory::createOperatorAndCollector.
class WindowTableFunctionCollectingOutput : public omnistream::datastream::RecordWriterOutput {
public:
    WindowTableFunctionCollectingOutput() : RecordWriterOutput(nullptr, nullptr)
    {
    }

    std::vector<omnistream::VectorBatch*> outputs;
    Watermark watermark_{INT64_MIN};

    void collect(void* record) override
    {
        outputs.push_back(
            static_cast<omnistream::VectorBatch*>(reinterpret_cast<StreamRecord*>(record)->getValue()));
    }

    void emitWatermark(Watermark* mark) override
    {
        watermark_ = Watermark(mark->getTimestamp());
    }

    Watermark* getWatermark()
    {
        return &watermark_;
    }

    void emitWatermarkStatus(WatermarkStatus* status) override
    {
    }

    void close() override
    {
    }
};

struct TestOperator {
    WindowTableFunctionOperator* op = nullptr;
    WindowTableFunctionCollectingOutput* output = nullptr;
    MockProcessingTimeService* processingTimeService = nullptr;
};

TestOperator CreateTestOperator(const nlohmann::json& description, bool processingTime = false)
{
    omnistream::OperatorConfig opConfig(
        std::string(omnistream::OPERATOR_NAME_WINDOW_TABLE_FUNCTION),
        "WindowTableFunction_Test",
        nlohmann::json::array(),
        nlohmann::json::array(),
        description);
    auto* output = new WindowTableFunctionCollectingOutput();
    auto* op = dynamic_cast<WindowTableFunctionOperator*>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    TestOperator handle{op, output, nullptr};
    if (processingTime) {
        handle.processingTimeService = new MockProcessingTimeService();
        op->setProcessingTimeService(handle.processingTimeService);
    }
    op->open();
    return handle;
}

void DestroyTestOperator(TestOperator& handle)
{
    handle.op->close();
    delete handle.op;
    delete handle.processingTimeService;
    delete handle.output;
}

// Builds a VectorBatch with columns f0 VARCHAR, f1 INT, f2 rowtime(TIMESTAMP).
omnistream::VectorBatch* MakeInputBatch(
    const std::vector<std::string>& f0,
    const std::vector<int32_t>& f1,
    const std::vector<int64_t>& rowTime,
    const std::vector<RowKind>& rowKinds)
{
    const int32_t n = static_cast<int32_t>(f0.size());
    auto* batch = new omnistream::VectorBatch(n);
    std::vector<std::string> f0Copy = f0;
    std::vector<int32_t> f1Copy = f1;
    std::vector<int64_t> rowTimeCopy = rowTime;
    batch->Append(omniruntime::TestUtil::CreateVarcharVector(f0Copy.data(), n));
    batch->Append(omniruntime::TestUtil::CreateVector<int32_t>(n, f1Copy.data()));
    batch->Append(omniruntime::TestUtil::CreateVector<int64_t>(n, rowTimeCopy.data()));
    for (int32_t i = 0; i < n; ++i) {
        batch->setRowKind(i, rowKinds[i]);
    }
    return batch;
}

std::string GetVarchar(omnistream::VectorBatch* batch, int32_t col, int32_t row)
{
    auto* vec = reinterpret_cast<omniruntime::vec::Vector<
        omniruntime::vec::LargeStringContainer<std::string_view>>*>(batch->Get(col));
    return std::string(vec->GetValue(row));
}

struct OutputRow {
    std::string f0;
    int32_t f1 = 0;
    int64_t rowTime = 0;
    int64_t windowStart = 0;
    int64_t windowEnd = 0;
    int64_t windowTime = 0;
};

std::vector<OutputRow> ReadOutputRows(const std::vector<omnistream::VectorBatch*>& batches)
{
    std::vector<OutputRow> rows;
    for (auto* batch : batches) {
        for (int32_t r = 0; r < batch->GetRowCount(); ++r) {
            OutputRow row;
            row.f0 = GetVarchar(batch, 0, r);
            row.f1 = batch->GetValueAt<int32_t>(1, r);
            row.rowTime = batch->GetValueAt<int64_t>(2, r);
            row.windowStart = batch->GetValueAt<int64_t>(3, r);
            row.windowEnd = batch->GetValueAt<int64_t>(4, r);
            row.windowTime = batch->GetValueAt<int64_t>(5, r);
            rows.push_back(row);
        }
    }
    return rows;
}

void SortRowsByWindowEnd(std::vector<OutputRow>& rows)
{
    std::stable_sort(rows.begin(), rows.end(), [](const OutputRow& a, const OutputRow& b) {
        if (a.rowTime != b.rowTime) {
            return a.rowTime < b.rowTime;
        }
        return a.windowEnd < b.windowEnd;
    });
}

void ExpectOutputRowsEqual(
    const std::vector<OutputRow>& actual, const std::vector<OutputRow>& expected, const std::string& msg)
{
    std::vector<OutputRow> actualSorted = actual;
    std::vector<OutputRow> expectedSorted = expected;
    SortRowsByWindowEnd(actualSorted);
    SortRowsByWindowEnd(expectedSorted);
    ASSERT_EQ(actualSorted.size(), expectedSorted.size()) << msg;
    for (size_t i = 0; i < expectedSorted.size(); ++i) {
        EXPECT_EQ(actualSorted[i].f0, expectedSorted[i].f0) << msg << " at index " << i;
        EXPECT_EQ(actualSorted[i].f1, expectedSorted[i].f1) << msg << " at index " << i;
        EXPECT_EQ(actualSorted[i].rowTime, expectedSorted[i].rowTime) << msg << " at index " << i;
        EXPECT_EQ(actualSorted[i].windowStart, expectedSorted[i].windowStart) << msg << " at index " << i;
        EXPECT_EQ(actualSorted[i].windowEnd, expectedSorted[i].windowEnd) << msg << " at index " << i;
        EXPECT_EQ(actualSorted[i].windowTime, expectedSorted[i].windowTime) << msg << " at index " << i;
    }
}

} // namespace

class WindowTableFunctionOperatorTest : public ::testing::TestWithParam<const char*> {
protected:
    std::string shiftTimeZone() const
    {
        return GetParam();
    }

    // Converts a UTC-aligned wall clock value to the stored epoch millis, matching
    // the Flink test's localMills(epochMills).
    int64_t LocalMills(int64_t utcWallClock) const
    {
        return TimeWindowUtil::toUtcTimestampMills(utcWallClock, shiftTimeZone());
    }

    std::vector<RowKind> InsertKinds(int32_t n) const
    {
        return std::vector<RowKind>(n, RowKind::INSERT);
    }
};

INSTANTIATE_TEST_SUITE_P(TimeZone,
    WindowTableFunctionOperatorTest,
    ::testing::Values("UTC", "Asia/Shanghai"));

TEST_P(WindowTableFunctionOperatorTest, TumblingWindowsAppendsWindowColumns)
{
    auto desc = MakeTumbleDescription("event", 2, shiftTimeZone(), MakeEventTimeOutputTypes());
    TestOperator handle = CreateTestOperator(desc);
    ASSERT_NE(handle.op, nullptr);

    handle.op->processBatch(new StreamRecord(
        MakeInputBatch({"key1", "key2"}, {1, 1}, {20L, 3999L}, InsertKinds(2))));

    std::vector<OutputRow> expected = {
        {"key1", 1, 20L, LocalMills(0L), LocalMills(3000L), 2999L},
        {"key2", 1, 3999L, LocalMills(3000L), LocalMills(6000L), 5999L}};
    ExpectOutputRowsEqual(ReadOutputRows(handle.output->outputs), expected, "tumbling windows");

    // Late element would not be dropped.
    handle.op->processBatch(new StreamRecord(
        MakeInputBatch({"key2"}, {1}, {80L}, InsertKinds(1))));
    expected.push_back({"key2", 1, 80L, LocalMills(0L), LocalMills(3000L), 2999L});
    ExpectOutputRowsEqual(ReadOutputRows(handle.output->outputs), expected, "tumbling windows with late record");

    for (auto* batch : handle.output->outputs) {
        delete batch;
    }
    DestroyTestOperator(handle);
}

TEST_P(WindowTableFunctionOperatorTest, ProcessingTimeTumblingWindowsUsesServiceTime)
{
    auto desc = MakeTumbleDescription("processing", -1, shiftTimeZone(), MakeProcessingTimeOutputTypes());
    TestOperator handle = CreateTestOperator(desc, /*processingTime=*/true);
    ASSERT_NE(handle.op, nullptr);

    // Rowtime column value is ignored in processing time; window bound only depends on the service time.
    handle.processingTimeService->setCurrentTime(20L);
    handle.op->processBatch(
        new StreamRecord(MakeInputBatch({"key1"}, {1}, {20L}, InsertKinds(1))));

    handle.processingTimeService->setCurrentTime(3999L);
    handle.op->processBatch(
        new StreamRecord(MakeInputBatch({"key2"}, {1}, {3999L}, InsertKinds(1))));

    std::vector<OutputRow> expected = {
        {"key1", 1, 20L, LocalMills(0L), LocalMills(3000L), 2999L},
        {"key2", 1, 3999L, LocalMills(3000L), LocalMills(6000L), 5999L}};
    ExpectOutputRowsEqual(ReadOutputRows(handle.output->outputs), expected, "processing time tumbling windows");

    for (auto* batch : handle.output->outputs) {
        delete batch;
    }
    DestroyTestOperator(handle);
}

TEST_P(WindowTableFunctionOperatorTest, HopWindowsAssignMultipleWindowsPerRow)
{
    auto desc = MakeHopDescription("event", 2, shiftTimeZone(), MakeEventTimeOutputTypes());
    TestOperator handle = CreateTestOperator(desc);
    ASSERT_NE(handle.op, nullptr);

    handle.op->processBatch(new StreamRecord(
        MakeInputBatch({"key1", "key2"}, {1, 1}, {20L, 3999L}, InsertKinds(2))));

    std::vector<OutputRow> expected = {
        {"key1", 1, 20L, LocalMills(-2000L), LocalMills(1000L), 999L},
        {"key1", 1, 20L, LocalMills(-1000L), LocalMills(2000L), 1999L},
        {"key1", 1, 20L, LocalMills(0L), LocalMills(3000L), 2999L},
        {"key2", 1, 3999L, LocalMills(1000L), LocalMills(4000L), 3999L},
        {"key2", 1, 3999L, LocalMills(2000L), LocalMills(5000L), 4999L},
        {"key2", 1, 3999L, LocalMills(3000L), LocalMills(6000L), 5999L}};
    ExpectOutputRowsEqual(ReadOutputRows(handle.output->outputs), expected, "hop windows");

    // Late element would not be dropped.
    handle.op->processBatch(
        new StreamRecord(MakeInputBatch({"key2"}, {1}, {80L}, InsertKinds(1))));
    expected.push_back({"key2", 1, 80L, LocalMills(-2000L), LocalMills(1000L), 999L});
    expected.push_back({"key2", 1, 80L, LocalMills(-1000L), LocalMills(2000L), 1999L});
    expected.push_back({"key2", 1, 80L, LocalMills(0L), LocalMills(3000L), 2999L});
    ExpectOutputRowsEqual(ReadOutputRows(handle.output->outputs), expected, "hop windows with late record");

    for (auto* batch : handle.output->outputs) {
        delete batch;
    }
    DestroyTestOperator(handle);
}

TEST_P(WindowTableFunctionOperatorTest, ProcessingTimeHopWindowsAssignMultipleWindows)
{
    auto desc = MakeHopDescription("processing", -1, shiftTimeZone(), MakeProcessingTimeOutputTypes());
    TestOperator handle = CreateTestOperator(desc, /*processingTime=*/true);
    ASSERT_NE(handle.op, nullptr);

    handle.processingTimeService->setCurrentTime(20L);
    handle.op->processBatch(
        new StreamRecord(MakeInputBatch({"key1"}, {1}, {20L}, InsertKinds(1))));

    handle.processingTimeService->setCurrentTime(3999L);
    handle.op->processBatch(
        new StreamRecord(MakeInputBatch({"key2"}, {1}, {3999L}, InsertKinds(1))));

    std::vector<OutputRow> expected = {
        {"key1", 1, 20L, LocalMills(-2000L), LocalMills(1000L), 999L},
        {"key1", 1, 20L, LocalMills(-1000L), LocalMills(2000L), 1999L},
        {"key1", 1, 20L, LocalMills(0L), LocalMills(3000L), 2999L},
        {"key2", 1, 3999L, LocalMills(1000L), LocalMills(4000L), 3999L},
        {"key2", 1, 3999L, LocalMills(2000L), LocalMills(5000L), 4999L},
        {"key2", 1, 3999L, LocalMills(3000L), LocalMills(6000L), 5999L}};
    ExpectOutputRowsEqual(ReadOutputRows(handle.output->outputs), expected, "processing time hop windows");

    for (auto* batch : handle.output->outputs) {
        delete batch;
    }
    DestroyTestOperator(handle);
}

TEST_P(WindowTableFunctionOperatorTest, CumulativeWindowsAssignStepAlignedWindows)
{
    auto desc = MakeCumulateDescription("event", 2, shiftTimeZone(), MakeEventTimeOutputTypes());
    TestOperator handle = CreateTestOperator(desc);
    ASSERT_NE(handle.op, nullptr);

    handle.op->processBatch(new StreamRecord(
        MakeInputBatch({"key1", "key2"}, {1, 1}, {20L, 3999L}, InsertKinds(2))));

    std::vector<OutputRow> expected = {
        {"key1", 1, 20L, LocalMills(0L), LocalMills(1000L), 999L},
        {"key1", 1, 20L, LocalMills(0L), LocalMills(2000L), 1999L},
        {"key1", 1, 20L, LocalMills(0L), LocalMills(3000L), 2999L},
        {"key2", 1, 3999L, LocalMills(3000L), LocalMills(4000L), 3999L},
        {"key2", 1, 3999L, LocalMills(3000L), LocalMills(5000L), 4999L},
        {"key2", 1, 3999L, LocalMills(3000L), LocalMills(6000L), 5999L}};
    ExpectOutputRowsEqual(ReadOutputRows(handle.output->outputs), expected, "cumulate windows");

    // Late element would not be dropped.
    handle.op->processBatch(
        new StreamRecord(MakeInputBatch({"key2"}, {1}, {80L}, InsertKinds(1))));
    expected.push_back({"key2", 1, 80L, LocalMills(0L), LocalMills(1000L), 999L});
    expected.push_back({"key2", 1, 80L, LocalMills(0L), LocalMills(2000L), 1999L});
    expected.push_back({"key2", 1, 80L, LocalMills(0L), LocalMills(3000L), 2999L});
    ExpectOutputRowsEqual(ReadOutputRows(handle.output->outputs), expected, "cumulate windows with late record");

    for (auto* batch : handle.output->outputs) {
        delete batch;
    }
    DestroyTestOperator(handle);
}

TEST_P(WindowTableFunctionOperatorTest, ProcessingTimeCumulativeWindows)
{
    auto desc = MakeCumulateDescription("processing", -1, shiftTimeZone(), MakeProcessingTimeOutputTypes());
    TestOperator handle = CreateTestOperator(desc, /*processingTime=*/true);
    ASSERT_NE(handle.op, nullptr);

    handle.processingTimeService->setCurrentTime(20L);
    handle.op->processBatch(
        new StreamRecord(MakeInputBatch({"key1"}, {1}, {20L}, InsertKinds(1))));

    handle.processingTimeService->setCurrentTime(3999L);
    handle.op->processBatch(
        new StreamRecord(MakeInputBatch({"key2"}, {1}, {3999L}, InsertKinds(1))));

    std::vector<OutputRow> expected = {
        {"key1", 1, 20L, LocalMills(0L), LocalMills(1000L), 999L},
        {"key1", 1, 20L, LocalMills(0L), LocalMills(2000L), 1999L},
        {"key1", 1, 20L, LocalMills(0L), LocalMills(3000L), 2999L},
        {"key2", 1, 3999L, LocalMills(3000L), LocalMills(4000L), 3999L},
        {"key2", 1, 3999L, LocalMills(3000L), LocalMills(5000L), 4999L},
        {"key2", 1, 3999L, LocalMills(3000L), LocalMills(6000L), 5999L}};
    ExpectOutputRowsEqual(ReadOutputRows(handle.output->outputs), expected, "processing time cumulate windows");

    for (auto* batch : handle.output->outputs) {
        delete batch;
    }
    DestroyTestOperator(handle);
}

TEST_P(WindowTableFunctionOperatorTest, NullRowTimeRecordIsDropped)
{
    auto desc = MakeTumbleDescription("event", 2, shiftTimeZone(), MakeEventTimeOutputTypes());
    TestOperator handle = CreateTestOperator(desc);
    ASSERT_NE(handle.op, nullptr);

    // Row 1 carries a NULL rowtime and must be silently dropped.
    auto* batch = new omnistream::VectorBatch(2);
    std::vector<std::string> keys = {"key1", "key2"};
    std::vector<int32_t> values = {1, 1};
    std::vector<int64_t> times = {20L, 3999L};
    batch->Append(omniruntime::TestUtil::CreateVarcharVector(keys.data(), 2));
    batch->Append(omniruntime::TestUtil::CreateVector<int32_t>(2, values.data()));
    auto* tsVec = new omniruntime::vec::Vector<int64_t>(2);
    tsVec->SetValue(0, 20L);
    tsVec->SetNull(1);
    batch->Append(tsVec);
    batch->setRowKind(0, RowKind::INSERT);
    batch->setRowKind(1, RowKind::INSERT);

    handle.op->processBatch(new StreamRecord(batch));

    std::vector<OutputRow> expected = {
        {"key1", 1, 20L, LocalMills(0L), LocalMills(3000L), 2999L}};
    ExpectOutputRowsEqual(ReadOutputRows(handle.output->outputs), expected, "null rowtime record is dropped");

    for (auto* out : handle.output->outputs) {
        delete out;
    }
    DestroyTestOperator(handle);
}

TEST_P(WindowTableFunctionOperatorTest, WindowOffsetShiftsWindowBoundaries)
{
    // Positive offset: windows are aligned to 100ms instead of 0.
    auto positiveDesc =
        MakeTumbleDescription("event", 2, shiftTimeZone(), MakeEventTimeOutputTypes(), 3000, 100);
    TestOperator positiveOp = CreateTestOperator(positiveDesc);
    ASSERT_NE(positiveOp.op, nullptr);
    positiveOp.op->processBatch(
        new StreamRecord(MakeInputBatch({"key1"}, {1}, {100L}, InsertKinds(1))));
    positiveOp.op->processBatch(
        new StreamRecord(MakeInputBatch({"key2"}, {1}, {3099L}, InsertKinds(1))));
    positiveOp.op->processBatch(
        new StreamRecord(MakeInputBatch({"key3"}, {1}, {3100L}, InsertKinds(1))));

    std::vector<OutputRow> expectedPositive = {
        {"key1", 1, 100L, LocalMills(100L), LocalMills(3100L), 3099L},
        {"key2", 1, 3099L, LocalMills(100L), LocalMills(3100L), 3099L},
        {"key3", 1, 3100L, LocalMills(3100L), LocalMills(6100L), 6099L}};
    ExpectOutputRowsEqual(
        ReadOutputRows(positiveOp.output->outputs), expectedPositive, "positive window offset");
    for (auto* out : positiveOp.output->outputs) {
        delete out;
    }
    DestroyTestOperator(positiveOp);

    // Negative offset: window boundaries shift back by 100ms.
    auto negativeDesc =
        MakeTumbleDescription("event", 2, shiftTimeZone(), MakeEventTimeOutputTypes(), 3000, -100);
    TestOperator negativeOp = CreateTestOperator(negativeDesc);
    ASSERT_NE(negativeOp.op, nullptr);
    negativeOp.op->processBatch(
        new StreamRecord(MakeInputBatch({"key1"}, {1}, {99L}, InsertKinds(1))));
    negativeOp.op->processBatch(
        new StreamRecord(MakeInputBatch({"key2"}, {1}, {-100L}, InsertKinds(1))));
    negativeOp.op->processBatch(
        new StreamRecord(MakeInputBatch({"key3"}, {1}, {-101L}, InsertKinds(1))));

    std::vector<OutputRow> expectedNegative = {
        {"key1", 1, 99L, LocalMills(-100L), LocalMills(2900L), 2899L},
        {"key2", 1, -100L, LocalMills(-100L), LocalMills(2900L), 2899L},
        {"key3", 1, -101L, LocalMills(-3100L), LocalMills(-100L), -101L}};
    ExpectOutputRowsEqual(
        ReadOutputRows(negativeOp.output->outputs), expectedNegative, "negative window offset");
    for (auto* out : negativeOp.output->outputs) {
        delete out;
    }
    DestroyTestOperator(negativeOp);
}

TEST_P(WindowTableFunctionOperatorTest, RowKindIsPreservedInOutput)
{
    auto desc = MakeTumbleDescription("event", 2, shiftTimeZone(), MakeEventTimeOutputTypes());
    TestOperator handle = CreateTestOperator(desc);
    ASSERT_NE(handle.op, nullptr);

    std::vector<RowKind> rowKinds = {
        RowKind::INSERT, RowKind::UPDATE_BEFORE, RowKind::UPDATE_AFTER, RowKind::DELETE};
    handle.op->processBatch(
        new StreamRecord(MakeInputBatch({"k1", "k2", "k3", "k4"}, {1, 2, 3, 4}, {20L, 21L, 22L, 23L}, rowKinds)));

    std::vector<OutputRow> allRows = ReadOutputRows(handle.output->outputs);
    ASSERT_EQ(allRows.size(), 4);
    for (size_t i = 0; i < rowKinds.size(); ++i) {
        ASSERT_EQ(handle.output->outputs.size(), 1u);
        EXPECT_EQ(handle.output->outputs.front()->getRowKind(static_cast<int32_t>(i)), rowKinds[i])
            << "RowKind not preserved at index " << i;
        EXPECT_EQ(allRows[i].rowTime, static_cast<int64_t>(20L + i));
    }

    for (auto* out : handle.output->outputs) {
        delete out;
    }
    DestroyTestOperator(handle);
}

TEST_P(WindowTableFunctionOperatorTest, WatermarkIsPassedThrough)
{
    auto desc = MakeTumbleDescription("event", 2, shiftTimeZone(), MakeEventTimeOutputTypes());
    TestOperator handle = CreateTestOperator(desc);
    ASSERT_NE(handle.op, nullptr);

    Watermark mark(999L);
    handle.op->ProcessWatermark(&mark);

    ASSERT_NE(handle.output->getWatermark(), nullptr);
    EXPECT_EQ(handle.output->getWatermark()->getTimestamp(), 999L);

    DestroyTestOperator(handle);
}

TEST_P(WindowTableFunctionOperatorTest, InvalidConfigurationThrows)
{
    // Event time requires timeAttributeIndex >= 0.
    auto missingIndex = MakeTumbleDescription("event", -1, shiftTimeZone(), MakeEventTimeOutputTypes());
    EXPECT_THROW(new WindowTableFunctionOperator(missingIndex, nullptr), std::logic_error);

    // outputTypes must be inputTypes + 3 window columns.
    auto badOutputTypes = MakeTumbleDescription("event", 2, shiftTimeZone(), kInputTypes);
    EXPECT_THROW(new WindowTableFunctionOperator(badOutputTypes, nullptr), std::logic_error);

    // Unsupported window type.
    auto sessionWindow = MakeDescription("SESSION(size=[3 s])",
        {{"windowSize", 3000}},
        "event",
        2,
        shiftTimeZone(),
        kInputTypes,
        MakeEventTimeOutputTypes());
    EXPECT_THROW(new WindowTableFunctionOperator(sessionWindow, nullptr), std::logic_error);

    // CUMULATE requires maxSize % step == 0.
    auto badCumulate = MakeDescription("CUMULATE(maxSize=[3 s], step=[2 s])",
        {{"maxSize", 3000}, {"step", 2000}, {"windowOffset", 0}},
        "event",
        2,
        shiftTimeZone(),
        kInputTypes,
        MakeEventTimeOutputTypes());
    EXPECT_THROW(new WindowTableFunctionOperator(badCumulate, nullptr), std::logic_error);

    // timeAttributeIndex out of range of inputTypes.
    auto outOfRangeIndex =
        MakeTumbleDescription("event", 3, shiftTimeZone(), MakeEventTimeOutputTypes());
    EXPECT_THROW(new WindowTableFunctionOperator(outOfRangeIndex, nullptr), std::logic_error);
}
