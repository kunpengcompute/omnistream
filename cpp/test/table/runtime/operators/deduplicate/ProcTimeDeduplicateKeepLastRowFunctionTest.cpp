/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "table/runtime/operators/deduplicate/ProcTimeDeduplicateKeepLastRowFunction.h"
#include "table/typeutils/RowDataSerializer.h"
#include "test/core/operators/OutputTest.h"

namespace {
using omnistream::VectorBatch;
using omniruntime::type::OMNI_LONG;
using omniruntime::type::OMNI_TIME_WITHOUT_TIME_ZONE;
using omniruntime::type::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE;

class NormalizeOutput : public OutputTestVectorBatch {
public:
    ~NormalizeOutput() override
    {
        for (auto* batch : getAll()) {
            delete batch;
        }
    }

    void collect(void* value) override
    {
        std::unique_ptr<StreamRecord> record(static_cast<StreamRecord*>(value));
        OutputTestVectorBatch::collect(record.get());
    }
};

struct Change {
    int64_t key;
    int64_t value;
    RowKind kind;
    int64_t timestamp;
};

StreamRecord* makeBatch(std::initializer_list<Change> changes)
{
    auto* batch = VectorBatch::CreateVectorBatch(changes.size(), {OMNI_LONG, OMNI_LONG});
    int row = 0;
    for (const auto& change : changes) {
        batch->SetValueAt<int64_t>(0, row, change.value);
        batch->SetValueAt<int64_t>(1, row, change.key);
        batch->setRowKind(row, change.kind);
        batch->setTimestamp(row, change.timestamp);
        ++row;
    }
    return new StreamRecord(batch);
}

// Tombstone batch: the non-key column is NULL, only the key is set. Mirrors the Kafka
// tombstone DELETE rows that ChangelogNormalize must resolve against the stored full row.
StreamRecord* makeTombstoneBatch(std::initializer_list<Change> changes)
{
    auto* batch = VectorBatch::CreateVectorBatch(changes.size(), {OMNI_LONG, OMNI_LONG});
    int row = 0;
    for (const auto& change : changes) {
        batch->Get(0)->SetNull(row);
        batch->SetValueAt<int64_t>(1, row, change.key);
        batch->setRowKind(row, change.kind);
        batch->setTimestamp(row, change.timestamp);
        ++row;
    }
    return new StreamRecord(batch);
}

void expectChanges(VectorBatch* batch, const std::vector<Change>& changes)
{
    ASSERT_EQ(batch->GetRowCount(), changes.size());
    for (size_t row = 0; row < changes.size(); ++row) {
        SCOPED_TRACE(row);
        EXPECT_EQ(batch->GetValueAt<int64_t>(1, row), changes[row].key);
        EXPECT_EQ(batch->GetValueAt<int64_t>(0, row), changes[row].value);
        EXPECT_EQ(batch->getRowKind(row), changes[row].kind);
        EXPECT_EQ(batch->getTimestamp(row), changes[row].timestamp);
    }
}

struct TimeChange {
    int64_t key;
    int64_t time;
    int64_t ts;
    RowKind kind;
    int64_t timestamp;
};

StreamRecord* makeTimeBatch(std::initializer_list<TimeChange> changes)
{
    auto* batch = VectorBatch::CreateVectorBatch(
        changes.size(), {OMNI_LONG, OMNI_TIME_WITHOUT_TIME_ZONE, OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE});
    int row = 0;
    for (const auto& change : changes) {
        batch->SetValueAt<int64_t>(0, row, change.key);
        batch->SetValueAt<int64_t>(1, row, change.time);
        batch->SetValueAt<int64_t>(2, row, change.ts);
        batch->setRowKind(row, change.kind);
        batch->setTimestamp(row, change.timestamp);
        ++row;
    }
    return new StreamRecord(batch);
}

void expectTimeChanges(VectorBatch* batch, const std::vector<TimeChange>& changes)
{
    ASSERT_EQ(batch->GetRowCount(), changes.size());
    for (size_t row = 0; row < changes.size(); ++row) {
        SCOPED_TRACE(row);
        EXPECT_EQ(batch->GetValueAt<int64_t>(0, row), changes[row].key);
        EXPECT_EQ(batch->GetValueAt<int64_t>(1, row), changes[row].time);
        EXPECT_EQ(batch->GetValueAt<int64_t>(2, row), changes[row].ts);
        EXPECT_EQ(batch->getRowKind(row), changes[row].kind);
        EXPECT_EQ(batch->getTimestamp(row), changes[row].timestamp);
    }
}

// ChangelogNormalize (inputInsertOnly=false) semantics, aligned with Flink's
// DeduplicateFunctionHelper.processLastRowOnChangelog (state TTL disabled). Flink's generated
// RecordEqualiser is rowKind-sensitive (the BinaryRowData fast path memcmp's the whole row and
// the generic path checks getRowKind()), so only a same-content INSERT is suppressed; a
// same-content UPDATE_AFTER (INSERT state vs UA input) is forwarded as UPDATE_BEFORE/UPDATE_AFTER.
class ProcTimeDeduplicateKeepLastRowFunctionTest : public testing::TestWithParam<bool> {
protected:
    void createOperator(const nlohmann::json& config)
    {
        taskInfo.setStateBackend("HashMapStateBackend");
        auto streamConfig = taskInfo.getStreamConfigPOD();
        auto operatorDescription = streamConfig.getOperatorDescription();
        operatorDescription.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        streamConfig.setOperatorDescription(operatorDescription);
        taskInfo.setStreamConfigPOD(streamConfig);

        env.SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
        env.setTaskConfiguration(taskInfo);
        initializer = std::make_unique<StreamTaskStateInitializerImpl>(&backend, &env);
        auto* keySerializer = new RowDataSerializer(new omnistream::RowType(true, std::vector<std::string>{"BIGINT"}));
        op = std::make_unique<KeyedProcessOperator<RowData*, VectorBatch*, VectorBatch*>>(
            new ProcTimeDeduplicateKeepLastRowFunction(config), &output, config);
        op->setup();
        op->initializeState(initializer.get(), keySerializer);
        op->open();
    }

    TaskInformationPOD taskInfo;
    omnistream::RuntimeEnvironmentV2 env;
    HashMapStateBackend backend;
    std::unique_ptr<StreamTaskStateInitializerImpl> initializer;
    NormalizeOutput output;
    std::unique_ptr<KeyedProcessOperator<RowData*, VectorBatch*, VectorBatch*>> op;
};

TEST_P(ProcTimeDeduplicateKeepLastRowFunctionTest, NormalizesChangesAcrossBatches)
{
    const bool generateUpdateBefore = GetParam();
    createOperator({{"inputTypes", {"BIGINT", "BIGINT"}}, {"grouping", {1}}, {"generateUpdateBefore", generateUpdateBefore}});

    op->processBatch(makeBatch({{1, 10, RowKind::INSERT, 100}, {2, 20, RowKind::INSERT, 101}}));
    ASSERT_EQ(output.getAll().size(), 1);
    expectChanges(output.getAll().back(), {{1, 10, RowKind::INSERT, 100}, {2, 20, RowKind::INSERT, 101}});

    // Same-content UPDATE_AFTER is forwarded (Flink equaliser compares rowKind, so INSERT state
    // vs UPDATE_AFTER input is not equal); DELETE of an unknown key produces nothing.
    op->processBatch(makeBatch({{1, 10, RowKind::UPDATE_AFTER, 102}, {3, 0, RowKind::DELETE, 103}}));
    ASSERT_EQ(output.getAll().size(), 2);
    if (generateUpdateBefore) {
        expectChanges(
            output.getAll().back(), {{1, 10, RowKind::UPDATE_BEFORE, 102}, {1, 10, RowKind::UPDATE_AFTER, 102}});
    } else {
        expectChanges(output.getAll().back(), {{1, 10, RowKind::UPDATE_AFTER, 102}});
    }

    // Same-content INSERT is suppressed (state and input are both INSERT kind).
    op->processBatch(makeBatch({{1, 10, RowKind::INSERT, 108}}));
    EXPECT_EQ(output.getAll().size(), 2);

    op->processBatch(makeBatch({
        {1, 11, RowKind::INSERT, 104},
        {1, 12, RowKind::UPDATE_AFTER, 105},
        {2, 0, RowKind::DELETE, 106},
        {2, 21, RowKind::INSERT, 107},
    }));
    ASSERT_EQ(output.getAll().size(), 3);
    if (generateUpdateBefore) {
        expectChanges(
            output.getAll().back(),
            {
                {1, 10, RowKind::UPDATE_BEFORE, 104},
                {1, 11, RowKind::UPDATE_AFTER, 104},
                {1, 11, RowKind::UPDATE_BEFORE, 105},
                {1, 12, RowKind::UPDATE_AFTER, 105},
                {2, 20, RowKind::DELETE, 106},
                {2, 21, RowKind::INSERT, 107},
            });
    } else {
        expectChanges(
            output.getAll().back(),
            {
                {1, 11, RowKind::UPDATE_AFTER, 104},
                {1, 12, RowKind::UPDATE_AFTER, 105},
                {2, 20, RowKind::DELETE, 106},
                {2, 21, RowKind::INSERT, 107},
            });
    }
}

TEST_P(ProcTimeDeduplicateKeepLastRowFunctionTest, NormalizesTimeAndTimestampLtzNonKeyColumns)
{
    const bool generateUpdateBefore = GetParam();
    // Non-key columns use the same time-derived types as key columns; the operator must
    // normalize them through the OMNI_LONG millis representation.
    createOperator({{"inputTypes", {"BIGINT", "TIME_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"}},
                    {"grouping", {0}},
                    {"generateUpdateBefore", generateUpdateBefore}});

    op->processBatch(makeTimeBatch({
        {1, 1000, 1000000, RowKind::INSERT, 100},
        {2, 2000, 2000000, RowKind::INSERT, 101},
    }));
    ASSERT_EQ(output.getAll().size(), 1);
    expectTimeChanges(
        output.getAll().back(), {{1, 1000, 1000000, RowKind::INSERT, 100}, {2, 2000, 2000000, RowKind::INSERT, 101}});

    // key1 keeps the same time-derived values but its input is UPDATE_AFTER while the state is
    // INSERT: forwarded (Flink equaliser compares rowKind). key2 changes: retracted.
    op->processBatch(makeTimeBatch({
        {1, 1000, 1000000, RowKind::UPDATE_AFTER, 102},
        {2, 3000, 3000000, RowKind::UPDATE_AFTER, 103},
    }));
    ASSERT_EQ(output.getAll().size(), 2);
    if (generateUpdateBefore) {
        expectTimeChanges(output.getAll().back(), {
            {1, 1000, 1000000, RowKind::UPDATE_BEFORE, 102},
            {1, 1000, 1000000, RowKind::UPDATE_AFTER, 102},
            {2, 2000, 2000000, RowKind::UPDATE_BEFORE, 103},
            {2, 3000, 3000000, RowKind::UPDATE_AFTER, 103},
        });
    } else {
        expectTimeChanges(output.getAll().back(), {
            {1, 1000, 1000000, RowKind::UPDATE_AFTER, 102},
            {2, 3000, 3000000, RowKind::UPDATE_AFTER, 103},
        });
    }

    // DELETE with zeroed non-key columns emits the stored full row.
    op->processBatch(makeTimeBatch({{1, 0, 0, RowKind::DELETE, 104}}));
    ASSERT_EQ(output.getAll().size(), 3);
    expectTimeChanges(output.getAll().back(), {{1, 1000, 1000000, RowKind::DELETE, 104}});
}

TEST_P(ProcTimeDeduplicateKeepLastRowFunctionTest, DuplicateInsertSuppressedAndSameContentUpdateAfterForwarded)
{
    const bool generateUpdateBefore = GetParam();
    createOperator({{"inputTypes", {"BIGINT", "BIGINT"}}, {"grouping", {1}}, {"generateUpdateBefore", generateUpdateBefore}});

    op->processBatch(makeBatch({{1, 10, RowKind::INSERT, 100}}));
    ASSERT_EQ(output.getAll().size(), 1);
    expectChanges(output.getAll().back(), {{1, 10, RowKind::INSERT, 100}});

    // Identical INSERT is suppressed (state and input are both INSERT kind). An identical
    // UPDATE_AFTER is NOT suppressed because Flink's generated equaliser is rowKind-sensitive.
    op->processBatch(makeBatch({{1, 10, RowKind::INSERT, 101}}));
    EXPECT_EQ(output.getAll().size(), 1);
    op->processBatch(makeBatch({{1, 10, RowKind::UPDATE_AFTER, 102}}));
    ASSERT_EQ(output.getAll().size(), 2);
    if (generateUpdateBefore) {
        expectChanges(
            output.getAll().back(), {{1, 10, RowKind::UPDATE_BEFORE, 102}, {1, 10, RowKind::UPDATE_AFTER, 102}});
    } else {
        expectChanges(output.getAll().back(), {{1, 10, RowKind::UPDATE_AFTER, 102}});
    }

    op->processBatch(makeBatch({{1, 11, RowKind::UPDATE_AFTER, 103}}));
    ASSERT_EQ(output.getAll().size(), 3);
    if (generateUpdateBefore) {
        expectChanges(output.getAll().back(), {
            {1, 10, RowKind::UPDATE_BEFORE, 103},
            {1, 11, RowKind::UPDATE_AFTER, 103},
        });
    } else {
        expectChanges(output.getAll().back(), {{1, 11, RowKind::UPDATE_AFTER, 103}});
    }
}

TEST_P(ProcTimeDeduplicateKeepLastRowFunctionTest, TombstoneDeleteEmitsStoredFullRow)
{
    createOperator({{"inputTypes", {"BIGINT", "BIGINT"}}, {"grouping", {1}}, {"generateUpdateBefore", GetParam()}});

    op->processBatch(makeBatch({{1, 10, RowKind::INSERT, 100}, {2, 20, RowKind::INSERT, 101}}));
    ASSERT_EQ(output.getAll().size(), 1);
    expectChanges(output.getAll().back(), {{1, 10, RowKind::INSERT, 100}, {2, 20, RowKind::INSERT, 101}});

    // A tombstone DELETE carries only the key; the emitted DELETE must be the stored full row.
    op->processBatch(makeTombstoneBatch({{1, 0, RowKind::DELETE, 102}, {2, 0, RowKind::DELETE, 103}}));
    ASSERT_EQ(output.getAll().size(), 2);
    expectChanges(output.getAll().back(), {{1, 10, RowKind::DELETE, 102}, {2, 20, RowKind::DELETE, 103}});

    // DELETE for a never-seen key produces nothing.
    op->processBatch(makeTombstoneBatch({{3, 0, RowKind::DELETE, 104}}));
    EXPECT_EQ(output.getAll().size(), 2);
}

TEST_P(ProcTimeDeduplicateKeepLastRowFunctionTest, IgnoresDeleteAndUpdateBeforeForUnknownKey)
{
    createOperator({{"inputTypes", {"BIGINT", "BIGINT"}}, {"grouping", {1}}, {"generateUpdateBefore", GetParam()}});

    op->processBatch(makeBatch({{5, 0, RowKind::DELETE, 100}, {6, 0, RowKind::UPDATE_BEFORE, 101}}));
    EXPECT_EQ(output.getAll().size(), 0);
}

TEST_P(ProcTimeDeduplicateKeepLastRowFunctionTest, OrphanUpdateBeforeEmitsDelete)
{
    createOperator({{"inputTypes", {"BIGINT", "BIGINT"}}, {"grouping", {1}}, {"generateUpdateBefore", GetParam()}});

    op->processBatch(makeBatch({{1, 10, RowKind::INSERT, 100}}));
    ASSERT_EQ(output.getAll().size(), 1);
    expectChanges(output.getAll().back(), {{1, 10, RowKind::INSERT, 100}});

    // UPDATE_BEFORE without a following UPDATE_AFTER is treated as a removal: emit DELETE with
    // the stored full row (not the incoming row content).
    op->processBatch(makeBatch({{1, 99, RowKind::UPDATE_BEFORE, 101}}));
    ASSERT_EQ(output.getAll().size(), 2);
    expectChanges(output.getAll().back(), {{1, 10, RowKind::DELETE, 101}});

    // State is cleared, so the next INSERT starts fresh.
    op->processBatch(makeBatch({{1, 11, RowKind::INSERT, 102}}));
    ASSERT_EQ(output.getAll().size(), 3);
    expectChanges(output.getAll().back(), {{1, 11, RowKind::INSERT, 102}});
}

TEST_P(ProcTimeDeduplicateKeepLastRowFunctionTest, InsertAfterDeleteStartsFresh)
{
    createOperator({{"inputTypes", {"BIGINT", "BIGINT"}}, {"grouping", {1}}, {"generateUpdateBefore", GetParam()}});

    op->processBatch(makeBatch({{1, 10, RowKind::INSERT, 100}}));
    op->processBatch(makeBatch({{1, 0, RowKind::DELETE, 101}}));
    ASSERT_EQ(output.getAll().size(), 2);
    expectChanges(output.getAll().back(), {{1, 10, RowKind::DELETE, 101}});

    // Re-insert after delete is a brand new INSERT, not an UPDATE_AFTER.
    op->processBatch(makeBatch({{1, 12, RowKind::INSERT, 102}}));
    ASSERT_EQ(output.getAll().size(), 3);
    expectChanges(output.getAll().back(), {{1, 12, RowKind::INSERT, 102}});
}

INSTANTIATE_TEST_SUITE_P(UpdateBefore, ProcTimeDeduplicateKeepLastRowFunctionTest, testing::Bool());
} // namespace
