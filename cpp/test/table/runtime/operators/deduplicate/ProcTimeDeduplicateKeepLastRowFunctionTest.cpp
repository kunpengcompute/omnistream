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

class ProcTimeDeduplicateKeepLastRowFunctionTest : public testing::TestWithParam<bool> {};

TEST_P(ProcTimeDeduplicateKeepLastRowFunctionTest, NormalizesChangesAcrossBatches)
{
    const bool generateUpdateBefore = GetParam();
    const nlohmann::json config = {
        {"inputTypes", {"BIGINT", "BIGINT"}},
        {"grouping", {1}},
        {"generateUpdateBefore", generateUpdateBefore},
    };
    TaskInformationPOD taskInfo;
    taskInfo.setStateBackend("HashMapStateBackend");
    auto streamConfig = taskInfo.getStreamConfigPOD();
    auto operatorDescription = streamConfig.getOperatorDescription();
    operatorDescription.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
    streamConfig.setOperatorDescription(operatorDescription);
    taskInfo.setStreamConfigPOD(streamConfig);

    omnistream::RuntimeEnvironmentV2 env;
    env.SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env.setTaskConfiguration(taskInfo);
    HashMapStateBackend backend;
    StreamTaskStateInitializerImpl initializer(&backend, &env);
    auto* keySerializer = new RowDataSerializer(new omnistream::RowType(true, std::vector<std::string>{"BIGINT"}));
    NormalizeOutput output;
    KeyedProcessOperator<RowData*, VectorBatch*, VectorBatch*> op(
        new ProcTimeDeduplicateKeepLastRowFunction(config), &output, config);
    op.setup();
    op.initializeState(&initializer, keySerializer);
    op.open();

    op.processBatch(makeBatch({{1, 10, RowKind::INSERT, 100}, {2, 20, RowKind::INSERT, 101}}));
    ASSERT_EQ(output.getAll().size(), 1);
    expectChanges(output.getAll().back(), {{1, 10, RowKind::INSERT, 100}, {2, 20, RowKind::INSERT, 101}});

    op.processBatch(makeBatch({{1, 10, RowKind::UPDATE_AFTER, 102}, {3, 0, RowKind::DELETE, 103}}));
    EXPECT_EQ(output.getAll().size(), 1);

    op.processBatch(makeBatch({
        {1, 11, RowKind::INSERT, 104},
        {1, 12, RowKind::UPDATE_AFTER, 105},
        {2, 0, RowKind::DELETE, 106},
        {2, 21, RowKind::INSERT, 107},
    }));
    ASSERT_EQ(output.getAll().size(), 2);
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

INSTANTIATE_TEST_SUITE_P(UpdateBefore, ProcTimeDeduplicateKeepLastRowFunctionTest, testing::Bool());
} // namespace
