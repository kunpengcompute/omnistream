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

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/RowKind.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/misc/DropUpdateBeforeOperator.h"
#include "test/core/operators/OutputTest.h"

namespace {
using omnistream::VectorBatch;
using omniruntime::type::OMNI_LONG;

struct RowChange {
    int64_t id;
    int64_t value;
    bool valueIsNull;
    RowKind kind;
    int64_t timestamp;
};

VectorBatch* makeBatch(std::initializer_list<RowChange> changes)
{
    auto* batch = VectorBatch::CreateVectorBatch(static_cast<int32_t>(changes.size()), {OMNI_LONG, OMNI_LONG});
    int32_t row = 0;
    for (const auto& change : changes) {
        batch->SetValueAt<int64_t>(0, row, change.id);
        batch->SetValueAt<int64_t>(1, row, change.value);
        if (change.valueIsNull) {
            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(1))->SetNull(row);
        }
        batch->setRowKind(row, change.kind);
        batch->setTimestamp(row, change.timestamp);
        ++row;
    }
    return batch;
}

void expectBatch(VectorBatch* batch, const std::vector<RowChange>& expected)
{
    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), static_cast<int32_t>(expected.size()));
    auto* ids = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(0));
    auto* values = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(1));
    for (size_t row = 0; row < expected.size(); ++row) {
        SCOPED_TRACE(row);
        const auto& change = expected[row];
        const auto rowIndex = static_cast<int32_t>(row);
        EXPECT_EQ(ids->GetValue(rowIndex), change.id);
        EXPECT_EQ(batch->getRowKind(row), change.kind);
        EXPECT_EQ(batch->getTimestamp(row), change.timestamp);
        EXPECT_EQ(values->IsNull(rowIndex), change.valueIsNull);
        if (!change.valueIsNull) {
            EXPECT_EQ(values->GetValue(rowIndex), change.value);
        }
    }
}

class DropUpdateBeforeOutput : public OutputTestVectorBatch {
public:
    ~DropUpdateBeforeOutput() override
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

    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        watermarkStatus_ = watermarkStatus;
    }

    WatermarkStatus* getWatermarkStatus() const
    {
        return watermarkStatus_;
    }

private:
    WatermarkStatus* watermarkStatus_ = nullptr;
};

class DropUpdateBeforeOperatorTest : public testing::Test {
protected:
    void SetUp() override
    {
        const auto description = nlohmann::json::parse(R"({"operator":"DropUpdateBefore"})");
        op_ = std::make_unique<omnistream::DropUpdateBeforeOperator>(description, &output_);
        op_->open();
    }

    void TearDown() override
    {
        if (op_ != nullptr) {
            op_->close();
        }
    }

    void process(VectorBatch* batch)
    {
        op_->processBatch(new StreamRecord(batch));
    }

    DropUpdateBeforeOutput output_;
    std::unique_ptr<omnistream::DropUpdateBeforeOperator> op_;
};

TEST_F(DropUpdateBeforeOperatorTest, DropsUpdateBeforeRows)
{
    process(makeBatch({
        {1, 10, false, RowKind::INSERT, 101},
        {2, 20, false, RowKind::UPDATE_BEFORE, 102},
        {2, 21, false, RowKind::UPDATE_AFTER, 103},
        {3, 30, false, RowKind::DELETE, 104},
        {4, 40, false, RowKind::UPDATE_BEFORE, 105},
        {5, 50, false, RowKind::INSERT, 106},
    }));

    ASSERT_EQ(output_.getAll().size(), 1U);
    expectBatch(
        output_.getAll()[0],
        {
            {1, 10, false, RowKind::INSERT, 101},
            {2, 21, false, RowKind::UPDATE_AFTER, 103},
            {3, 30, false, RowKind::DELETE, 104},
            {5, 50, false, RowKind::INSERT, 106},
        });
}

TEST_F(DropUpdateBeforeOperatorTest, ForwardsOriginalBatchWhenNoUpdateBefore)
{
    auto* input = makeBatch({
        {1, 10, false, RowKind::INSERT, 201},
        {2, 20, false, RowKind::UPDATE_AFTER, 202},
        {3, 30, false, RowKind::DELETE, 203},
    });
    process(input);

    ASSERT_EQ(output_.getAll().size(), 1U);
    EXPECT_EQ(output_.getAll()[0], input);
    expectBatch(
        output_.getAll()[0],
        {
            {1, 10, false, RowKind::INSERT, 201},
            {2, 20, false, RowKind::UPDATE_AFTER, 202},
            {3, 30, false, RowKind::DELETE, 203},
        });
}

TEST_F(DropUpdateBeforeOperatorTest, EmitsNothingWhenAllRowsAreUpdateBefore)
{
    process(makeBatch({
        {1, 10, false, RowKind::UPDATE_BEFORE, 301},
        {2, 20, false, RowKind::UPDATE_BEFORE, 302},
        {3, 30, false, RowKind::UPDATE_BEFORE, 303},
    }));

    EXPECT_TRUE(output_.getAll().empty());
}

TEST_F(DropUpdateBeforeOperatorTest, IgnoresEmptyBatch)
{
    process(VectorBatch::CreateVectorBatch(0, {OMNI_LONG, OMNI_LONG}));

    EXPECT_TRUE(output_.getAll().empty());
}

TEST_F(DropUpdateBeforeOperatorTest, PreservesNullValuesForKeptRows)
{
    process(makeBatch({
        {1, 0, true, RowKind::INSERT, 401},
        {2, 0, true, RowKind::UPDATE_BEFORE, 402},
        {3, 30, false, RowKind::UPDATE_AFTER, 403},
        {4, 0, true, RowKind::DELETE, 404},
    }));

    ASSERT_EQ(output_.getAll().size(), 1U);
    expectBatch(
        output_.getAll()[0],
        {
            {1, 0, true, RowKind::INSERT, 401},
            {3, 30, false, RowKind::UPDATE_AFTER, 403},
            {4, 0, true, RowKind::DELETE, 404},
        });
}

TEST_F(DropUpdateBeforeOperatorTest, ProcessesMultipleBatchesIndependently)
{
    process(makeBatch({
        {1, 10, false, RowKind::UPDATE_BEFORE, 501},
        {2, 20, false, RowKind::UPDATE_BEFORE, 502},
    }));
    EXPECT_TRUE(output_.getAll().empty());

    process(makeBatch({
        {1, 10, false, RowKind::UPDATE_BEFORE, 503},
        {1, 11, false, RowKind::UPDATE_AFTER, 504},
        {2, 20, false, RowKind::INSERT, 505},
    }));
    ASSERT_EQ(output_.getAll().size(), 1U);
    expectBatch(
        output_.getAll()[0],
        {
            {1, 11, false, RowKind::UPDATE_AFTER, 504},
            {2, 20, false, RowKind::INSERT, 505},
        });

    auto* input = makeBatch({
        {3, 30, false, RowKind::INSERT, 506},
        {4, 40, false, RowKind::UPDATE_AFTER, 507},
    });
    process(input);
    ASSERT_EQ(output_.getAll().size(), 2U);
    EXPECT_EQ(output_.getAll()[1], input);
    expectBatch(
        output_.getAll()[1],
        {
            {3, 30, false, RowKind::INSERT, 506},
            {4, 40, false, RowKind::UPDATE_AFTER, 507},
        });
}

TEST_F(DropUpdateBeforeOperatorTest, ForwardsWatermark)
{
    const int64_t watermarkTimestamp = 123456789L;
    Watermark watermark(watermarkTimestamp);

    op_->ProcessWatermark(&watermark);

    ASSERT_NE(output_.getWatermark(), nullptr);
    EXPECT_EQ(output_.getWatermark()->getTimestamp(), watermarkTimestamp);
}

TEST_F(DropUpdateBeforeOperatorTest, ForwardsWatermarkStatus)
{
    op_->processWatermarkStatus(WatermarkStatus::idle());

    ASSERT_NE(output_.getWatermarkStatus(), nullptr);
    EXPECT_TRUE(output_.getWatermarkStatus()->IsIdle());
    EXPECT_EQ(output_.getWatermarkStatus()->GetStatus(), WatermarkStatus::idleStatus);

    op_->processWatermarkStatus(WatermarkStatus::active());

    ASSERT_NE(output_.getWatermarkStatus(), nullptr);
    EXPECT_TRUE(output_.getWatermarkStatus()->IsActive());
    EXPECT_EQ(output_.getWatermarkStatus()->GetStatus(), WatermarkStatus::activeStatus);
}

TEST_F(DropUpdateBeforeOperatorTest, HandlesSingleBatchContainingAllRowKinds)
{
    process(makeBatch({
        {1, 10, false, RowKind::INSERT, 601},
        {2, 20, false, RowKind::UPDATE_BEFORE, 602},
        {2, 21, false, RowKind::UPDATE_AFTER, 603},
        {3, 30, false, RowKind::DELETE, 604},
        {4, 0, true, RowKind::UPDATE_BEFORE, 605},
        {5, 50, false, RowKind::INSERT, 606},
        {6, 60, false, RowKind::DELETE, 607},
    }));

    ASSERT_EQ(output_.getAll().size(), 1U);
    expectBatch(
        output_.getAll()[0],
        {
            {1, 10, false, RowKind::INSERT, 601},
            {2, 21, false, RowKind::UPDATE_AFTER, 603},
            {3, 30, false, RowKind::DELETE, 604},
            {5, 50, false, RowKind::INSERT, 606},
            {6, 60, false, RowKind::DELETE, 607},
        });
}

TEST_F(DropUpdateBeforeOperatorTest, NameAndTypeName)
{
    EXPECT_STREQ(op_->getName(), "DropUpdateBefore");
    EXPECT_NE(op_->getTypeName().find("DropUpdateBefore"), std::string::npos);
}
} // namespace
