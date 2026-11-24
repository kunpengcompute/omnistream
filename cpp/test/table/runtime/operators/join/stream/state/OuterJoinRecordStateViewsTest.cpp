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

#include "table/runtime/operators/join/stream/state/OuterJoinRecordStateViews.h"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "core/typeutils/LongSerializer.h"
#include "runtime/state/DefaultKeyedStateStore.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/KeyGroupRange.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"

namespace omnistream {
namespace {
class OuterJoinRecordStateViewsTest : public testing::Test {
protected:
    void SetUp() override
    {
        range_ = new KeyGroupRange(0, 9);
        keyContext_ = new InternalKeyContextImpl<long>(range_, 10);
        keyContext_->setCurrentKey(1L);
        keySerializer_ = new LongSerializer();
        backend_ = new HeapKeyedStateBackend<long>(keySerializer_, keyContext_);
        stateStore_ = new DefaultKeyedStateStore<long>(backend_);
        runtimeContext_ = new StreamingRuntimeContext<long>(stateStore_, nullptr);
        recordRowType_ = new RowType(false, std::vector<RowField>{RowField("value", BasicLogicalType::BIGINT)});
        recordSerializer_ = new RowDataSerializer(recordRowType_);
        recordType_ = new InternalTypeInfo(recordRowType_, recordSerializer_);
    }

    void TearDown() override
    {
        delete runtimeContext_;
        delete stateStore_;
        delete backend_;
        delete keyContext_;
        delete range_;
        delete recordType_;
        delete recordRowType_;
        // MapStateDescriptor owns recordSerializer_ and is released by backend_.
    }

    static std::shared_ptr<RowData> makeRecord(long value)
    {
        auto* row = BinaryRowData::createBinaryRowDataWithMem(1);
        row->setLong(0, value);
        return std::shared_ptr<RowData>(row);
    }

    static std::vector<long> collectRecords(OuterJoinRecordStateView& view)
    {
        std::vector<long> values;
        auto iterator = view.getRecords();
        while (iterator->hasNext()) {
            auto record = iterator->next();
            values.push_back(*record->getLong(0));
        }
        std::sort(values.begin(), values.end());
        return values;
    }

    static std::vector<std::pair<long, int32_t>> collectRecordsAndAssociations(OuterJoinRecordStateView& view)
    {
        std::vector<std::pair<long, int32_t>> values;
        auto iterator = view.getRecordsAndNumOfAssociations();
        while (iterator->hasNext()) {
            auto [record, associations] = iterator->next();
            values.emplace_back(*record->getLong(0), associations);
        }
        std::sort(values.begin(), values.end());
        return values;
    }

    KeyGroupRange* range_{};
    InternalKeyContextImpl<long>* keyContext_{};
    LongSerializer* keySerializer_{};
    HeapKeyedStateBackend<long>* backend_{};
    DefaultKeyedStateStore<long>* stateStore_{};
    StreamingRuntimeContext<long>* runtimeContext_{};
    RowDataSerializer* recordSerializer_{};
    RowType* recordRowType_{};
    InternalTypeInfo* recordType_{};
};

TEST_F(OuterJoinRecordStateViewsTest, CreatesOuterInputSideHasNoUniqueKeyView)
{
    auto view = OuterJoinRecordStateViews::create<long>(runtimeContext_, "test-records", recordType_);

    EXPECT_EQ(view->getJoinRecordStateViewType(), JoinRecordStateView::OUTER_INPUT_SIDE_HAS_NO_UNIQUE_KEY);
    EXPECT_EQ(view->getStateType(), StateType::HEAP);
    EXPECT_TRUE(JoinRecordStateView::isOuterJoinRecordStateViewType(view->getJoinRecordStateViewType()));
    EXPECT_FALSE(view->getRecords()->hasNext());
    EXPECT_FALSE(view->getRecordsAndNumOfAssociations()->hasNext());
}

TEST_F(OuterJoinRecordStateViewsTest, MaintainsDuplicatesAndAssociationCounts)
{
    auto view = OuterJoinRecordStateViews::create<long>(runtimeContext_, "test-records", recordType_);

    view->addRecord(makeRecord(10), 2);
    view->addRecord(makeRecord(10), 4);
    view->addRecord(makeRecord(20), 7);
    EXPECT_EQ(collectRecordsAndAssociations(*view), (std::vector<std::pair<long, int32_t>>{{10, 4}, {10, 4}, {20, 7}}));

    view->updateNumOfAssociations(makeRecord(10), 9);
    EXPECT_EQ(collectRecordsAndAssociations(*view), (std::vector<std::pair<long, int32_t>>{{10, 9}, {10, 9}, {20, 7}}));

    view->retractRecord(makeRecord(10));
    EXPECT_EQ(collectRecordsAndAssociations(*view), (std::vector<std::pair<long, int32_t>>{{10, 9}, {20, 7}}));
    EXPECT_EQ(collectRecords(*view), (std::vector<long>{10, 20}));

    view->retractRecord(makeRecord(10));
    EXPECT_EQ(collectRecordsAndAssociations(*view), (std::vector<std::pair<long, int32_t>>{{20, 7}}));

    view->retractRecord(makeRecord(10));
    EXPECT_EQ(collectRecordsAndAssociations(*view), (std::vector<std::pair<long, int32_t>>{{20, 7}}));
}

TEST_F(OuterJoinRecordStateViewsTest, UsesDefaultAssociationCountAndRecreatesExpiredRecordOnUpdate)
{
    auto view = OuterJoinRecordStateViews::create<long>(runtimeContext_, "test-records", recordType_);

    view->addRecord(makeRecord(10));
    EXPECT_EQ(collectRecordsAndAssociations(*view), (std::vector<std::pair<long, int32_t>>{{10, -1}}));

    view->updateNumOfAssociations(makeRecord(20), 5);
    EXPECT_EQ(collectRecordsAndAssociations(*view), (std::vector<std::pair<long, int32_t>>{{10, -1}, {20, 5}}));
}
} // namespace
} // namespace omnistream
