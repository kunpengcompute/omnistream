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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/GroupAggSavepointAdaptor.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/checkpoint/StreamingJoinSavepointAdaptor.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

namespace {

std::vector<int8_t> copyOutput(DataOutputSerializer& output)
{
    return std::vector<int8_t>(output.getData(), output.getData() + output.getPosition());
}

std::shared_ptr<StateMetaInfoSnapshot> makeKeyValueMeta(
    const std::string& name,
    const std::string& stateType,
    TypeSerializer* valueSerializer,
    TypeSerializer* namespaceSerializer = VoidNamespaceSerializer::INSTANCE)
{
    std::unordered_map<std::string, std::string> options;
    options[StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] = stateType;
    std::unordered_map<std::string, TypeSerializer*> serializers;
    serializers[StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY] = namespaceSerializer;
    serializers[StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY] = valueSerializer;
    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        options,
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        serializers);
}

class SingleEntryIterator final : public KeyValueStateIterator {
public:
    SingleEntryIterator(std::vector<int8_t> keyBytes, std::vector<int8_t> valueBytes, int kvStateId)
        : keyBytes_(std::move(keyBytes)),
          valueBytes_(std::move(valueBytes))
    {
        current_.key = ByteView(keyBytes_.data(), keyBytes_.size());
        current_.value = ByteView(valueBytes_.data(), valueBytes_.size());
        current_.keyGroup = 0;
        current_.kvStateId = kvStateId;
        current_.newKeyGroup = false;
        current_.newKeyValueState = false;
    }

    void next() override
    {
        valid_ = false;
    }

    int keyGroup() const override
    {
        return current_.keyGroup;
    }

    ByteView key() const override
    {
        return current_.key;
    }

    ByteView value() const override
    {
        return current_.value;
    }

    int kvStateId() const override
    {
        return current_.kvStateId;
    }

    const CurrentEntry& current() const override
    {
        return current_;
    }

    bool isNewKeyValueState() const override
    {
        return false;
    }

    bool isNewKeyGroup() const override
    {
        return false;
    }

    bool isValid() const override
    {
        return valid_;
    }

    void close() override
    {
        closed = true;
    }

    bool closed = false;

private:
    std::vector<int8_t> keyBytes_;
    std::vector<int8_t> valueBytes_;
    CurrentEntry current_;
    bool valid_ = true;
};

class RecordingVectorBatchAccessor final : public VectorBatchStateAccessor {
public:
    explicit RecordingVectorBatchAccessor(int arity = 1, int64_t value = 42) : arity_(arity), value_(value)
    {
    }

    bool getSerializedBatch(omnistream::VectorBatchId, ByteView*) override
    {
        return false;
    }

    std::unique_ptr<RowData> getRow(omnistream::VectorBatchId batchId, int32_t rowId) override
    {
        requestedRows.emplace_back(batchId, rowId);
        std::unique_ptr<BinaryRowData> row(BinaryRowData::createBinaryRowDataWithMem(arity_));
        for (int i = 0; i < arity_; ++i) {
            row->setLong(i, value_ + i);
        }
        return row;
    }

    void close() override
    {
        ++closeCalls;
    }

    std::vector<std::pair<omnistream::VectorBatchId, int32_t>> requestedRows;
    int closeCalls = 0;

private:
    int arity_;
    int64_t value_;
};

class TestSnapshotResources : public FullSnapshotResources {
public:
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() override
    {
        return metaInfos;
    }

    KeyGroupRange* getKeyGroupRange() override
    {
        return &keyGroupRange;
    }

    TypeSerializer* getKeySerializer() override
    {
        return nullptr;
    }

    std::shared_ptr<KeyValueStateIterator> createKVStateIterator() override
    {
        return iterator;
    }

    std::shared_ptr<VectorBatchStateAccessor> createVectorBatchStateAccessor(
        const std::string& logicalStateName, const VectorBatchAccessorOptions&) override
    {
        requestedLogicalStateName = logicalStateName;
        return accessor;
    }

    void cleanup() override
    {
    }

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos;
    KeyGroupRange keyGroupRange{0, 0};
    std::shared_ptr<SingleEntryIterator> iterator;
    std::shared_ptr<RecordingVectorBatchAccessor> accessor;
    std::string requestedLogicalStateName;
};

class SavepointAdaptorExecuteSaveTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        bridge_ = std::make_shared<NiceMock<MockSavepointBridge>>();
        ON_CALL(*bridge_, AcquireSavepointOutputStream(_, _)).WillByDefault(Return(kMockProvider));
        ON_CALL(*bridge_, CreateSavepointOutputDirectBuffer(_, _)).WillByDefault(Return(kMockDirectBuffer));
        ON_CALL(*bridge_, GetSavepointOutputStreamPos(_)).WillByDefault(Return(0L));
        savepointType_.reset(SavepointType::savepoint(SavepointFormatType::CANONICAL));
        checkpointOptions_.reset(
            CheckpointOptions::AlignedNoTimeout(*savepointType_, CheckpointStorageLocationReference::GetDefault()));
    }

    size_t saveAndGetPosition(omnistream::OperatorSavepointAdaptor& adaptor, FullSnapshotResources& resources)
    {
        CheckpointStateOutputStreamProxy stream(bridge_, 1L, checkpointOptions_.get());
        KeyGroupRangeOffsets offsets(*resources.getKeyGroupRange());
        adaptor.save(stream, offsets, resources, "key-serializer");
        return stream.getPos();
    }

    std::shared_ptr<NiceMock<MockSavepointBridge>> bridge_;
    std::unique_ptr<SavepointType> savepointType_;
    std::unique_ptr<CheckpointOptions> checkpointOptions_;
};

TEST_F(SavepointAdaptorExecuteSaveTest, GroupAggSaveExecutesAccumulatorExpansion)
{
    omnistream::GroupAggSavepointAdaptor adaptor;
    adaptor.prepareForSave({{"aggInfoList", {{"accTypes", {"RAW('example.DataView', '')", "BIGINT"}}}}});
    omnistream::RowType rowType(true, std::vector<std::string>{"BIGINT"});
    RowDataSerializer serializer(&rowType);
    std::unique_ptr<BinaryRowData> row(BinaryRowData::createBinaryRowDataWithMem(1));
    row->setLong(0, 37);
    DataOutputSerializer valueOutput(128);
    serializer.serialize(row.get(), valueOutput);
    auto iterator = std::make_shared<SingleEntryIterator>(std::vector<int8_t>{0x01}, copyOutput(valueOutput), 0);
    TestSnapshotResources resources;
    resources.metaInfos = {makeKeyValueMeta("accState", "VALUE", &serializer)};
    resources.iterator = iterator;

    EXPECT_GT(saveAndGetPosition(adaptor, resources), 0U);
    EXPECT_TRUE(iterator->closed);
    EXPECT_TRUE(resources.requestedLogicalStateName.empty());
}

TEST_F(SavepointAdaptorExecuteSaveTest, GroupAggSaveRejectsMalformedAccumulator)
{
    omnistream::GroupAggSavepointAdaptor adaptor;
    adaptor.prepareForSave({{"aggInfoList", {{"accTypes", {"BIGINT", "RAW('example.DataView', '')"}}}}});
    auto iterator =
        std::make_shared<SingleEntryIterator>(std::vector<int8_t>{0x01}, std::vector<int8_t>{0x01, 0x02, 0x03}, 0);
    TestSnapshotResources resources;
    resources.metaInfos = {makeKeyValueMeta("accState", "VALUE", LongSerializer::INSTANCE)};
    resources.iterator = iterator;

    EXPECT_THROW(saveAndGetPosition(adaptor, resources), std::exception);
    EXPECT_TRUE(iterator->closed);
}

TEST_F(SavepointAdaptorExecuteSaveTest, StreamingJoinSaveExecutesConvertKVRowData)
{
    omnistream::StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    adaptor.prepareForSave({{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", {"BIGINT"}}});
    std::vector<int8_t> keyBytes{0x01};
    DataOutputSerializer valueOutput;
    OutputBufferStatus valueOutputStatus{};
    valueOutput.setBackendBuffer(&valueOutputStatus);
    valueOutput.writeBoolean(false);
    valueOutput.writeInt(3);
    auto valueBytes = copyOutput(valueOutput);
    auto iterator = std::make_shared<SingleEntryIterator>(std::move(keyBytes), std::move(valueBytes), 0);
    auto accessor = std::make_shared<RecordingVectorBatchAccessor>();
    TestSnapshotResources resources;
    resources.metaInfos = {
        makeKeyValueMeta(omnistream::StreamingJoinSavepointUtil::LEFT_STATE_NAME, "MAP", LongSerializer::INSTANCE)};
    resources.iterator = iterator;
    resources.accessor = accessor;

    EXPECT_GT(saveAndGetPosition(adaptor, resources), 0U);
    EXPECT_TRUE(iterator->closed);
    EXPECT_TRUE(accessor->requestedRows.empty());
    EXPECT_EQ(accessor->closeCalls, 0);
}

} // namespace
