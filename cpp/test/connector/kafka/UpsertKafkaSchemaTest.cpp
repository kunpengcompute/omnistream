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
#include "connector/kafka/sink/DynamicKafkaRecordSerializationSchema.h"
#include "connector/kafka/source/reader/deserializer/DynamicKafkaDeserializationSchema.h"
#include "test/core/operators/OutputTest.h"

namespace {
using nlohmann::json;
using omnistream::VectorBatch;
using namespace omniruntime::type;

constexpr int64_t USER_ID = 9007199254740993LL;
const json DESCRIPTION = {
    {"physicalTypes", {"VARCHAR(9)", "BIGINT", "TIMESTAMP(3)", "DOUBLE"}},
    {"outputTypes", {"VARCHAR(9)", "BIGINT", "TIMESTAMP(3)", "DOUBLE"}},
    {"outputNames", {"region", "user_id", "updated_at", "weight"}},
    {"keyProjection", {1}},
    {"keyFieldNames", {"pk_user_id"}},
    {"valueProjection", {3, 0, 2}},
    {"valueFieldNames", {"weight", "region", "updated_at"}},
};

class KafkaMessage : public RdKafka::Message {
public:
    MOCK_METHOD(const std::string*, key, (), (const, override));
    MOCK_METHOD(void*, payload, (), (const, override));
    MOCK_METHOD(size_t, len, (), (const, override));
    MOCK_METHOD(RdKafka::MessageTimestamp, timestamp, (), (const, override));

    std::string errstr() const override
    {
        return {};
    }
    RdKafka::ErrorCode err() const override
    {
        return RdKafka::ERR_NO_ERROR;
    }
    RdKafka::Topic* topic() const override
    {
        return nullptr;
    }
    std::string topic_name() const override
    {
        return {};
    }
    void DeleteMessage(uint64_t*, uint64_t*, uint64_t*) override
    {
    }
    int32_t partition() const override
    {
        return 0;
    }
    const void* key_pointer() const override
    {
        return nullptr;
    }
    size_t key_len() const override
    {
        return 0;
    }
    int64_t offset() const override
    {
        return 0;
    }
    void* msg_opaque() const override
    {
        return nullptr;
    }
    int64_t latency() const override
    {
        return 0;
    }
    rd_kafka_message_s* c_ptr() override
    {
        return nullptr;
    }
    Status status() const override
    {
        return MSG_STATUS_PERSISTED;
    }
    RdKafka::Headers* headers() override
    {
        return nullptr;
    }
    RdKafka::Headers* headers(RdKafka::ErrorCode*) override
    {
        return nullptr;
    }
    int32_t broker_id() const override
    {
        return 0;
    }
    int32_t leader_epoch() const override
    {
        return 0;
    }
    RdKafka::Error* offset_store() override
    {
        return nullptr;
    }
};

TEST(UpsertKafkaSchemaTest, SerializesProjectedKeyValueAndTombstones)
{
    DynamicKafkaRecordSerializationSchema schema(DESCRIPTION);
    std::unique_ptr<VectorBatch> batch(
        VectorBatch::CreateVectorBatch(1, {OMNI_VARCHAR, OMNI_LONG, OMNI_TIMESTAMP_WITHOUT_TIME_ZONE, OMNI_DOUBLE}));
    batch->Get(0)->SetNull(0);
    batch->SetValueAt<int64_t>(1, 0, USER_ID);
    batch->SetValueAt<int64_t>(2, 0, -877);
    batch->SetValueAt<double>(3, 0, 1e7);

    const json expectedKey = {{"pk_user_id", USER_ID}};
    const json expectedValue = {{"weight", 1e7}, {"region", nullptr}, {"updated_at", "1969-12-31 23:59:59.123"}};
    for (auto kind : {RowKind::INSERT, RowKind::UPDATE_AFTER}) {
        batch->setRowKind(0, kind);
        auto record = schema.serializeUpsert(batch.get(), 0);
        EXPECT_EQ(json::parse(record.key), expectedKey);
        ASSERT_TRUE(record.value.has_value());
        EXPECT_EQ(json::parse(*record.value), expectedValue);
    }
    for (auto kind : {RowKind::UPDATE_BEFORE, RowKind::DELETE}) {
        batch->setRowKind(0, kind);
        auto record = schema.serializeUpsert(batch.get(), 0);
        EXPECT_EQ(json::parse(record.key), expectedKey);
        EXPECT_FALSE(record.value.has_value());
    }
}

TEST(UpsertKafkaSchemaTest, DecodesMixedBatchWithTombstoneAndSkipsMissingKeys)
{
    using testing::Return;

    DynamicKafkaDeserializationSchema schema(DESCRIPTION);
    std::string key = R"({"pk_user_id":9007199254740993})";
    // The trailing bytes are outside the Kafka payload.
    std::string value = R"({"weight":1.0E7,"region":null,"updated_at":"1969-12-31 23:59:59.123"}trailing)";
    testing::NiceMock<KafkaMessage> insert;
    ON_CALL(insert, key()).WillByDefault(Return(&key));
    ON_CALL(insert, payload()).WillByDefault(Return(value.data()));
    ON_CALL(insert, len()).WillByDefault(Return(value.find("trailing")));
    ON_CALL(insert, timestamp())
        .WillByDefault(Return(RdKafka::MessageTimestamp{RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 100}));

    testing::NiceMock<KafkaMessage> tombstone;
    ON_CALL(tombstone, key()).WillByDefault(Return(&key));
    ON_CALL(tombstone, timestamp())
        .WillByDefault(Return(RdKafka::MessageTimestamp{RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 200}));

    testing::NiceMock<KafkaMessage> unkeyed;
    TestCollector collector{};
    schema.deserialize(&unkeyed, &collector);
    ASSERT_EQ(collector.collectedData, nullptr);
    schema.deserialize(std::vector<RdKafka::Message*>{&insert, &unkeyed, &tombstone}, &collector);
    std::unique_ptr<VectorBatch> batch(collector.collectedData);
    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 2);
    EXPECT_EQ(batch->GetValueAt<int64_t>(1, 0), USER_ID);
    EXPECT_EQ(batch->GetValueAt<int64_t>(1, 1), USER_ID);
    EXPECT_TRUE(batch->Get(0)->IsNull(0));
    EXPECT_EQ(batch->GetValueAt<int64_t>(2, 0), -877);
    EXPECT_DOUBLE_EQ(batch->GetValueAt<double>(3, 0), 1e7);
    EXPECT_EQ(batch->getRowKind(0), RowKind::INSERT);
    EXPECT_EQ(batch->getTimestamp(0), 100);
    EXPECT_EQ(batch->getRowKind(1), RowKind::DELETE);
    EXPECT_EQ(batch->getTimestamp(1), 200);
    EXPECT_TRUE(batch->Get(0)->IsNull(1));
    EXPECT_TRUE(batch->Get(2)->IsNull(1));
    EXPECT_TRUE(batch->Get(3)->IsNull(1));
}
} // namespace
