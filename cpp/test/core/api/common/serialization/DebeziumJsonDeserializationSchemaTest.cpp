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

#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/api/common/serialization/DebeziumJsonDeserializationSchema.h"
#include "functions/Collector.h"

namespace {

using nlohmann::json;
using omniruntime::vec::LargeStringContainer;
using omniruntime::vec::Vector;
using omnistream::VectorBatch;

constexpr int64_t TS_MS = 1704164645006L;
constexpr int32_t DATE_EPOCH_DAYS = 19724;
constexpr int64_t TIME_MILLIS_OF_DAY = 45296000L;

class CollectingCollector : public Collector {
public:
    void collect(void* record) override
    {
        batches.emplace_back(static_cast<VectorBatch*>(record));
    }

    void close() override
    {
    }

    std::vector<std::unique_ptr<VectorBatch>> batches;
};

json MakeDescription(const std::vector<std::string>& names, const std::vector<std::string>& types,
    bool schemaInclude = false, bool ignoreParseErrors = false,
    const std::vector<std::string>& metadata = {})
{
    json description;
    description["outputNames"] = names;
    description["outputTypes"] = types;
    description["debeziumSchemaInclude"] = schemaInclude;
    description["debeziumIgnoreParseErrors"] = ignoreParseErrors;
    description["debeziumMetadata"] = metadata;
    return description;
}

json AllTypesDescription(bool schemaInclude = false, bool ignoreParseErrors = false)
{
    return MakeDescription(
        {"c", "bi", "i", "vc", "bo", "s", "d", "t", "ts", "tsltz"},
        {"CHAR", "BIGINT", "INTEGER", "VARCHAR(20)", "BOOLEAN", "VARCHAR(2147483647)", "DATE",
            "TIME_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"},
        schemaInclude,
        ignoreParseErrors);
}

std::unique_ptr<VectorBatch> Decode(DebeziumJsonDeserializationSchema& schema, const std::string& message)
{
    CollectingCollector collector;
    const auto* data = reinterpret_cast<const uint8_t*>(message.data());
    schema.deserialize(data, message.size(), &collector);
    if (collector.batches.empty()) {
        return nullptr;
    }
    return std::move(collector.batches.front());
}

size_t DecodeCount(DebeziumJsonDeserializationSchema& schema, const std::string& message)
{
    CollectingCollector collector;
    const auto* data = reinterpret_cast<const uint8_t*>(message.data());
    schema.deserialize(data, message.size(), &collector);
    return collector.batches.size();
}

std::string GetString(VectorBatch* batch, int column, int row)
{
    auto* vector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(batch->Get(column));
    return std::string(vector->GetValue(row));
}

std::string InsertMessage()
{
    return R"({"before":null,"after":{"c":"A","bi":9007199254740993,"i":42,"vc":"hello","bo":true,"s":"world","d":"2024-01-02","t":"12:34:56","ts":"2024-01-02 03:04:05.006","tsltz":"2024-01-02T03:04:05.006Z"},"source":{"db":"inventory","schema":"inventory","table":"products","ts_ms":1704164645006},"op":"c","ts_ms":1704164645006})";
}

void ExpectAllTypesRow(VectorBatch* batch, int row, int32_t id, const std::string& text)
{
    EXPECT_EQ(batch->GetValueAt<int64_t>(1, row), 9007199254740993LL);
    EXPECT_EQ(batch->GetValueAt<int32_t>(2, row), id);
    EXPECT_EQ(GetString(batch, 3, row), text);
}

} // namespace

TEST(DebeziumJsonDeserializationSchemaTest, DecodesAllSupportedTypes)
{
    json description = AllTypesDescription();
    DebeziumJsonDeserializationSchema schema(description);
    auto batch = Decode(schema, InsertMessage());

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(batch->getRowKind(0), RowKind::INSERT);
    EXPECT_EQ(GetString(batch.get(), 0, 0), "A");
    ExpectAllTypesRow(batch.get(), 0, 42, "hello");
    EXPECT_EQ(batch->GetValueAt<bool>(4, 0), true);
    EXPECT_EQ(GetString(batch.get(), 5, 0), "world");
    EXPECT_EQ(batch->GetValueAt<int32_t>(6, 0), DATE_EPOCH_DAYS);
    EXPECT_EQ(batch->GetValueAt<int64_t>(7, 0), TIME_MILLIS_OF_DAY);
    EXPECT_EQ(batch->GetValueAt<int64_t>(8, 0), TS_MS);
    EXPECT_EQ(batch->GetValueAt<int64_t>(9, 0), TS_MS);
}

TEST(DebeziumJsonDeserializationSchemaTest, UpdateEmitsBeforeThenAfter)
{
    json description = AllTypesDescription();
    DebeziumJsonDeserializationSchema schema(description);
    const std::string message = R"({"before":{"c":"A","bi":1,"i":1,"vc":"old","bo":false,"s":"before","d":"2024-01-01","t":"00:00:00","ts":"2024-01-01 00:00:00.000","tsltz":"2024-01-01T00:00:00.000Z"},"after":{"c":"B","bi":2,"i":2,"vc":"new","bo":true,"s":"after","d":"2024-01-02","t":"12:34:56","ts":"2024-01-02 03:04:05.006","tsltz":"2024-01-02T03:04:05.006Z"},"op":"u","ts_ms":1704164645006})";
    auto batch = Decode(schema, message);

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 2);
    EXPECT_EQ(batch->getRowKind(0), RowKind::UPDATE_BEFORE);
    EXPECT_EQ(batch->getRowKind(1), RowKind::UPDATE_AFTER);
    EXPECT_EQ(GetString(batch.get(), 0, 0), "A");
    EXPECT_EQ(batch->GetValueAt<int32_t>(2, 0), 1);
    EXPECT_EQ(GetString(batch.get(), 3, 0), "old");
    EXPECT_EQ(GetString(batch.get(), 0, 1), "B");
    EXPECT_EQ(batch->GetValueAt<int32_t>(2, 1), 2);
    EXPECT_EQ(GetString(batch.get(), 3, 1), "new");
}

TEST(DebeziumJsonDeserializationSchemaTest, DeleteUsesBefore)
{
    json description = AllTypesDescription();
    DebeziumJsonDeserializationSchema schema(description);
    const std::string message = R"({"before":{"c":"A","bi":7,"i":7,"vc":"gone","bo":true,"s":"gone","d":"2024-01-02","t":"12:34:56","ts":"2024-01-02 03:04:05.006","tsltz":"2024-01-02T03:04:05.006Z"},"after":null,"op":"d","ts_ms":1704164645006})";
    auto batch = Decode(schema, message);

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(batch->getRowKind(0), RowKind::DELETE);
    EXPECT_EQ(GetString(batch.get(), 3, 0), "gone");
    EXPECT_EQ(batch->GetValueAt<int32_t>(2, 0), 7);
}

TEST(DebeziumJsonDeserializationSchemaTest, SnapshotReadEmitsInsert)
{
    json description = AllTypesDescription();
    DebeziumJsonDeserializationSchema schema(description);
    std::string message = InsertMessage();
    message.replace(message.find("\"op\":\"c\""), 8, "\"op\":\"r\"");
    auto batch = Decode(schema, message);

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(batch->getRowKind(0), RowKind::INSERT);
}

TEST(DebeziumJsonDeserializationSchemaTest, SkipsTombstoneMessages)
{
    json description = AllTypesDescription();
    DebeziumJsonDeserializationSchema schema(description);
    CollectingCollector collector;
    schema.deserialize(nullptr, 0, &collector);
    schema.deserialize(reinterpret_cast<const uint8_t*>(""), 0, &collector);
    EXPECT_TRUE(collector.batches.empty());
}

TEST(DebeziumJsonDeserializationSchemaTest, DecodesSchemaIncludedEnvelope)
{
    json description = AllTypesDescription(true);
    DebeziumJsonDeserializationSchema schema(description);
    const std::string message = R"({"schema":{"type":"struct"},"payload":{"before":null,"after":{"c":"A","bi":9007199254740993,"i":42,"vc":"hello","bo":true,"s":"world","d":"2024-01-02","t":"12:34:56","ts":"2024-01-02 03:04:05.006","tsltz":"2024-01-02T03:04:05.006Z"},"source":{"db":"inventory","schema":"inventory","table":"products","ts_ms":1704164645006},"op":"c","ts_ms":1704164645006}})";
    auto batch = Decode(schema, message);

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(batch->getRowKind(0), RowKind::INSERT);
    EXPECT_EQ(GetString(batch.get(), 0, 0), "A");
    EXPECT_EQ(batch->GetValueAt<int64_t>(8, 0), TS_MS);
}

TEST(DebeziumJsonDeserializationSchemaTest, MissingAndNullFieldsBecomeNull)
{
    json description = AllTypesDescription();
    DebeziumJsonDeserializationSchema schema(description);
    const std::string message = R"({"before":null,"after":{"i":42,"s":null},"op":"c","ts_ms":1})";
    auto batch = Decode(schema, message);

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_TRUE(batch->Get(0)->IsNull(0));
    EXPECT_TRUE(batch->Get(1)->IsNull(0));
    EXPECT_EQ(batch->GetValueAt<int32_t>(2, 0), 42);
    EXPECT_TRUE(batch->Get(3)->IsNull(0));
    EXPECT_TRUE(batch->Get(4)->IsNull(0));
    EXPECT_TRUE(batch->Get(5)->IsNull(0));
    EXPECT_TRUE(batch->Get(6)->IsNull(0));
    EXPECT_TRUE(batch->Get(7)->IsNull(0));
    EXPECT_TRUE(batch->Get(8)->IsNull(0));
    EXPECT_TRUE(batch->Get(9)->IsNull(0));
}

TEST(DebeziumJsonDeserializationSchemaTest, IgnoreParseErrorsSetsFieldToNull)
{
    json description = AllTypesDescription(false, true);
    DebeziumJsonDeserializationSchema schema(description);
    const std::string message = R"({"before":null,"after":{"c":"A","bi":1,"i":"oops","vc":"hello","bo":true,"s":"world","d":"2024-01-02","t":"12:34:56","ts":"2024-01-02 03:04:05.006","tsltz":"2024-01-02T03:04:05.006Z"},"op":"c","ts_ms":1})";
    auto batch = Decode(schema, message);

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_TRUE(batch->Get(2)->IsNull(0));
    EXPECT_EQ(GetString(batch.get(), 3, 0), "hello");
    EXPECT_EQ(GetString(batch.get(), 0, 0), "A");
}

TEST(DebeziumJsonDeserializationSchemaTest, FailOnParseErrorsThrows)
{
    json description = AllTypesDescription(false, false);
    DebeziumJsonDeserializationSchema schema(description);
    const std::string message = R"({"before":null,"after":{"c":"A","bi":1,"i":"oops","vc":"hello","bo":true,"s":"world","d":"2024-01-02","t":"12:34:56","ts":"2024-01-02 03:04:05.006","tsltz":"2024-01-02T03:04:05.006Z"},"op":"c","ts_ms":1})";
    EXPECT_THROW(Decode(schema, message), std::exception);
}

TEST(DebeziumJsonDeserializationSchemaTest, ReplicaIdentityErrorHonoursIgnoreParseErrors)
{
    const std::string message = R"({"before":null,"after":{"i":1},"op":"u","ts_ms":1})";

    DebeziumJsonDeserializationSchema failing(AllTypesDescription(false, false));
    EXPECT_THROW(Decode(failing, message), std::exception);

    DebeziumJsonDeserializationSchema ignoring(AllTypesDescription(false, true));
    EXPECT_EQ(DecodeCount(ignoring, message), 0);
}

TEST(DebeziumJsonDeserializationSchemaTest, NonObjectBeforeAfterFollowsFlinkSemantics)
{
    const std::string deleteMessage = R"({"before":"oops","after":null,"op":"d","ts_ms":1})";
    DebeziumJsonDeserializationSchema failing(AllTypesDescription(false, false));
    EXPECT_THROW(Decode(failing, deleteMessage), std::exception);

    DebeziumJsonDeserializationSchema ignoring(AllTypesDescription(false, true));
    EXPECT_EQ(DecodeCount(ignoring, deleteMessage), 0);

    const std::string insertMessage = R"({"before":"oops","after":{"i":1},"op":"c","ts_ms":1})";
    DebeziumJsonDeserializationSchema ignoringInsert(AllTypesDescription(false, true));
    auto batch = Decode(ignoringInsert, insertMessage);
    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(batch->GetValueAt<int32_t>(2, 0), 1);
}

TEST(DebeziumJsonDeserializationSchemaTest, FailsOnInvalidUnusedBefore)
{
    const std::string message =
        R"({"before":{"i":"oops"},"after":{"i":1},"op":"c","ts_ms":1})";
    DebeziumJsonDeserializationSchema schema(AllTypesDescription(false, false));
    EXPECT_THROW(Decode(schema, message), std::exception);
}

TEST(DebeziumJsonDeserializationSchemaTest, FailsOnInvalidUnusedAfter)
{
    const std::string message =
        R"({"before":{"i":1},"after":{"i":"oops"},"op":"d","ts_ms":1})";
    DebeziumJsonDeserializationSchema schema(AllTypesDescription(false, false));
    EXPECT_THROW(Decode(schema, message), std::exception);
}

TEST(DebeziumJsonDeserializationSchemaTest, IgnoresInvalidUnusedBeforeWhenIgnoringParseErrors)
{
    const std::string message =
        R"({"before":{"i":"oops"},"after":{"i":1},"op":"c","ts_ms":1})";
    DebeziumJsonDeserializationSchema schema(AllTypesDescription(false, true));
    auto batch = Decode(schema, message);

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(batch->getRowKind(0), RowKind::INSERT);
    EXPECT_EQ(batch->GetValueAt<int32_t>(2, 0), 1);
}

TEST(DebeziumJsonDeserializationSchemaTest, IgnoresInvalidUnusedAfterWhenIgnoringParseErrors)
{
    const std::string message =
        R"({"before":{"i":1},"after":{"i":"oops"},"op":"d","ts_ms":1})";
    DebeziumJsonDeserializationSchema schema(AllTypesDescription(false, true));
    auto batch = Decode(schema, message);

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(batch->getRowKind(0), RowKind::DELETE);
    EXPECT_EQ(batch->GetValueAt<int32_t>(2, 0), 1);
}

TEST(DebeziumJsonDeserializationSchemaTest, AcceptsValidUnusedSide)
{
    const std::string message =
        R"({"before":{"i":2},"after":{"i":1},"op":"c","ts_ms":1})";
    DebeziumJsonDeserializationSchema schema(AllTypesDescription(false, false));
    auto batch = Decode(schema, message);

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(batch->getRowKind(0), RowKind::INSERT);
    EXPECT_EQ(batch->GetValueAt<int32_t>(2, 0), 1);
}

TEST(DebeziumJsonDeserializationSchemaTest, UnknownOpHonoursIgnoreParseErrors)
{
    const std::string message = R"({"before":null,"after":{"i":1},"op":"x","ts_ms":1})";
    DebeziumJsonDeserializationSchema failing(AllTypesDescription(false, false));
    EXPECT_THROW(Decode(failing, message), std::exception);

    DebeziumJsonDeserializationSchema ignoring(AllTypesDescription(false, true));
    EXPECT_EQ(DecodeCount(ignoring, message), 0);
}

TEST(DebeziumJsonDeserializationSchemaTest, ReadsSupportedMetadataColumns)
{
    json description = MakeDescription(
        {"id", "schema", "ingestion-timestamp", "source.timestamp", "source.database", "source.schema",
            "source.table"},
        {"INTEGER", "VARCHAR(2147483647)", "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)", "VARCHAR(2147483647)", "VARCHAR(2147483647)",
            "VARCHAR(2147483647)"},
        true,
        false,
        {"schema", "ingestion-timestamp", "source.timestamp", "source.database", "source.schema",
            "source.table"});
    DebeziumJsonDeserializationSchema schema(description);
    const std::string message = R"({"schema":{"type":"struct"},"payload":{"before":null,"after":{"id":7},"source":{"db":"inventory","schema":"inv","table":"products","ts_ms":1704164645006},"op":"c","ts_ms":1704164645006}})";
    auto batch = Decode(schema, message);

    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(batch->GetValueAt<int32_t>(0, 0), 7);
    EXPECT_EQ(GetString(batch.get(), 1, 0), R"({"type":"struct"})");
    EXPECT_EQ(batch->GetValueAt<int64_t>(2, 0), TS_MS);
    EXPECT_EQ(batch->GetValueAt<int64_t>(3, 0), TS_MS);
    EXPECT_EQ(GetString(batch.get(), 4, 0), "inventory");
    EXPECT_EQ(GetString(batch.get(), 5, 0), "inv");
    EXPECT_EQ(GetString(batch.get(), 6, 0), "products");
}

TEST(DebeziumJsonDeserializationSchemaTest, RejectsUnsupportedMetadataKey)
{
    json description = MakeDescription({"id", "properties"}, {"INTEGER", "VARCHAR(2147483647)"}, false,
        false, {"source.properties"});
    EXPECT_THROW(
        {
            DebeziumJsonDeserializationSchema schema(description);
        },
        std::exception);
}
