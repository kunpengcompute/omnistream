/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include <gtest/gtest.h>
#include "core/api/common/serialization/JsonRowDataDeserializationSchema.h"
#include "types/logical/LogicalType.h"
#include "runtime/operators/source/csv/CsvRow.h"
#include "runtime/operators/source/csv/CsvSchema.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "runtime/operators/source/csv/CsvInputFormat.h"
#include "streaming/api/operators/StreamSource.h"
#include "table/runtime/operators/source/InputSplit.h"
#include "typeutils/BinaryRowDataSerializer.h"
#include "streaming/runtime/io/RecordWriterOutput.h"
#include "test/table/runtime/operators/DummyStreamPartitioner.h"

TEST(SourceTestTest, CsvRowKeepsQuotesInUnquotedJsonField)
{
    omnistream::csv::CsvSchema schema({omniruntime::type::OMNI_LONG, omniruntime::type::OMNI_VARCHAR});
    omnistream::csv::CsvRow row("11,[\"only\"]", schema);

    ASSERT_EQ(row.getNodes().size(), 2);
    EXPECT_EQ(row.getNodes()[0]->getValue(), "11");
    EXPECT_EQ(row.getNodes()[1]->getValue(), "[\"only\"]");
}

TEST(SourceTestTest, CsvRowParsesQuotedJsonField)
{
    omnistream::csv::CsvSchema schema({omniruntime::type::OMNI_LONG, omniruntime::type::OMNI_VARCHAR});
    omnistream::csv::CsvRow row("11,\"[\"\"only\"\"]\"", schema);

    ASSERT_EQ(row.getNodes().size(), 2);
    EXPECT_EQ(row.getNodes()[0]->getValue(), "11");
    EXPECT_EQ(row.getNodes()[1]->getValue(), "[\"only\"]");
}

TEST(SourceTestTest, JsonRowDataDeserializationSchemaHandlesNullStringField)
{
    nlohmann::json description = {
        {"outputNames", {"id", "json_str"}}, {"outputTypes", {"BIGINT", "VARCHAR(2147483647)"}}};
    JsonRowDataDeserializationSchema schema(description);

    std::string record = R"({"id":1,"json_str":null})";
    std::vector<const uint8_t*> messages{reinterpret_cast<const uint8_t*>(record.data())};
    std::vector<size_t> lengths{record.size()};

    auto* batch = reinterpret_cast<omnistream::VectorBatch*>(schema.deserialize(messages, lengths));
    ASSERT_NE(batch, nullptr);

    auto* idVector = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(0));
    auto* jsonVector =
        reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
            batch->Get(1));

    EXPECT_FALSE(idVector->IsNull(0));
    EXPECT_EQ(idVector->GetValue(0), 1);
    EXPECT_TRUE(jsonVector->IsNull(0));

    delete batch;
}

TEST(SourceTestTest, JsonRowDataDeserializationSchemaKeepsTimestampsWhenSkippingBadMessages)
{
    nlohmann::json description = {{"outputNames", {"i", "id", "ts", "text"}},
        {"outputTypes", {"INTEGER", "BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "VARCHAR(2147483647)"}},
        {"ignoreParseErrors", true}};
    JsonRowDataDeserializationSchema schema(description);
    const std::vector<std::string> records = {
        R"({"i":" +2147483647 ","id":"-9223372036854775808","ts":"2026-11-03 10:59:32.628","text":"NULL"})",
        "{",
        R"({"i":"2147483648","id":"123x","ts":"39:41.7","text":"''"})",
        R"({"i":123.75,"id":9223372036854775808,"ts":null,"text":null})",
        "null",
    };
    std::vector<const uint8_t*> messages;
    std::vector<size_t> lengths;
    for (const auto& record : records) {
        messages.push_back(reinterpret_cast<const uint8_t*>(record.data()));
        lengths.push_back(record.size());
    }
    std::vector<int64_t> timestamps{10, 20, 30, 40, 50};
    class BatchCollector final : public Collector {
    public:
        void collect(void* record) override
        {
            batch.reset(static_cast<omnistream::VectorBatch*>(record));
        }
        void close() override {}
        std::unique_ptr<omnistream::VectorBatch> batch;
    } collector;

    schema.deserialize(messages, lengths, timestamps, &collector);
    ASSERT_NE(collector.batch, nullptr);
    auto& batch = *collector.batch;
    ASSERT_EQ(batch.GetRowCount(), 3);
    EXPECT_EQ(batch.getTimestamp(0), 10);
    EXPECT_EQ(batch.getTimestamp(1), 30);
    EXPECT_EQ(batch.getTimestamp(2), 40);
    auto* integers = static_cast<omniruntime::vec::Vector<int32_t>*>(batch.Get(0));
    auto* longs = static_cast<omniruntime::vec::Vector<int64_t>*>(batch.Get(1));
    auto* text = static_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
        batch.Get(3));
    EXPECT_EQ(integers->GetValue(0), INT32_MAX);
    EXPECT_EQ(longs->GetValue(0), INT64_MIN);
    EXPECT_FALSE(batch.Get(2)->IsNull(0));
    EXPECT_EQ(text->GetValue(0), "NULL");
    EXPECT_TRUE(integers->IsNull(1));
    EXPECT_TRUE(longs->IsNull(1));
    EXPECT_TRUE(batch.Get(2)->IsNull(1));
    EXPECT_EQ(text->GetValue(1), "''");
    EXPECT_EQ(integers->GetValue(2), 123);
    EXPECT_TRUE(longs->IsNull(2));
    EXPECT_TRUE(text->IsNull(2));
}

TEST(SourceTestTest, JsonRowDataDeserializationSchemaRejectsInvalidFieldByDefault)
{
    nlohmann::json description = {{"outputNames", {"id"}}, {"outputTypes", {"BIGINT"}}};
    JsonRowDataDeserializationSchema schema(description);
    const std::string record = R"({"id":"NULL"})";
    std::vector<const uint8_t*> messages{reinterpret_cast<const uint8_t*>(record.data())};
    std::vector<size_t> lengths{record.size()};
    EXPECT_THROW(schema.deserialize(messages, lengths), std::runtime_error);
}

TEST(SourceTestTest, DISABLED_initoper)
{
    std::string description = R"DELIM({ "format" : "csv",
                        "delimiter" : null,
                        "filePath" : "input/genbid.csv",
                        "fields" : [{"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "BIGINT", "timestampKind" : 0, "fieldName" : "auction"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "BIGINT", "timestampKind" : 0, "fieldName" : "bidder"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "BIGINT", "timestampKind" : 0, "fieldName" : "price"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "STRING", "timestampKind" : 0, "fieldName" : "channel"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "STRING", "timestampKind" : 0, "fieldName" : "url"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "TIMESTAMP(3)", "timestampKind" : 0, "fieldName" : "dateTime"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "STRING", "timestampKind" : 0, "fieldName" : "extra"  }
                                    ],
                        "selectFields" : [ 0, 1, 2, 3, 4 ],
                        "csvSelectFieldToProjectFieldMapping" : [ 0, 1, 2, 3, 4 ],
                        "csvSelectFieldToCsvFieldMapping" : [ 3, 5, 2, 1, 0 ]})DELIM";
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    std::vector<DataTypeId> fields;
    for (auto& field : opDescriptionJSON["fields"]) {
        fields.push_back(LogicalType::flinkTypeToOmniTypeId(field["type"]));
    }
    omnistream::csv::CsvSchema schema(fields);
    std::vector<int> csvSelectFieldToProjectFieldMapping = opDescriptionJSON["csvSelectFieldToProjectFieldMapping"];
    std::vector<int> csvSelectFieldToCsvFieldMapping = opDescriptionJSON["csvSelectFieldToCsvFieldMapping"];
    std::vector<int> oneMap;
    oneMap.resize(csvSelectFieldToProjectFieldMapping.size());
    for (int i = 0; i < csvSelectFieldToProjectFieldMapping.size(); i++) {
        oneMap[csvSelectFieldToProjectFieldMapping[i]] = csvSelectFieldToCsvFieldMapping[i];
    }
    auto csvInputFormat = new omnistream::csv::CsvInputFormat<omnistream::VectorBatch>(schema, 1000, oneMap);
    omnistream::InputSplit* inputSplit = new omnistream::InputSplit(opDescriptionJSON["filePath"], 0, 100000);
    auto func = new omnistream::InputFormatSourceFunction<omnistream::VectorBatch>(csvInputFormat, inputSplit);
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    const int32_t BUFFER_CAPACITY = 256;
    uint8_t* address = new uint8_t[BUFFER_CAPACITY];
    auto* partitioner = new DummyStreamPartitioner();
    auto* recordWriter = new omnistream::datastream::RecordWriter(address, BUFFER_CAPACITY, partitioner);
    auto* recordWriteOutput = new omnistream::datastream::RecordWriterOutput(binaryRowDataSerializer, recordWriter);
    auto* source = new omnistream::StreamSource(func, recordWriteOutput);
    source->setup();
    source->run();
}
