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
#include <nlohmann/json.hpp>
#include "connector/kafka/sink/KafkaSink.h"
#include "connector/kafka/sink/DynamicKafkaRecordSerializationSchema.h"
#include "streaming/api/operators/sink/SinkWriterOperator.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <cstdlib>
#include <ctime>

using json = nlohmann::json;

const std::unordered_map<std::string, DeliveryGuarantee> deliveryGuaranteeMap = {
    {"EXACTLY_ONCE", DeliveryGuarantee::EXACTLY_ONCE},
    {"NONE", DeliveryGuarantee::NONE},
    {"AT_LEAST_ONCE", DeliveryGuarantee::AT_LEAST_ONCE}};

TEST(KafkaSinkTest, SerializeDecimal64And128AsExactJsonNumbers)
{
    std::vector<std::string> inputFields = {"decimal64", "decimal128", "description"};
    std::vector<std::string> inputTypes = {"DECIMAL64(10,5)", "DECIMAL128(23,3)", "VARCHAR(2147483647)"};
    DynamicKafkaRecordSerializationSchema serializer(inputFields, inputTypes);

    omnistream::VectorBatch input(1);
    auto decimal64 = new omniruntime::vec::Vector<int64_t>(1, omniruntime::type::OMNI_DECIMAL64);
    decimal64->SetValue(0, -123);
    input.Append(decimal64);
    auto decimal128 = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(1);
    decimal128->SetValue(0, omniruntime::type::Decimal128("12345678901234567890123"));
    input.Append(decimal128);
    std::string description = "exact \"value\"";
    input.Append(omniruntime::TestUtil::CreateVarcharVector(&description, 1));

    auto record = serializer.Serialize(&input, 0);
    std::string serialized(record.value, record.valueLen);
    std::free(record.value);

    EXPECT_EQ(
        serialized, R"({"decimal64":-0.00123,"decimal128":12345678901234567890.123,"description":"exact \"value\""})");
    auto parsed = nlohmann::json::parse(serialized);
    EXPECT_TRUE(parsed["decimal64"].is_number());
    EXPECT_TRUE(parsed["decimal128"].is_number());
}

TEST(KafkaSinkTest, SerializeNegativeDecimal128AndNullDecimal)
{
    std::vector<std::string> inputFields = {"negative", "nullable"};
    std::vector<std::string> inputTypes = {"DECIMAL128(23,3)", "DECIMAL(18,3)"};
    DynamicKafkaRecordSerializationSchema serializer(inputFields, inputTypes);

    omnistream::VectorBatch input(1);
    auto negative = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(1);
    negative->SetValue(0, omniruntime::type::Decimal128(-908));
    input.Append(negative);
    auto nullable = new omniruntime::vec::Vector<int64_t>(1, omniruntime::type::OMNI_DECIMAL64);
    nullable->SetNull(0);
    input.Append(nullable);

    auto record = serializer.Serialize(&input, 0);
    std::string serialized(record.value, record.valueLen);
    std::free(record.value);

    EXPECT_EQ(serialized, R"({"negative":-0.908,"nullable":null})");
}

TEST(KafkaSinkTest, SerializeDecimalWithoutInsignificantTrailingZeros)
{
    std::vector<std::string> inputFields = {"decimal64", "decimal128", "integer", "zero"};
    std::vector<std::string> inputTypes = {
        "DECIMAL64(10,2)", "DECIMAL128(23,2)", "DECIMAL64(10,2)", "DECIMAL128(23,2)"};
    DynamicKafkaRecordSerializationSchema serializer(inputFields, inputTypes);

    omnistream::VectorBatch input(1);
    auto decimal64 = new omniruntime::vec::Vector<int64_t>(1, omniruntime::type::OMNI_DECIMAL64);
    decimal64->SetValue(0, 120);
    input.Append(decimal64);
    auto decimal128 = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(1);
    decimal128->SetValue(0, omniruntime::type::Decimal128(120));
    input.Append(decimal128);
    auto integer = new omniruntime::vec::Vector<int64_t>(1, omniruntime::type::OMNI_DECIMAL64);
    integer->SetValue(0, 100);
    input.Append(integer);
    auto zero = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(1);
    zero->SetValue(0, omniruntime::type::Decimal128(0));
    input.Append(zero);

    auto record = serializer.Serialize(&input, 0);
    std::string serialized(record.value, record.valueLen);
    std::free(record.value);

    EXPECT_EQ(serialized, R"({"decimal64":1.2,"decimal128":1.2,"integer":1,"zero":0})");
    auto parsed = nlohmann::json::parse(serialized);
    EXPECT_TRUE(parsed["decimal64"].is_number());
    EXPECT_TRUE(parsed["decimal128"].is_number());
    EXPECT_TRUE(parsed["integer"].is_number());
    EXPECT_TRUE(parsed["zero"].is_number());
}

// std::string generateRandomString(size_t length) {
//     const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
//     std::string result;
//     result.reserve(length);
//
//     for (size_t i = 0; i < length; ++i) {
//         result += charset[rand() % (sizeof(charset) - 1)]; // 随机选择字符
//     }
//
//     return result;
// }

TEST(KafkaSinkTest, DISABLED_StringDataSink)
{
    std::string jsonString = R"({
        "deliveryGuarantee": "AT_LEAST_ONCE",
        "transactionalIdPrefix": "kafka-sink",
        "properties": {
            "bootstrap.servers": "127.0.0.1:9092",
            "transaction.timeout.ms": "600000"
        },
        "topic": "kafka-sink",
        "inputTypes": [],
        "inputFields": []
    })";
    json tableOptions = json::parse(jsonString);

    if (!tableOptions.contains("deliveryGuarantee")) {
        throw std::invalid_argument("JSON object does not contain 'deliveryGuarantee' field.");
    }
    std::string guaranteeStr = tableOptions["deliveryGuarantee"];
    auto it = deliveryGuaranteeMap.find(guaranteeStr);
    if (it == deliveryGuaranteeMap.end()) {
        throw std::invalid_argument("Invalid value for 'deliveryGuarantee': " + guaranteeStr);
    }
    auto deliveryGuarantee = it->second;

    std::string transactionalIdPrefix = tableOptions["transactionalIdPrefix"];
    std::string topic = tableOptions["topic"];
    auto kafkaProducerConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    for (auto& item : tableOptions["properties"].items()) {
        std::string key = item.key();
        if (key.find("bootstrap.servers") || key.find("transaction.timeout.ms")) {
            std::string value = item.value();
            std::string errorString;
            RdKafka::Conf::ConfResult result = kafkaProducerConfig->set(key, value, errorString);
            if (result != RdKafka::Conf::CONF_OK) {
                std::cerr << "Failed to set Kafka configuration: " << key << " = " << value << std::endl;
            }
        }
    }
    auto kafkaSink =
        new KafkaSink(deliveryGuarantee, kafkaProducerConfig, transactionalIdPrefix, topic, tableOptions, 100);
    SinkWriterOperator* sinkWriterOperator;
    std::vector<std::any> fieldByPosition;
    std::map<std::string, std::any> fieldByName;
    std::map<std::string, int> positionByName;

    fieldByPosition.push_back(-7908116295878520943);
    fieldByPosition.push_back(8.989201265750876E307);
    auto now = std::chrono::system_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    fieldByPosition.push_back(millis);
    auto row = new Row(RowKind::INSERT, fieldByPosition, fieldByName, positionByName);
    auto streamRecord = new StreamRecord(row);

    try {
        sinkWriterOperator = new SinkWriterOperator(kafkaSink, tableOptions);
        //        srand(static_cast<unsigned int>(time(0)));
        //        size_t length = 10;
        //        std::string randomString = generateRandomString(length);
        for (int i = 0; i < 100000; ++i) {
            sinkWriterOperator->processElement(streamRecord);
        }
    } catch (std::runtime_error& e) {
        std::cout << "Cannot connect to Kafka, please check kafka server." << std::endl;
    } catch (std::exception& e) {
        std::cout << " " << e.what() << std::endl;
    }
    //    delete row;
    //    delete streamRecord;
    //    delete kafkaSink;
    //    delete sinkWriterOperator;
    EXPECT_EQ(0, 0);
}
