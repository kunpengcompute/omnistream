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
#include "streaming/runtime/io/AbstractStreamTaskNetworkInput.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/io/network/api/serialization/RecordDeserializer.h"
#include "runtime/io/network/api/serialization/DeserializationResult.h"
#include "streaming/runtime/io/DataOutput.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "basictypes/String.h"
#include <cstdint>
#include <memory>

using namespace std;

class PrintDataOutput : public DataOutput {
    void emitRecord(StreamRecord* streamRecord) override
    {
        std::cout << "PrintDataOutput emitRecord:" << reinterpret_cast<String*>(streamRecord->getValue()) << std::endl;
    }

public:
    void emitWatermark(Watermark* watermark) override
    {
    }
};

TEST(AbstractStreamTaskNetworkInputTest, Constructor)
{
    auto* typeSerializer = new LongSerializer();
    auto recordDeserializers =
        std::make_unique<std::unordered_map<long, omnistream::datastream::RecordDeserializer*>>();
    recordDeserializers->emplace(1, new omnistream::datastream::SpillingAdaptiveSpanningRecordDeserializer());

    omnistream::datastream::AbstractStreamTaskNetworkInput input(typeSerializer, std::move(recordDeserializers));
    EXPECT_NE(input.getActiveSerializer(1), nullptr);
    EXPECT_EQ(input.getActiveSerializer(2), nullptr);
}

TEST(AbstractStreamTaskNetworkInputTest, Destructor)
{
    auto* typeSerializer = new LongSerializer();
    auto recordDeserializers =
        std::make_unique<std::unordered_map<long, omnistream::datastream::RecordDeserializer*>>();
    recordDeserializers->emplace(1, new omnistream::datastream::SpillingAdaptiveSpanningRecordDeserializer());

    omnistream::datastream::AbstractStreamTaskNetworkInput* input =
        new omnistream::datastream::AbstractStreamTaskNetworkInput(typeSerializer, std::move(recordDeserializers));
    delete input;
    // No specific expectations, just ensure no memory leaks
}

TEST(AbstractStreamTaskNetworkInputTest, EmitNextProcessElement_BufferConsumed)
{
    auto* typeSerializer = new LongSerializer();
    auto recordDeserializers =
        std::make_unique<std::unordered_map<long, omnistream::datastream::RecordDeserializer*>>();
    omnistream::datastream::SpillingAdaptiveSpanningRecordDeserializer* deserializer =
        new omnistream::datastream::SpillingAdaptiveSpanningRecordDeserializer();
    recordDeserializers->emplace(1, deserializer);

    omnistream::datastream::AbstractStreamTaskNetworkInput input(typeSerializer, std::move(recordDeserializers));
    PrintDataOutput output;
    int32_t inputNumber = 0;

    std::string str = "dummy";
    auto data = reinterpret_cast<uint8_t*>(str.data());
    input.emitNextProcessBuffer(data, 6, 1);
    uint32_t status = input.emitNextProcessElement(output, inputNumber);

    EXPECT_TRUE(status & OmniDataInputStatus::BUFFER_CONSUMED_TRUE);
}

TEST(AbstractStreamTaskNetworkInputTest, EmitNextProcessBuffer_InvalidChannelInfo)
{
    auto* typeSerializer = new LongSerializer();
    auto recordDeserializers =
        std::make_unique<std::unordered_map<long, omnistream::datastream::RecordDeserializer*>>();

    omnistream::datastream::AbstractStreamTaskNetworkInput input(typeSerializer, std::move(recordDeserializers));

    std::string str = "dummy";
    auto data = reinterpret_cast<uint8_t*>(str.data());
    EXPECT_THROW(input.emitNextProcessBuffer(data, 6, -1), std::logic_error);
}

TEST(AbstractStreamTaskNetworkInputTest, EmitNextProcessBuffer_ChannelInfoNotPresent)
{
    auto* typeSerializer = new LongSerializer();
    auto recordDeserializers =
        std::make_unique<std::unordered_map<long, omnistream::datastream::RecordDeserializer*>>();

    omnistream::datastream::AbstractStreamTaskNetworkInput input(typeSerializer, std::move(recordDeserializers));

    std::string str = "dummy";
    auto data = reinterpret_cast<uint8_t*>(str.data());
    EXPECT_THROW(input.emitNextProcessBuffer(data, 6, 1), std::logic_error);
}

TEST(AbstractStreamTaskNetworkInputTest, ProcessElement_RecordWithoutTimestamp)
{
    auto* typeSerializer = new LongSerializer();
    auto recordDeserializers =
        std::make_unique<std::unordered_map<long, omnistream::datastream::RecordDeserializer*>>();
    recordDeserializers->emplace(1, new omnistream::datastream::SpillingAdaptiveSpanningRecordDeserializer());

    omnistream::datastream::AbstractStreamTaskNetworkInput input(typeSerializer, std::move(recordDeserializers));
    PrintDataOutput output;
    int32_t inputNumber = 0;

    String str("10");
    StreamRecord record(&str);
    record.setTag(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP);
    bool breakBatchEmitting = input.processElement(&record, output, inputNumber);
    EXPECT_EQ(inputNumber, 1);
    EXPECT_FALSE(breakBatchEmitting);
}
