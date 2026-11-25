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
#include <basictypes/String.h>
#include "streaming/api/operators/SourceOperator.h"
#include "connector/kafka/source/KafkaSource.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "streaming/runtime/io/OmniPushingAsyncDataInput.h"

class MockSourceOutput : public Output {
public:
    std::vector<StreamRecord*> recordVector;
    explicit MockSourceOutput()
    {
    }

    void collect(void* record) override
    {
        auto* streamRecord = static_cast<StreamRecord*>(record);
        auto* value = static_cast<String*>(streamRecord->getValue());
        //        std::cout << value->toString() << std::endl;
        value->putRefCount();
    };

    void emitWatermark(Watermark* watermark) override
    {
        LOG(">>>> OmniStreamTaskNetworkOutput.emitWatermark");
    }

    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        LOG(">>>> OmniStreamTaskNetworkOutput.emitWatermarkStatus");
    }

    void close() override
    {
    }
};

TEST(SourceOperatorTest, DISABLED_filterTest)
{
    auto description =
        R"({"batch":false,"format":"kafka","emitProgressiveWatermarks":true,"properties":{"auto.offset.reset":"earliest","bootstrap.servers":"localhost:9092","group.id":"test-group"},"watermarkStrategy":"no","outOfOrdernessMillis":1000,"deserializationSchema":"SimpleStringSchema","hasMetadata":false,"outputNames":[],"outputTypes":[]})";
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    // create kafka source
    std::shared_ptr<KafkaSource> source = std::make_shared<KafkaSource>(opDescriptionJSON, false);
    ProcessingTimeService* timeService = new SystemProcessingTimeService();
    auto out = new MockSourceOutput();
    auto emitOut = new omnistream::OmniAsyncDataOutputToOutput(out);
    auto* op = new SourceOperator<>(nullptr, opDescriptionJSON, source, timeService);

    // 处理第一个参数
    std::vector<int> partitions = {0};
    // 处理第二个参数
    std::vector<int> offsets = {-2};

    std::vector<KafkaPartitionSplit> splitsChange;
    for (size_t i = 0; i < partitions.size(); ++i) {
        auto tp = std::shared_ptr<RdKafka::TopicPartition>(
            RdKafka::TopicPartition::create("datastream-source-topic", partitions[i]),
            [](RdKafka::TopicPartition* ptr) { delete ptr; });
        KafkaPartitionSplit split(tp, offsets[i]);
        splitsChange.push_back(split);
    }
    // add split event;
    op->open();
    auto splitSerializer = source->getSplitSerializer();
    AddSplitEvent event(splitsChange, splitSerializer);
    op->handleOperatorEvent(event);
    std::this_thread::sleep_for(std::chrono::seconds(5));
    int index = 0;
    while (index++ < 10) {
        auto status = op->emitNext(emitOut);
        if (status != DataInputStatus::MORE_AVAILABLE) {
            std::cout << "Polling stopped with status." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        // 可以在这里添加一些处理逻辑
    }
    delete splitSerializer;
}
