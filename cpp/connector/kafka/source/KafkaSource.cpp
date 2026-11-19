/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "KafkaSource.h"
#include "core/api/common/serialization/DeserializationFactory.h"
#include "reader/deserializer/DynamicKafkaDeserializationSchema.h"

KafkaSource::KafkaSource(nlohmann::json& opDescriptionJSON, bool isBatch) : isBatch(isBatch)
{
    nlohmann::json properties = opDescriptionJSON["properties"];
    // obtain the config
    for (auto& [key, value] : properties.items()) {
        auto iter = ConsumerConfigUtil::GetConfig().find(key);
        if (iter != ConsumerConfigUtil::GetConfig().end() && iter->second != "") {
            props.emplace(iter->second, value);
        }
    }
    // opt kafka consumer para from file
    std::unordered_map<std::string, std::string> optConsumerConfig = ConfigLoader::GetKafkaConsumerConfig();
    for (const auto& pair : optConsumerConfig) {
        props.emplace(pair.first, pair.second);
    }

    if (opDescriptionJSON.value("upsertMode", false)) {
        this->isBatch = true;
        props["enable.auto.commit"] = "false";
        props["commit.offsets.on.checkpoint"] = "false";
        deserializationSchema = new DynamicKafkaDeserializationSchema(opDescriptionJSON);
    } else {
        auto innerDeserializationSchema = DeserializationFactory::getDeserializationSchema(opDescriptionJSON);
        deserializationSchema = KafkaRecordDeserializationSchema::valueOnly(innerDeserializationSchema);
    }

    if (props.find("group.id") == props.end()) {
        INFO_RELEASE("Offset commit on checkpoint is disabled because group.id is not specified");
        if (props.find("commit.offsets.on.checkpoint") == props.end()) {
            props.emplace("commit.offsets.on.checkpoint", "false");
        }
    }
}

SourceReader<KafkaPartitionSplit>* KafkaSource::createReader(SourceReaderContext* readerContext) const
{
    auto subTaskId = readerContext->getSubTaskIndex();
    auto elementsQueue = new FutureCompletingBlockingQueue<RdKafka::Message>(subTaskId);
    deserializationSchema->open();
    std::function<SplitReader<RdKafka::Message, KafkaPartitionSplit>*()> splitReaderSupplier = [this, readerContext]() {
        return new KafkaPartitionSplitReader(props, readerContext);
    };
    RecordEmitter<RdKafka::Message, KafkaPartitionSplitState>* recordEmitter =
        new KafkaRecordEmitter(deserializationSchema);
    SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit>* kafkaSourceFetcherManager =
        new KafkaSourceFetcherManager(elementsQueue, splitReaderSupplier);
    return new KafkaSourceReader(
        elementsQueue, kafkaSourceFetcherManager, recordEmitter, props, readerContext, isBatch);
}

KafkaPartitionSplitSerializer* KafkaSource::getSplitSerializer() const
{
    return new KafkaPartitionSplitSerializer();
}
