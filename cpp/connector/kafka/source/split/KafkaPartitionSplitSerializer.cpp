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

#include "KafkaPartitionSplitSerializer.h"

#include <sstream>
#include "common.h"

int KafkaPartitionSplitSerializer::getVersion() const
{
    return CURRENT_VERSION;
}

namespace {
void requireAvailable(const std::vector<uint8_t>& serialized, size_t offset, size_t bytes, const std::string& field)
{
    if (offset > serialized.size() || bytes > serialized.size() - offset) {
        std::ostringstream oss;
        oss << "Corrupt KafkaPartitionSplit bytes: need " << bytes << " byte(s) for " << field << " at offset "
            << offset << ", totalBytes=" << serialized.size();
        throw std::runtime_error(oss.str());
    }
}

int64_t readBigEndianLong(const std::vector<uint8_t>& serialized, size_t offset)
{
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(int64_t); ++i) {
        value = (value << KafkaPartitionSplitSerializer::ONE_BYTE_LENGTH) | serialized[offset + i];
    }
    return static_cast<int64_t>(value);
}
} // namespace

std::vector<uint8_t> KafkaPartitionSplitSerializer::serialize(const KafkaPartitionSplit& split)
{
    std::vector<uint8_t> serialized;

    // 获取 KafkaPartitionSplit 的相关字段
    const auto& topicPartition = split.getTopicPartition();
    const std::string& topic = topicPartition->topic();
    uint32_t partition = static_cast<uint32_t>(topicPartition->partition());
    uint64_t startingOffset = static_cast<uint64_t>(split.getStartingOffset());
    uint64_t stoppingOffset = static_cast<uint64_t>(split.getStoppingOffset());

    // 序列化 topic 的长度（int16_t）
    uint16_t topicSize = static_cast<uint16_t>(topic.size());
    serialized.push_back(static_cast<uint8_t>((topicSize >> ONE_BYTE_LENGTH) & 0xFF)); // 高字节
    serialized.push_back(static_cast<uint8_t>(topicSize & 0xFF));                      // 低字节

    // 序列化 topic 的内容
    for (char c : topic) {
        serialized.push_back(static_cast<uint8_t>(c));
    }

    // 序列化 partition（int32_t）
    serialized.push_back(static_cast<uint8_t>((partition >> THREE_BYTE_LENGTH) & 0xFF)); // 最高字节
    serialized.push_back(static_cast<uint8_t>((partition >> TWO_BYTE_LENGTH) & 0xFF));   // 次高字节
    serialized.push_back(static_cast<uint8_t>((partition >> ONE_BYTE_LENGTH) & 0xFF));   // 次低字节
    serialized.push_back(static_cast<uint8_t>(partition & 0xFF));                        // 最低字节

    // 序列化 startingOffset（int64_t）
    for (int i = 7; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((startingOffset >> (ONE_BYTE_LENGTH * i)) & 0xFF));
    }

    // 序列化 stoppingOffset（int64_t）
    for (int i = 7; i >= 0; --i) {
        serialized.push_back(static_cast<uint8_t>((stoppingOffset >> (ONE_BYTE_LENGTH * i)) & 0xFF));
    }

    return serialized;
}

KafkaPartitionSplit* KafkaPartitionSplitSerializer::deserialize(int version, std::vector<uint8_t>& serialized)
{
    if (version != CURRENT_VERSION) {
        throw std::runtime_error("Unsupported version");
    }

    size_t start = 0;
    requireAvailable(serialized, start, sizeof(int16_t), "topic length");
    uint16_t topicSize = (serialized[start] << ONE_BYTE_LENGTH) | serialized[start + 1];
    start += sizeof(int16_t);
    requireAvailable(serialized, start, topicSize, "topic");
    std::string topic(serialized.begin() + start, serialized.begin() + start + topicSize);

    start += topicSize;
    requireAvailable(serialized, start, sizeof(int32_t), "partition");
    int32_t partition = static_cast<int32_t>(
        (static_cast<uint32_t>(serialized[start]) << THREE_BYTE_LENGTH) |
        (static_cast<uint32_t>(serialized[start + 1]) << TWO_BYTE_LENGTH) |
        (static_cast<uint32_t>(serialized[start + 2]) << ONE_BYTE_LENGTH) |
        static_cast<uint32_t>(serialized[start + 3]));

    start += sizeof(int32_t);
    requireAvailable(serialized, start, sizeof(int64_t), "startingOffset");
    int64_t startingOffset = readBigEndianLong(serialized, start);

    start += sizeof(int64_t);
    requireAvailable(serialized, start, sizeof(int64_t), "stoppingOffset");
    int64_t stoppingOffset = readBigEndianLong(serialized, start);

    std::shared_ptr<RdKafka::TopicPartition> topicPartition;
    if (startingOffset < 0) {
        topicPartition = std::shared_ptr<RdKafka::TopicPartition>(RdKafka::TopicPartition::create(topic, partition));
    } else {
        topicPartition =
            std::shared_ptr<RdKafka::TopicPartition>(RdKafka::TopicPartition::create(topic, partition, startingOffset));
    }
    if (!topicPartition) {
        throw std::runtime_error(
            "Failed to create Kafka TopicPartition for topic=" + topic + ", partition=" + std::to_string(partition));
    }

    try {
        auto* split = new KafkaPartitionSplit(topicPartition, startingOffset, stoppingOffset);
        return split;
    } catch (const std::exception& e) {
        LOG("Error: KafkaPartitionSplit invalid, topic=" << topic << ", partition=" << partition << ", startingOffset="
                                                         << startingOffset << ", stoppingOffset=" << stoppingOffset
                                                         << ", bytes=" << serialized.size() << ", error=" << e.what());
        throw;
    }
}
