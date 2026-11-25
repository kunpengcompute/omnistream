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

#include "RdKafkaConsumer.h"
#include <thread>
#include "core/include/common.h"

ConsumerRecords* ConsumerRecords::EMPTY = new ConsumerRecords();

RdKafkaConsumer::RdKafkaConsumer(const std::unordered_map<std::string, std::string>& config)
{
    // 创建配置对象
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    // 不同类型配置的对应映射处理
    // 设置用户配置
    for (const auto& kv : config) {
        if (kv.first == "max.poll.records") {
            batch_size_ = std::stoi(kv.second);
            continue;
        }
        std::string errstr;
        if (conf->set(kv.first, kv.second, errstr) != RdKafka::Conf::CONF_OK) {
            LOG("unknown rdkafka config given, but it's ok to ignore it here");
        }
    }

    // 创建消费者实例
    std::string errstr;
    consumer_ = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!consumer_) {
        delete conf;
        throw std::runtime_error("Failed to create consumer: " + errstr);
    }
    delete conf;
}

RdKafkaConsumer::~RdKafkaConsumer()
{
    close();
    delete consumer_;
}

void RdKafkaConsumer::setBatchSize(int size)
{
    batch_size_ = size;
}

ConsumerRecords* RdKafkaConsumer::poll(int timeoutMs)
{
    std::unordered_map<RdKafka::TopicPartition*, std::vector<RdKafka::Message*>> records =
        consumer_->consumeBatch(timeoutMs, batch_size_);

    ConsumerRecords* consumerRecords = new ConsumerRecords(std::move(records));

    return consumerRecords;
}

void RdKafkaConsumer::assign(std::vector<RdKafka::TopicPartition*>& partitions)
{
    RdKafka::ErrorCode resp = consumer_->assign(partitions);
    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "% assign failed: " << RdKafka::err2str(resp) << std::endl;
    }
}

void RdKafkaConsumer::assignment(std::vector<RdKafka::TopicPartition*>& partitions)
{
    RdKafka::ErrorCode resp = consumer_->assignment(partitions);
    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "% assignment failed: " << RdKafka::err2str(resp) << std::endl;
    }
}

void RdKafkaConsumer::position(std::vector<RdKafka::TopicPartition*>& partitions)
{
    RdKafka::ErrorCode resp = consumer_->position(partitions);
    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "% position failed: " << RdKafka::err2str(resp) << std::endl;
    }
}

void RdKafkaConsumer::committed(std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitions)
{
    std::vector<RdKafka::TopicPartition*> rawPartitions;
    rawPartitions.reserve(partitions.size());
    for (const auto& ptr : partitions) {
        rawPartitions.push_back(ptr.get());
    }
    RdKafka::ErrorCode resp = consumer_->committed(rawPartitions, 10000);
    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "% committed failed: " << RdKafka::err2str(resp) << std::endl;
    }
}

void RdKafkaConsumer::seek(RdKafka::TopicPartition& partition)
{
    RdKafka::ErrorCode resp = consumer_->seek(partition, 10000);
    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "% seek failed: " << RdKafka::err2str(resp) << " " << std::to_string(resp) << std::endl;
    }
}

void RdKafkaConsumer::seek(
    std::unordered_map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& partitionsStartingFromSpecifiedOffsets)
{
    for (const auto& pair : partitionsStartingFromSpecifiedOffsets) {
        INFO_RELEASE(
            "RdKafkaConsumer::seek topic " << pair.first->topic() << " partition" << pair.first->partition()
                                           << " offset " << pair.second);
        pair.first->set_offset(pair.second);
        seek(*(pair.first));
    }
}

void RdKafkaConsumer::seekToEnd(std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitions)
{
    for (const auto& tp : partitions) {
        tp->set_offset(RdKafka::Topic::OFFSET_END);
        seek(*tp);
    }
}

void RdKafkaConsumer::seekToBeginning(std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitions)
{
    for (const auto& tp : partitions) {
        tp->set_offset(RdKafka::Topic::OFFSET_BEGINNING);
        seek(*tp);
    }
}

void RdKafkaConsumer::endOffsets(std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitions)
{
    for (const auto& tp : partitions) {
        int64_t low;
        int64_t high;
        // 时间从kafka配置读取 default.api.timeout.ms
        RdKafka::ErrorCode resp =
            consumer_->query_watermark_offsets(tp->topic().c_str(), tp->partition(), &low, &high, 10000);
        if (resp != RdKafka::ErrorCode::ERR_NO_ERROR) {
            LOG("Failed to query watermark offsets for topic: " + tp->topic() +
                ", partition: " + std::to_string(tp->partition()) + ". Error: " + RdKafka::err2str(resp));
        } else {
            tp->set_offset(high);
        }
    }
}

void RdKafkaConsumer::close()
{
    if (closed_) {
        return;
    }
    closed_ = true;
    RdKafka::ErrorCode resp = consumer_->close();
    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "% close failed: " << RdKafka::err2str(resp) << std::endl;
    }
}

void RdKafkaConsumer::commitAsync()
{
}

void RdKafkaConsumer::commitOffsets(const std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& offsets)
{
    std::vector<RdKafka::TopicPartition*> partitions;
    std::vector<std::unique_ptr<RdKafka::TopicPartition>> ownedPartitions;
    partitions.reserve(offsets.size());
    ownedPartitions.reserve(offsets.size());

    for (const auto& entry : offsets) {
        std::unique_ptr<RdKafka::TopicPartition> tp(
            RdKafka::TopicPartition::create(entry.first->topic(), entry.first->partition()));
        tp->set_offset(entry.second);
        partitions.push_back(tp.get());
        ownedPartitions.push_back(std::move(tp));
    }

    RdKafka::ErrorCode resp = consumer_->commitSync(partitions);
    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "% commitOffsets failed: " << RdKafka::err2str(resp) << std::endl;
        INFO_RELEASE("Error:Failed to commit offsets: " << RdKafka::err2str(resp));
        throw std::runtime_error("Failed to commit offsets: " + RdKafka::err2str(resp));
    }
}
