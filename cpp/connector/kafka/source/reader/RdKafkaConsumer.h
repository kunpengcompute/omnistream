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

#ifndef FLINK_TNEL_RDKAFKACONSUMER_H
#define FLINK_TNEL_RDKAFKACONSUMER_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <map>
#include <chrono>
#include <memory>
#include <librdkafka/rdkafkacpp.h>
#include <algorithm>
#include "connector/kafka/source/unordered_dense.h"

#include "core/include/common.h"

struct TopicPartitionComparator {
    bool operator()(const RdKafka::TopicPartition* lhs, const RdKafka::TopicPartition* rhs) const
    {
        // 这里需要实现具体的比较逻辑，例如比较 topic_name 和 partition
        return std::string(lhs->topic()) == std::string(rhs->topic()) && lhs->partition() == rhs->partition();
    }
};

// 自定义哈希函数
struct TopicPartitionHash {
    std::size_t operator()(const RdKafka::TopicPartition* tp) const
    {
        const std::string& topic = tp->topic();
        int partition = tp->partition();
        std::size_t h1 = std::hash<std::string>{}(topic);
        std::size_t h2 = std::hash<int>{}(partition);
        return h1 ^ (h2 << 1);
    }
};

// 消费者记录集合类
class ConsumerRecords {
public:
    static ConsumerRecords* EMPTY;

    explicit ConsumerRecords(std::unordered_map<RdKafka::TopicPartition*, std::vector<RdKafka::Message*>>&& records)
        : records_(std::move(records))
    {
    }

    ConsumerRecords() noexcept
    {
    }

    ~ConsumerRecords()
    {
        ankerl::unordered_dense::map<uint64_t, int> topParRefMap;
        ankerl::unordered_dense::map<uint64_t, int> topicRefMap;
        ankerl::unordered_dense::map<uint64_t, int> bufRefMap;
        try {
            topParRefMap.insert(std::make_pair(0, 0));
            topicRefMap.insert(std::make_pair(0, 0));
        } catch (const std::exception& ex) {
            std::cerr << "Exception caught in destructor when insert data" << ex.what() << std::endl;
        }
        ankerl::unordered_dense::map<uint64_t, int>::iterator topParRefMap_last;
        ankerl::unordered_dense::map<uint64_t, int>::iterator topicRefMap_last;
        topParRefMap_last = topParRefMap.begin();
        topicRefMap_last = topicRefMap.begin();
        try {
            FreeMsg(topParRefMap_last, topParRefMap, topicRefMap_last, topicRefMap, bufRefMap);
        } catch (const std::exception& ex) {
            std::cerr << "Exception caught in destructor when insert data" << ex.what() << std::endl;
        }
        for (auto& it : topParRefMap) {
            if (likely(it.first)) RdKafka::RdKafkaCInterface::DeleteTopParRefCnt(it.first, it.second);
        }
        for (auto& it : topicRefMap) {
            if (likely(it.first)) RdKafka::RdKafkaCInterface::DeleteTopicRecCnt(it.first, it.second);
        }

        for (auto& it : bufRefMap) {
            if (likely(it.first)) RdKafka::RdKafkaCInterface::DeleteBufRefCnt(it.first, it.second);
        }
    }

    void FreeMsg(
        ankerl::unordered_dense::map<uint64_t, int>::iterator& topParRefMap_last,
        ankerl::unordered_dense::map<uint64_t, int>& topParRefMap,
        ankerl::unordered_dense::map<uint64_t, int>::iterator& topicRefMap_last,
        ankerl::unordered_dense::map<uint64_t, int>& topicRefMap,
        ankerl::unordered_dense::map<uint64_t, int>& bufRefMap)
    {
        for (auto& pair : records_) {
            // 释放 vector 中每个 Message* 指向的对象,主动调用DeleteMessage批量减少引用计数后再delete
            delete pair.first;
            for (auto message : pair.second) {
                if (message) {
                    FreeOneMsg(message, topParRefMap_last, topParRefMap, topicRefMap_last, topicRefMap, bufRefMap);
                }
                // Only deletes the reference, not actually frees up memory.
                delete message;
            }
        }
    }

    void FreeOneMsg(
        RdKafka::Message* message,
        ankerl::unordered_dense::map<uint64_t, int>::iterator& topParRefMap_last,
        ankerl::unordered_dense::map<uint64_t, int>& topParRefMap,
        ankerl::unordered_dense::map<uint64_t, int>::iterator& topicRefMap_last,
        ankerl::unordered_dense::map<uint64_t, int>& topicRefMap,
        ankerl::unordered_dense::map<uint64_t, int>& bufRefMap)
    {
        uint64_t topParRefAddr = 0;
        uint64_t topicRefAddr = 0;
        uint64_t bufRefAddr = 0;
        message->DeleteMessage(&topParRefAddr, &topicRefAddr, &bufRefAddr);

        if (likely(topParRefMap_last->first == topParRefAddr)) {
            topParRefMap_last->second++;
        } else {
            auto it = topParRefMap.find(topParRefAddr);
            if (it == topParRefMap.end()) {
                it = topParRefMap.insert(std::make_pair(topParRefAddr, 1)).first;
            } else {
                it->second++;
            }
            topParRefMap_last = it;
        }

        if (likely(topicRefMap_last->first == topParRefAddr)) {
            topicRefMap_last->second++;
        } else {
            auto it = topicRefMap.find(topicRefAddr);
            if (it == topicRefMap.end()) {
                it = topicRefMap.insert(std::make_pair(topicRefAddr, 1)).first;
            } else {
                it->second++;
            }
            topicRefMap_last = it;
        }
        auto it = bufRefMap.find(bufRefAddr);
        if (it == bufRefMap.end()) {
            bufRefMap.insert(std::make_pair(bufRefAddr, 1));
        } else {
            it->second++;
        }
    }

    bool empty() const
    {
        return records_.empty();
    }

    std::vector<RdKafka::Message*>& records(RdKafka::TopicPartition* tp)
    {
        auto it = records_.find(tp);
        if (it != records_.end()) {
            return it->second;
        }
        return records_[tp];
    }

    void partitions(std::vector<RdKafka::TopicPartition*>& output) const
    {
        output.clear();
        for (const auto& pair : records_) {
            output.push_back(pair.first);
        }
    }

    void addRecord(RdKafka::Message* message)
    {
        std::string topic = message->topic_name();
        int partition = message->partition();
        // 查找是否已经存在对应的 TopicPartition 对象
        auto key_it = std::find_if(records_.begin(), records_.end(), [&](const auto& pair) {
            return pair.first->topic() == topic && pair.first->partition() == partition;
        });
        if (key_it != records_.end()) {
            // 如果已经存在，直接使用
            key_it->second.push_back(message);
        } else {
            // 如果不存在，创建新的对象
            auto key = RdKafka::TopicPartition::create(topic, partition);
            records_[key] = {message};
        }
    }

private:
    std::unordered_map<RdKafka::TopicPartition*, std::vector<RdKafka::Message*>> records_;
};

class RdKafkaConsumer {
public:
    static constexpr int DEFAULT_MAX_POLL_RECORDS = 1000;

    explicit RdKafkaConsumer(const std::unordered_map<std::string, std::string>& config);
    ~RdKafkaConsumer();

    // 根据配置文件设置批量大小
    void setBatchSize(int size);

    // 核心poll方法（带超时和批量控制）
    ConsumerRecords* poll(int timeoutMs);

    void assign(std::vector<RdKafka::TopicPartition*>& partitions);

    void assignment(std::vector<RdKafka::TopicPartition*>& partitions);

    void position(std::vector<RdKafka::TopicPartition*>& partitions);

    void committed(std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitions);

    void seek(RdKafka::TopicPartition& partition);

    void seek(
        std::unordered_map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& partitionsStartingFromSpecifiedOffsets);

    void seekToEnd(std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitions);

    void seekToBeginning(std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitions);

    void endOffsets(std::vector<std::shared_ptr<RdKafka::TopicPartition>>& partitions);

    void close();

    void commitAsync();

    void commitOffsets(const std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& offsets);

private:
    RdKafka::KafkaConsumer* consumer_;
    std::string topic_;
    int batch_size_ = DEFAULT_MAX_POLL_RECORDS;
    bool closed_ = false;
};

#endif // FLINK_TNEL_RDKAFKACONSUMER_H
