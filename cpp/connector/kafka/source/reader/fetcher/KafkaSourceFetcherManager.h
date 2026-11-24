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

#ifndef FLINK_TNEL_KAFKASOURCEFETCHERMANAGER_H
#define FLINK_TNEL_KAFKASOURCEFETCHERMANAGER_H

#include <iostream>
#include <memory>
#include <map>
#include <functional>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include "SingleThreadFetcherManager.h"
#include "connector/kafka/source/reader/RdKafkaConsumer.h"

using OffsetCommitCallback =
    std::function<void(const std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>&, const std::exception_ptr&)>;

class KafkaSourceFetcherManager : public SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit> {
public:
    KafkaSourceFetcherManager(
        FutureCompletingBlockingQueue<RdKafka::Message>* elementsQueue,
        std::function<SplitReader<RdKafka::Message, KafkaPartitionSplit>*()>& splitReaderSupplier);

    void commitOffsets(
        const std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& offsetsToCommit,
        OffsetCommitCallback callback);

private:
    void enqueueOffsetsCommitTask(
        SplitFetcher<RdKafka::Message, KafkaPartitionSplit>* splitFetcher,
        const std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& offsetsToCommit,
        OffsetCommitCallback callback);
};

#endif // FLINK_TNEL_KAFKASOURCEFETCHERMANAGER_H
