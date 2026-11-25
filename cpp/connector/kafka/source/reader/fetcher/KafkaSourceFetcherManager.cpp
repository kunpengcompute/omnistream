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

#include "KafkaSourceFetcherManager.h"
#include "CommitOffsetsTask.h"
#include "SplitFetcher.h"
#include <iostream>

KafkaSourceFetcherManager::KafkaSourceFetcherManager(
    FutureCompletingBlockingQueue<RdKafka::Message>* elementsQueue,
    std::function<SplitReader<RdKafka::Message, KafkaPartitionSplit>*()>& splitReaderSupplier)
    : SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit>(elementsQueue, splitReaderSupplier)
{
}

void KafkaSourceFetcherManager::commitOffsets(
    const std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& offsetsToCommit, OffsetCommitCallback callback)
{
    if (offsetsToCommit.empty()) {
        return;
    }

    auto splitFetcher = getRunningFetcher();
    if (splitFetcher != nullptr) {
        enqueueOffsetsCommitTask(splitFetcher, offsetsToCommit, callback);
    } else {
        splitFetcher = createSplitFetcher();
        enqueueOffsetsCommitTask(splitFetcher, offsetsToCommit, callback);
        startFetcher(splitFetcher);
    }
}

void KafkaSourceFetcherManager::enqueueOffsetsCommitTask(
    SplitFetcher<RdKafka::Message, KafkaPartitionSplit>* splitFetcher,
    const std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& offsetsToCommit,
    OffsetCommitCallback callback)
{
    auto commitTask = std::make_shared<CommitOffsetsTask>(splitFetcher->getSplitReader(), offsetsToCommit, callback);
    splitFetcher->enqueueTask(commitTask);
}
