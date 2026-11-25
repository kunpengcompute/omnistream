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

#ifndef FLINK_TNEL_KAFKASOURCEREADER_H
#define FLINK_TNEL_KAFKASOURCEREADER_H

#include <iostream>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <functional>

#include "connector/kafka/source/metrics/KafkaSourceReaderMetrics.h"
#include "SingleThreadMultiplexSourceReaderBase.h"

class KafkaSourceReader
    : public SingleThreadMultiplexSourceReaderBase<RdKafka::Message, KafkaPartitionSplit, KafkaPartitionSplitState> {
public:
    /**
     * 构造函数
     */
    KafkaSourceReader(
        FutureCompletingBlockingQueue<RdKafka::Message>* elementsQueue,
        SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit>* splitFetcherManager,
        RecordEmitter<RdKafka::Message, KafkaPartitionSplitState>* recordEmitter,
        const std::unordered_map<std::string, std::string>& props,
        SourceReaderContext* context,
        bool isBatch);

    /**
     * 析构函数
     */
    ~KafkaSourceReader() = default;

    KafkaPartitionSplitState* initializedState(KafkaPartitionSplit* split) override;

    std::vector<KafkaPartitionSplit> snapshotState(long checkpointId) override;

    // 通知检查点完成
    void notifyCheckpointComplete(long checkpointId) override;

    // 通知拆分完成
    void onSplitFinished(const std::unordered_map<std::string, KafkaPartitionSplitState*>& finishedSplitIds) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [ignored, splitState] : finishedSplitIds) {
            if (splitState->getCurrentOffset() >= 0) {
                offsetsOfFinishedSplits[splitState->getTopicPartition()] = splitState->getCurrentOffset();
            }
        }
    }

private:
    std::shared_ptr<KafkaSourceReaderMetrics> kafkaSourceReaderMetrics_;
    std::unordered_map<std::shared_ptr<RdKafka::TopicPartition>, long> offsetsOfFinishedSplits;
    std::unordered_map<long, std::unordered_map<std::shared_ptr<RdKafka::TopicPartition>, long>> offsetsToCommit_;
    bool commitOffsetsOnCheckpoint_;

    mutable std::mutex mutex_;
};

#endif // FLINK_TNEL_KAFKASOURCEREADER_H
