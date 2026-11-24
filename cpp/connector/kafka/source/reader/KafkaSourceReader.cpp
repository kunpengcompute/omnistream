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

#include "KafkaSourceReader.h"
#include "fetcher/KafkaSourceFetcherManager.h"

KafkaSourceReader::KafkaSourceReader(
    FutureCompletingBlockingQueue<RdKafka::Message>* elementsQueue,
    SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit>* splitFetcherManager,
    RecordEmitter<RdKafka::Message, KafkaPartitionSplitState>* recordEmitter,
    const std::unordered_map<std::string, std::string>& props,
    SourceReaderContext* context,
    bool isBatch)
    : SingleThreadMultiplexSourceReaderBase<RdKafka::Message, KafkaPartitionSplit, KafkaPartitionSplitState>(
          elementsQueue, splitFetcherManager, recordEmitter, context, isBatch)
{
    // 默认关闭 offset 提交，仅当显式配置为 "true" 时开启
    auto it = props.find("commit.offsets.on.checkpoint");
    if (it != props.end() && it->second == "true") {
        commitOffsetsOnCheckpoint_ = true;
        INFO_RELEASE(
            "Offset commit on checkpoint is enabled. "
            "Consuming offset will be reported back to Kafka cluster.");
    } else {
        commitOffsetsOnCheckpoint_ = false;
    }
}

KafkaPartitionSplitState* KafkaSourceReader::initializedState(KafkaPartitionSplit* split)
{
    return new KafkaPartitionSplitState(split);
}

std::vector<KafkaPartitionSplit> KafkaSourceReader::snapshotState(long checkpointId)
{
    std::vector<KafkaPartitionSplit> splits = SourceReaderBase::snapshotState(checkpointId);
    if (!commitOffsetsOnCheckpoint_) {
        return splits;
    }

    if (splits.empty() && offsetsOfFinishedSplits.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        offsetsToCommit_[checkpointId] = std::unordered_map<std::shared_ptr<RdKafka::TopicPartition>, long>();
    } else {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& offsetsMap = offsetsToCommit_[checkpointId];

        for (const auto& split : splits) {
            if (split.getStartingOffset() >= 0) {
                offsetsMap[split.getTopicPartition()] = split.getStartingOffset();
            }
        }

        for (const auto& [tp, offset] : offsetsOfFinishedSplits) {
            offsetsMap[tp] = offset;
        }
    }
    return splits;
}

// 通知检查点完成
void KafkaSourceReader::notifyCheckpointComplete(long checkpointId)
{
    if (!commitOffsetsOnCheckpoint_) {
        return;
    }

    // 获取要提交的偏移量
    std::shared_ptr<std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>> committedPartitions;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = offsetsToCommit_.find(checkpointId);
        if (it == offsetsToCommit_.end()) {
            return;
        }
        // 直接使用 unordered_map 的内容，不需要转换
        auto convertedMap = std::make_shared<std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>>();
        for (const auto& entry : it->second) {
            (*convertedMap)[entry.first] = entry.second;
        }
        committedPartitions = convertedMap;
    }

    auto kafkaFetcherManager = static_cast<KafkaSourceFetcherManager*>(splitFetcherManager);
    kafkaFetcherManager->commitOffsets(
        *committedPartitions,
        [this, checkpointId, committedPartitions](
            const std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>&, const std::exception_ptr& e) {
            // The offset commit here is needed by the external monitoring. It won't
            // break Flink job's correctness if we fail to commit the offset here.
            if (e != nullptr) {
                INFO_RELEASE("Error:Failed to commit consumer offsets for checkpoint:" << checkpointId << ", error: ");
            } else {
                INFO_RELEASE("Successfully committed offsets for checkpoint:" << checkpointId);

                std::lock_guard<std::mutex> lock(mutex_);
                // 从 offsetsOfFinishedSplits 中移除已提交的分区
                for (const auto& entry : *committedPartitions) {
                    offsetsOfFinishedSplits.erase(entry.first);
                }

                // 清理已提交的检查点
                auto it = offsetsToCommit_.begin();
                while (it != offsetsToCommit_.end() && it->first <= checkpointId) {
                    it = offsetsToCommit_.erase(it);
                }
            }
        });
}
