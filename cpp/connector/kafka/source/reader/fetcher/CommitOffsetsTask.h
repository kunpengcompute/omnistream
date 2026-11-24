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

#ifndef OMNISTREAM_COMMITOFFSETSTASK_H
#define OMNISTREAM_COMMITOFFSETSTASK_H

#include "SplitFetcherTask.h"
#include "connector/kafka/source/reader/RdKafkaConsumer.h"
#include "KafkaSourceFetcherManager.h"
#include <map>
#include <exception>

class CommitOffsetsTask : public SplitFetcherTask {
public:
    CommitOffsetsTask(
        KafkaPartitionSplitReader* splitReader,
        const std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t>& offsetsToCommit,
        OffsetCommitCallback callback)
        : splitReader(splitReader),
          offsetsToCommit(offsetsToCommit),
          callback(callback)
    {
    }

    ~CommitOffsetsTask() override
    {
    }

    bool Run() override
    {
        try {
            // 直接调用 commitOffsets 提交偏移量
            splitReader->commitOffsets(offsetsToCommit);
            if (callback) {
                callback(offsetsToCommit, nullptr);
            }
            return true;
        } catch (...) {
            auto exceptionPtr = std::current_exception();
            if (callback) {
                callback(offsetsToCommit, exceptionPtr);
            }
            return false;
        }
    }

    void WakeUp() override
    {
    }

    std::string ToString() override
    {
        std::string result = "CommitOffsetsTask: {";
        bool first = true;
        for (const auto& entry : offsetsToCommit) {
            if (!first) {
                result += ", ";
            }
            result += entry.first->topic();
            result += "-";
            result += std::to_string(entry.first->partition());
            result += ":";
            result += std::to_string(entry.second);
            first = false;
        }
        result += "}";
        return result;
    }

private:
    KafkaPartitionSplitReader* splitReader;
    std::map<std::shared_ptr<RdKafka::TopicPartition>, int64_t> offsetsToCommit;
    OffsetCommitCallback callback;
};

#endif // OMNISTREAM_COMMITOFFSETSTASK_H
