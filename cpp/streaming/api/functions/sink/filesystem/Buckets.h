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

#ifndef OMNISTREAM_BUCKETS_H
#define OMNISTREAM_BUCKETS_H

#include <map>
#include <string>
#include <vector>
#include "Bucket.h"
#include "BucketFactory.h"
#include "BucketAssigner.h"

class BucketerContext : public BucketAssignerContext {
    long elementTimestamp;
    long currentWatermark;
    long currentProcessingTime;

public:
    void update(long timestamp, long watermark1, long processingTime1)
    {
        this->elementTimestamp = timestamp;
        this->currentWatermark = watermark1;
        this->currentProcessingTime = processingTime1;
    }

    long getCurrentProcessingTime() const override
    {
        return currentProcessingTime;
    }

    long getCurrentWatermark() const override
    {
        return currentWatermark;
    }

    long getTimestamp() const override
    {
        return elementTimestamp;
    }
};

template <typename IN, typename BucketID>
class Buckets {
public:
    Buckets(
        std::string basePath,
        BucketAssigner<IN, BucketID>* bucketAssigner,
        BucketFactory<IN, BucketID>* bucketFactory,
        BucketWriter<IN, BucketID>* bucketWriter,
        RollingPolicy<IN, BucketID>* rollingPolicy,
        int subtaskIndex,
        OutputFileConfig* outputFileConfig,
        std::vector<int> nonPartitionIndexes,
        std::vector<std::string> inputTypes)
        : basePath(basePath),
          bucketFactory(bucketFactory),
          bucketAssigner(bucketAssigner),
          bucketWriter(bucketWriter),
          rollingPolicy(rollingPolicy),
          subtaskIndex(subtaskIndex),
          maxPartCounter(0),
          outputFileConfig(outputFileConfig),
          nonPartitionIndexes(nonPartitionIndexes),
          inputTypes(inputTypes)
    {
    }

    ~Buckets()
    {
        for (auto& entry : activeBuckets) {
            delete entry.second;
        }
    }

    Bucket<IN, BucketID>* onElement(
        IN batch, int rowId, long currentProcessingTime, long elementTimestamp, long currentWatermark)
    {
        bucketerContext.update(elementTimestamp, currentWatermark, currentProcessingTime);
        BucketID bucketId = bucketAssigner->getBucketId(batch, rowId, &bucketerContext);
        auto bucket = getOrCreateBucketForBucketId(bucketId);
        bucket->write(batch, rowId, currentProcessingTime);
        this->maxPartCounter = std::max(maxPartCounter, bucket->getPartCounter());
        return bucket;
    }

    void onProcessingTime(long timestamp)
    {
        for (auto& entry : activeBuckets) {
            entry.second->onProcessingTime(timestamp);
        }
    }

    void close()
    {
        for (auto& entry : activeBuckets) {
            delete entry.second;
        }
    }

    long getMaxPartCounter() const
    {
        return maxPartCounter;
    }

    void setMaxPartCounter(long counter)
    {
        maxPartCounter = counter;
    }

    void commitUpToCheckpoint(long checkpointId)
    {
        for (auto& entry : activeBuckets) {
            entry.second->onProcessingTime(LONG_MAX);
        }
    }

    std::map<BucketID, long> snapshotState()
    {
        std::map<BucketID, long> state;
        for (auto& entry : activeBuckets) {
            state[entry.first] = entry.second->getPartCounter();
        }
        return state;
    }

    void notifyCheckpointComplete(long checkpointId)
    {
        LOG("Buckets::notifyCheckpointComplete checkpointId=" << checkpointId);
    }

    std::string getBasePath() const
    {
        return basePath;
    }

    void closePartFileForBucket(BucketID bucketId)
    {
        auto it = activeBuckets.find(bucketId);
        if (it != activeBuckets.end()) {
            it->second->onProcessingTime(LONG_MAX);
            LOG("Buckets::closePartFileForBucket bucket=" << bucketId);
        }
    }

private:
    std::string basePath;
    BucketFactory<IN, BucketID>* bucketFactory;
    BucketAssigner<IN, BucketID>* bucketAssigner;
    BucketWriter<IN, BucketID>* bucketWriter;
    RollingPolicy<IN, BucketID>* rollingPolicy;
    int subtaskIndex;
    long maxPartCounter;
    OutputFileConfig* outputFileConfig;
    std::vector<int> nonPartitionIndexes;
    std::vector<std::string> inputTypes;
    std::map<BucketID, Bucket<IN, BucketID>*> activeBuckets;
    BucketerContext bucketerContext;
    Bucket<IN, BucketID>* getOrCreateBucketForBucketId(BucketID bucketId)
    {
        auto it = activeBuckets.find(bucketId);
        if (it != activeBuckets.end()) {
            return it->second;
        }
        std::string bucketPath = assembleBucketPath(bucketId);
        Bucket<IN, BucketID>* bucket = bucketFactory->getNewBucket(
            subtaskIndex,
            bucketId,
            bucketPath,
            maxPartCounter,
            bucketWriter,
            rollingPolicy,
            outputFileConfig,
            nonPartitionIndexes,
            inputTypes);

        activeBuckets[bucketId] = bucket;
        return bucket;
    }

    std::string assembleBucketPath(BucketID bucketId)
    {
        return bucketId.empty() ? basePath : basePath + "/" + bucketId;
    }
};

#endif // OMNISTREAM_BUCKETS_H
