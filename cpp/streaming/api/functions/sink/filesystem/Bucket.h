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

#ifndef OMNISTREAM_BUCKET_H
#define OMNISTREAM_BUCKET_H

#include <mutex>
#include <string>
#include "BucketWriter.h"
#include "RollingPolicy.h"
#include "OutputFileConfig.h"
#include "FileWriter.h"

template <typename IN, typename BucketID>
class Bucket {
public:
    static Bucket<IN, BucketID>* getNew(
        int subtaskIndex,
        BucketID bucketId,
        std::string bucketPath,
        long initialPartCounter,
        BucketWriter<IN, BucketID>* bucketWriter,
        RollingPolicy<IN, BucketID>* rollingPolicy,
        OutputFileConfig* outputFileConfig,
        std::vector<int> nonPartitionIndexes,
        std::vector<std::string> inputTypes)
    {
        return new Bucket<IN, BucketID>(
            subtaskIndex,
            bucketId,
            bucketPath,
            initialPartCounter,
            bucketWriter,
            rollingPolicy,
            outputFileConfig,
            nonPartitionIndexes,
            inputTypes);
    }

    Bucket(
        int subtaskIndex,
        BucketID bucketId,
        std::string bucketPath,
        long initialPartCounter,
        BucketWriter<IN, BucketID>* bucketWriter,
        RollingPolicy<IN, BucketID>* rollingPolicy,
        OutputFileConfig* outputFileConfig,
        std::vector<int> nonPartitionIndexes,
        std::vector<std::string> inputTypes)
        : subtaskIndex(subtaskIndex),
          bucketId(bucketId),
          bucketPath(bucketPath),
          partCounter(initialPartCounter),
          bucketWriter(bucketWriter),
          rollingPolicy(rollingPolicy),
          outputFileConfig(outputFileConfig),
          inProgressPart(nullptr),
          nonPartitionIndexes(nonPartitionIndexes),
          inputTypes(inputTypes)
    {
    }

    ~Bucket()
    {
        closePartFile();
    }

    BucketID getBucketId() const
    {
        return bucketId;
    }

    long getPartCounter() const
    {
        return partCounter;
    }

    void write(IN element, int rowId, long currentTime)
    {
        std::lock_guard<std::mutex> lock(inProgressPartMutex);

        // if there is no inProgressPart file for this bucket, i.e its a new bucket
        // or according to the rolling policy, a new part (file) should be created for this bucket
        if (!inProgressPart || rollingPolicy->shouldRollOnEvent(*inProgressPart, element)) {
            closePartFile();
            inProgressPart = rollPartFile(currentTime);
        }
        // when a valid inProgressPart (open file) exists, the element is written to it.
        inProgressPart->write(element, rowId, currentTime);
    }

    void onProcessingTime(long timestamp)
    {
        std::lock_guard<std::mutex> lock(inProgressPartMutex);
        if (inProgressPart && rollingPolicy->shouldRollOnProcessingTime(*inProgressPart, timestamp)) {
            closePartFile();
        }
    }

private:
    FileWriter<IN, BucketID>* rollPartFile(long currentTime)
    {
        closePartFile();
        std::string partFilePath = assembleNewPartPath();
        return bucketWriter->openNewInProgressFile(
            bucketId, partFilePath, currentTime, nonPartitionIndexes, inputTypes);
    }

    void closePartFile()
    {
        if (inProgressPart) {
            inProgressPart->close();
            delete inProgressPart;
            inProgressPart = nullptr;
        }
    }

    std::string assembleNewPartPath()
    {
        std::string path = bucketPath;
        if (!path.empty() && path.back() != '/') {
            path += '/';
        }
        return path + outputFileConfig->getPartPrefix() + "-" + std::to_string(subtaskIndex) + "-" +
               std::to_string(partCounter++) + outputFileConfig->getPartSuffix();
    }

    int subtaskIndex;
    BucketID bucketId;
    std::string bucketPath;
    long partCounter;
    BucketWriter<IN, BucketID>* bucketWriter;
    RollingPolicy<IN, BucketID>* rollingPolicy;
    OutputFileConfig* outputFileConfig;
    FileWriter<IN, BucketID>* inProgressPart;
    std::vector<int> nonPartitionIndexes;
    std::vector<std::string> inputTypes;
    std::mutex inProgressPartMutex;
};

#endif // OMNISTREAM_BUCKET_H
