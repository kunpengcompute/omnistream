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

#ifndef OMNISTREAM_BUCKET_FACTORY_H
#define OMNISTREAM_BUCKET_FACTORY_H

#include "Bucket.h"

template <typename IN, typename BucketID>
class BucketFactory {
public:
    Bucket<IN, BucketID>* getNewBucket(
        int subtaskIndex,
        const BucketID& bucketId,
        std::string bucketPath,
        long initialPartCounter,
        BucketWriter<IN, BucketID>* bucketWriter,
        RollingPolicy<IN, BucketID>* rollingPolicy,
        OutputFileConfig* outputFileConfig,
        std::vector<int> nonPartitionIndexes,
        std::vector<std::string> inputTypes)
    {
        return Bucket<IN, BucketID>::getNew(
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
};

#endif // OMNISTREAM_BUCKET_FACTORY_H
