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

#ifndef OMNISTREAM_ROLLING_POLICY_H
#define OMNISTREAM_ROLLING_POLICY_H

#include "./FileWriter.h"

template <typename IN, typename BucketID>
class RollingPolicy {
public:
    RollingPolicy(int64_t rollingFileSize_, int64_t rollingTimeInterval_, int64_t inactivityInterval_)
        : rollingFileSize(rollingFileSize_),
          rollingTimeInterval(rollingTimeInterval_),
          inactivityInterval(inactivityInterval_)
    {
        if (rollingFileSize <= 0 || rollingTimeInterval <= 0 || inactivityInterval <= 0) {
            throw std::invalid_argument("rollingFileSize, rollingTimeInterval and inactivityInterval must be > 0");
        }
    }

    bool shouldRollOnEvent(FileWriter<IN, BucketID>& part, IN element)
    {
        return part.getSize() > rollingFileSize;
    }

    bool shouldRollOnProcessingTime(FileWriter<IN, BucketID>& part, long currentTime)
    {
        return (currentTime - part.getCreationTime() >= rollingTimeInterval) ||
               (currentTime - part.getLastUpdateTime() >= inactivityInterval);
    }

private:
    bool rollOnCheckpoint;
    int64_t rollingFileSize;
    int64_t rollingTimeInterval;
    int64_t inactivityInterval;
};

#endif // OMNISTREAM_ROLLING_POLICY_H
