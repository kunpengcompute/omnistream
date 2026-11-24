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

#ifndef SHMMETRIC_H
#define SHMMETRIC_H

namespace omnistream {

class SHMMetric {
public:
    SHMMetric(long int threadID, long int* valuePtr, long int probeID, long int count)
        : threadID_(threadID),
          valuePtr_(valuePtr),
          probeID_(probeID),
          count_(count)
    {
        valuePtr[threadIDOffset] = threadID_;
        valuePtr[probeIDOffset] = probeID_;
    }

    void SetCount(long int count)
    {
        count_ = count;
    }

    void UpdateMetric(long count);

private:
    const int threadIDOffset = 0;
    const int probeIDOffset = 1;
    const int millisecondsOffset = 2;
    const int countOffset = 3;

    long int threadID_;
    long int* valuePtr_;
    long int probeID_;
    long int count_;
};
} // namespace omnistream

#endif // SHMMETRIC_H
