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

#ifndef VECTORBATCH_POOL_CONF_H
#define VECTORBATCH_POOL_CONF_H

namespace omnistream {

class VectorBatchPoolConf {
public:
    int totalVBCount;              // total VB units across all tasks
    int64_t totalMemoryBytes;      // total memory budget (bytes) across all tasks
    int numRequiredVBsPerTask;     // minimum VB units guaranteed per task
    int maxVBsPerTask;             // max VB units a single task can hold
    int64_t maxMemoryBytesPerTask; // max memory a single task can use

    VectorBatchPoolConf(
        int totalVBCount = 10000,
        int64_t totalMemoryBytes = 512L * 1024 * 1024,
        int numRequiredVBsPerTask = 100,
        int maxVBsPerTask = 2000,
        int64_t maxMemoryBytesPerTask = 128L * 1024 * 1024)
        : totalVBCount(totalVBCount),
          totalMemoryBytes(totalMemoryBytes),
          numRequiredVBsPerTask(numRequiredVBsPerTask),
          maxVBsPerTask(maxVBsPerTask),
          maxMemoryBytesPerTask(maxMemoryBytesPerTask)
    {
    }
};

} // namespace omnistream

#endif // VECTORBATCH_POOL_CONF_H
