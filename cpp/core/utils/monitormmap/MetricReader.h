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

// MetricReader.h
#ifndef METRIC_READER_H
#define METRIC_READER_H

#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "common.h"
#include "MetricManager.h"
#include "SHMMetricView.h"

namespace omnistream {
class MetricReader {
public:
    static const char* sharedMemoryKeyPrefix;
    MetricReader();
    ~MetricReader();

    bool Init(const std::string& monitorKey, pid_t managerPid);
    void* GetData() const;
    size_t GetSize() const;

    void EnableMonitoring();
    void DisableMonitoring();
    bool IsEnableMonitoring() const;
    uint32_t GetNumberOfMetrics();
    SHMMetricView* GetSHMMetricView(int index);

    std::string MetricViewToString(SHMMetricView* metric);

    std::vector<SHMMetricView*> GetAllMetrics();
    void PrintAllMetrics(std::vector<SHMMetricView*>);

private:
    std::string sharedMemoryKey;
    int sharedMemoryFd;
    size_t sharedMemorySize;
    void* sharedMemoryPtr;
};
} // namespace omnistream

#endif // METRIC_READER_H
