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

// metric_manager.h
#ifndef METRIC_MANAGER_H
#define METRIC_MANAGER_H

#include <chrono>
#include <cstdint>
#include <iostream>
#include <cstdint>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <mutex>

#include <unistd.h>
#include <stdexcept>
#include <thread>
#include <vector>

#include "SHMMetric.h"
#include "common.h"

// mem layout of shared mem
// offset 0  byte    0 default value, disable ;   1: enable
// offset 1-3  reserved
// offset  4-7   uint_32  number of current metric
// offset 8 : metric:
//       offset 0 - 8 bytes unsighed long int, pthread id,
//       offset 8 - 8 bytes long int probe ID, (predefined),
///      offset 16, 8 bytes  long int , miilssec since epoch,
///      offset 24: 8 bytes long counting value
namespace omnistream {

struct MetricMeta {
    int index;
    long threadID;
    long probeID;
};

class MetricManager {
public:
    static const char* sharedMemoryKeyPrefix;
    static long omniStreamTaskProcessInputID; // OmniStreamTask::processInput
    static const int sharedMemoryFDMode;      // POSIX shm 权限：仅属主可读写
    explicit MetricManager(const std::string& monitorKey);
    ~MetricManager();

    bool Setup(size_t size);
    [[nodiscard]] void* GetDataPtr() const;
    [[nodiscard]] size_t GetSize() const;

    void EnableMonitoring();
    void DisableMonitoring();
    bool IsEnableMonitoring() const;

    static int64_t GetMillisecondsSinceEpoch()
    {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    static int64_t GetThreadId()
    {
        auto threadID = std::this_thread::get_id();
        long* threadIdPtr = static_cast<long*>(static_cast<void*>(&threadID));
        return *threadIdPtr;
    }

    omnistream::SHMMetric* RegisterMetric(long probeID)
    {
        std::lock_guard<std::mutex> lock(mutex);
        int size = static_cast<int>(metrics.size());

        long threadId = GetThreadId();

        MetricMeta meta;
        meta.index = size;
        meta.probeID = probeID;
        meta.threadID = threadId;

        metrics.push_back(meta);

        auto shmMetric = new SHMMetric(threadId, static_cast<long int*>(sharedMemoryPtr) + 1 + size * 4, probeID, -1);
        INFO_RELEASE(
            "MetricManager::registerMetric Thread ID : " << threadId << "  Millisecond: "
                                                         << MetricManager::GetMillisecondsSinceEpoch()
                                                         << " Probe ID : " << probeID);
        SetNumberOfMetrics(size + 1);
        return shmMetric;
    }

    // Static method to get the instance
    static MetricManager* GetInstance()
    {
        // Double-checked locking to ensure thread safety and efficiency
        if (!instance) {
            std::lock_guard<std::mutex> lock(singletonMutex);
            if (!instance) {
                instance.reset(new MetricManager(sharedMemoryKeyPrefix));
            }
        }
        return instance.get();
    }

private:
    std::mutex mutex;
    std::vector<MetricMeta> metrics;

    std::string monitorKey;
    std::string sharedMemoryKey;
    int sharedMemoryFd = -1;
    void* sharedMemoryPtr = nullptr;
    size_t sharedMemorySize = 0;

    // Static pointer to the single instance
    static std::unique_ptr<MetricManager> instance;
    static std::mutex singletonMutex;

    bool CreateSharedMemory(size_t size);
    bool AttachSharedMemory(size_t size);
    void SetNumberOfMetrics(int number);
};

inline void MetricManager::SetNumberOfMetrics(int number)
{
    uint32_t* numberPtr = (static_cast<uint32_t*>(sharedMemoryPtr)) + 1;
    *numberPtr = number;
}

inline bool MetricManager::IsEnableMonitoring() const
{
    return (*static_cast<uint8_t*>(sharedMemoryPtr) != 0);
}

} // namespace omnistream

#endif // METRIC_MANAGER_H
