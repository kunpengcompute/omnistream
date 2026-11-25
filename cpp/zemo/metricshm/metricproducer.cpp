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

#include <iostream>
#include <unistd.h>
#include <cstring>
#include "core/utils/monitormmap/MetricManager.h"

using namespace omnistream;
using namespace std;

int main()
{
    const string monitorKey = "my_metric";
    const size_t sharedMemorySize = 32768;
    const int duration = 10;
    const int interval = 5;

    MetricManager* manager = MetricManager::GetInstance();
    if (!manager->Setup(sharedMemorySize)) {
        GErrorLog("Producer failed to setup shared memory");
        return 1;
    }

    MetricManager* managerPtr = MetricManager::GetInstance();
    SHMMetric* metric = managerPtr->RegisterMetric(MetricManager::omniStreamTaskProcessInputID);

    // Write data to shared memory in a loop, stop after 2 minutes

    int counter = 0;
    auto startTime = chrono::steady_clock::now();
    while (chrono::duration_cast<chrono::minutes>(chrono::steady_clock::now() - startTime).count() < duration) {
        counter++;

        if (managerPtr->IsEnableMonitoring()) {
            metric->UpdateMetric(counter);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    }
    INFO_RELEASE("Producer process finished after 2 minutes.");
    return 0;
}
