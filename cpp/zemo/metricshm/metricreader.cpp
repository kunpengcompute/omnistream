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
#include "core/utils/monitormmap/MetricReader.h"

using namespace omnistream;
using namespace std;

int main(int argc, char* argv[])
{
    const int argSize = 2;
    if (argc != argSize) {
        cerr << "Usage: " << argv[0] << " <writer_pid>" << endl;
        return 1;
    }

    const int duration = 10;
    const int interval = 2000;

    pid_t writerPid = atoi(argv[1]);

    MetricReader reader;
    if (!reader.Init(MetricManager::sharedMemoryKeyPrefix, writerPid)) {
        GErrorLog("Reader failed to initialize shared memory");
        return 1;
    }

    LOG_TRACE("Metric reader getDataPrt : " << reader.GetData());
    LOG_TRACE("Metric reader numberOfMetric : " << reader.GetNumberOfMetrics());

    LOG_TRACE("is MonitorEnabled  " << reader.IsEnableMonitoring());
    auto allMetrics = reader.GetAllMetrics();
    LOG_TRACE("vector size : " << allMetrics.size());
    reader.PrintAllMetrics(allMetrics);

    if (!reader.IsEnableMonitoring()) {
        reader.EnableMonitoring();
        LOG_TRACE("after enable monitor" << reader.IsEnableMonitoring());
    }

    auto startTime = chrono::steady_clock::now();
    while (chrono::duration_cast<chrono::minutes>(chrono::steady_clock::now() - startTime).count() < duration) {
        reader.PrintAllMetrics(allMetrics);
        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    }

    return 0;
}
