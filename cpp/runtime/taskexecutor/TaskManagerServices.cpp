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

#include "TaskManagerServices.h"

#include <io/network/OmniShuffleServiceFactory.h>
#include <shuffle/ShuffleServiceFactory.h>
#include "TaskManagerServices.h"

#include <io/network/OmniShuffleServiceFactory.h>
#include <shuffle/ShuffleServiceFactory.h>

namespace omnistream {

TaskManagerServices* TaskManagerServices::fromConfiguration(TaskManagerServiceConfigurationPOD taskMSConf)
{
    // todo: obtain the constructor parameters from taskMSConf
    auto shuffleContext = std::make_shared<ShuffleEnvironmentContext>(
        taskMSConf.getResourceID(),
        taskMSConf.getMemorySize(),
        taskMSConf.getPageSize(),
        taskMSConf.getRequestSegmentsTimeoutMillis(),
        taskMSConf.getNetworkBuffersPerChannel(),
        taskMSConf.getPartitionRequestInitialBackoff(),
        taskMSConf.getPartitionRequestMaxBackoff(),
        taskMSConf.getFloatingNetworkBuffersPerGate(),
        taskMSConf.getSegmentSize(),
        taskMSConf.getNumberofSegmentsGlobal(),
        taskMSConf.getsortShuffleMinBuffers(),
        taskMSConf.getsortShuffleMinParallelism(),
        taskMSConf.getmaxBuffersPerChannel());
    LOG_PART("taskMSConf will create");

    auto shuffleServiceFactory = std::make_shared<OmniShuffleServiceFactory>();
    // create shuffle environment
    auto shuffleEnv = shuffleServiceFactory->createOmniShuffleEnvironmentWithContext(shuffleContext);

    auto* taskManagerServices = new TaskManagerServices(shuffleEnv);
    return taskManagerServices;
}

std::shared_ptr<ShuffleEnvironment> TaskManagerServices::getShuffleEnvironment()
{
    return shuffleEnvironment_;
}
} // namespace omnistream
