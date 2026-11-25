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

#ifndef OMNIENVIRONMENT_H
#define OMNIENVIRONMENT_H

#include <memory>
#include <shuffle/ShuffleEnvironment.h>
#include "runtime/state/TaskStateManager.h"
#include <executiongraph/TaskInformationPOD.h>
#include <executiongraph/JobInformationPOD.h>

class RocksDBSharedResources;

namespace omnistream {
class TaskMetricGroup;

class EnvironmentV2 {
public:
    virtual std::shared_ptr<RocksDBSharedResources> getRocksDBSharedResources() const
    {
        return nullptr;
    }
    virtual void setRocksDBSharedResources(std::shared_ptr<RocksDBSharedResources> resources)
    {
        NOT_IMPL_EXCEPTION;
    }

    virtual std::shared_ptr<TaskStateManager> getTaskStateManager() = 0;
    virtual const TaskInformationPOD& taskConfiguration() const = 0;
    virtual const JobInformationPOD& jobConfiguration() const = 0;
    virtual std::shared_ptr<TaskMetricGroup> taskMetricGroup() const = 0;
};
} // namespace omnistream

#endif
