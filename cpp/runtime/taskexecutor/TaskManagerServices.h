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

#ifndef TASKMANAGERSERVICES_H
#define TASKMANAGERSERVICES_H

#include <memory>
#include <executiongraph/descriptor/TaskManagerServiceConfigurationPOD.h>

#include "shuffle/ShuffleEnvironment.h"
#include "TaskManagerServiceConfiguration.h"

namespace omnistream {
class TaskManagerServices {
public:
    explicit TaskManagerServices(std::shared_ptr<ShuffleEnvironment> shuffleEnvironment)
        : shuffleEnvironment_(shuffleEnvironment)
    {
    }

    ~TaskManagerServices()
    {
    }

    std::shared_ptr<ShuffleEnvironment> getShuffleEnvironment();

    static TaskManagerServices* fromConfiguration(TaskManagerServiceConfigurationPOD taskManagerServiceConfiguration);

private:
    std::shared_ptr<ShuffleEnvironment> shuffleEnvironment_;
};
} // namespace omnistream

#endif // TASKMANAGERSERVICES_H
