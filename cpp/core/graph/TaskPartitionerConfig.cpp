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

#include "TaskPartitionerConfig.h"

namespace omnistream::datastream {
TaskPartitionerConfig::TaskPartitionerConfig(string partitionName, int numberOfChannel, json options)
    : partitionerName(std::move(partitionName)),
      numberOfChannel(numberOfChannel),
      options(options)
{
}

string TaskPartitionerConfig::getPartitionName() const
{
    return partitionerName;
}

int TaskPartitionerConfig::getNumberOfChannel() const
{
    return numberOfChannel;
}

json TaskPartitionerConfig::getOptions() const
{
    return options;
}
} // namespace omnistream::datastream
