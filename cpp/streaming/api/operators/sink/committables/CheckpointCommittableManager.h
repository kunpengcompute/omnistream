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

#ifndef FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGER_H
#define FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGER_H

#include <memory>
#include <optional>
#include <vector>
#include <stdexcept>
#include "CommittableManager.h"

template <typename CommT>
class CheckpointCommittableManager : public CommittableManager<CommT> {
public:
    virtual ~CheckpointCommittableManager() = default;

    /**
     * Returns the checkpoint id in which the committable was created.
     *
     * @return checkpoint id
     */
    virtual long GetCheckpointId() const = 0;
};

#endif // FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGER_H
