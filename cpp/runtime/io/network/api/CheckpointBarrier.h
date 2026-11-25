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

#ifndef FLINK_TNEL_CHECKPOINTBARRIER_H
#define FLINK_TNEL_CHECKPOINTBARRIER_H

#include <stdexcept>
#include <string>
#include <sstream>
#include <memory>

#include "core/include/common.h"
#include "../../../checkpoint/CheckpointOptions.h"
#include "../../../event/RuntimeEvent.h"

class CheckpointBarrier : public omnistream::RuntimeEvent {
public:
    CheckpointBarrier(long id, long timestamp, CheckpointOptions* checkpointOptions);

    CheckpointBarrier(long id, long timestamp, std::shared_ptr<CheckpointOptions> checkpointOptions);

    ~CheckpointBarrier();

    std::shared_ptr<CheckpointOptions> GetCheckpointOptions() const;

    CheckpointBarrier* WithOptions(std::shared_ptr<CheckpointOptions> checkpointOptions);

    std::string GetEventClassName() override
    {
        return "CheckpointBarrier";
    }

    bool operator==(const CheckpointBarrier& other) const;

    long GetId() const;
    long GetTimestamp() const;
    bool IsCheckpoint() const;
    CheckpointBarrier* AsUnaligned(bool& isNeedDel);
    std::string ToString() const;

private:
    long id_;
    long timestamp_;
    std::shared_ptr<CheckpointOptions> checkpointOptions_;
};

namespace std {
template <>
struct hash<CheckpointBarrier> {
    std::size_t operator()(const CheckpointBarrier& ref) const;
};
} // namespace std

#endif // FLINK_TNEL_CHECKPOINTBARRIER_H
