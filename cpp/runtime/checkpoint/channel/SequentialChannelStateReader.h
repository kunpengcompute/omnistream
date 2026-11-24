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

#ifndef SEQUENTIAL_CHANNEL_STATE_READER_H
#define SEQUENTIAL_CHANNEL_STATE_READER_H

#include <vector>
#include <memory>

#include "RecoveredChannelStateHandler.h"

namespace omnistream {
class InputGate;
class ResultPartitionWriter;
class SequentialChannelStateReader {
public:
    virtual ~SequentialChannelStateReader() = default;
    virtual void readInputData(const std::vector<std::shared_ptr<InputGate>>& inputGates) = 0;
    virtual void readOutputData(
        const std::vector<std::shared_ptr<ResultPartitionWriter>>& writers, bool notifyAndBlockOnCompletion) = 0;
    virtual void close() = 0;

    static std::shared_ptr<SequentialChannelStateReader> NO_OP;
};
} // namespace omnistream
#endif // SEQUENTIAL_CHANNEL_STATE_READER_H
