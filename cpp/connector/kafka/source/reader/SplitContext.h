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

#ifndef FLINK_TNEL_SPLITCONTEXT_H
#define FLINK_TNEL_SPLITCONTEXT_H

#include "core/api/connector/source/ReaderOutput.h"
#include "connector/kafka/source/split/KafkaPartitionSplitState.h"

class SplitContext {
public:
    std::string splitId;
    KafkaPartitionSplitState* state;
    SourceOutput* sourceOutput = nullptr;

    SplitContext(const std::string& splitId, KafkaPartitionSplitState* state) : splitId(splitId), state(state)
    {
    }

    ~SplitContext()
    {
        delete state;
    }

    // 获取或创建 SplitOutput 的方法
    SourceOutput& getOrCreateSplitOutput(ReaderOutput* mainOutput)
    {
        if (sourceOutput == nullptr) {
            // 拆分输出应该在 SourceOperator 中处理 AddSplitsEvent 时创建。
            // 这里我们只是使用此方法来获取先前创建的输出。
            sourceOutput = &(mainOutput->CreateOutputForSplit(splitId));
        }
        return *sourceOutput;
    }
};

#endif // FLINK_TNEL_SPLITCONTEXT_H
