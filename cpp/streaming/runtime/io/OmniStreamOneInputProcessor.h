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

#ifndef OMNISTREAM_OMNISTREAMONEINPUTPROCESSOR_H
#define OMNISTREAM_OMNISTREAMONEINPUTPROCESSOR_H

#include "OmniStreamInputProcessor.h"
#include "OmniStreamTaskInput.h"
#include "OmniPushingAsyncDataInput.h"
#include "streaming/runtime/tasks/OperatorChain.h"

namespace omnistream {
class OmniStreamOneInputProcessor : public OmniStreamInputProcessor {
public:
    OmniStreamOneInputProcessor(
        OmniStreamTaskInput* input, OmniStreamTaskInput::OmniDataOutput* output, OperatorChainV2* operatorChain);
    ~OmniStreamOneInputProcessor() override;

    DataInputStatus processInput() override;
    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;
    OmniStreamTaskInput* GetInput();
    void close() override;
    //        CompletableFutureV2<void>* PrepareSnapshot(std::shared_ptr<ChannelStateWriter> writer, long checkpointID)
    //        override;
    std::shared_ptr<CompletableFutureV2<void>> PrepareSnapshot(
        std::shared_ptr<ChannelStateWriter> writer, long checkpointID) override;

private:
    OmniStreamTaskInput* input;
    OmniStreamTaskInput::OmniDataOutput* output;
    OperatorChainV2* endOfInputAware;
};
} // namespace omnistream

#endif // OMNISTREAM_OMNISTREAMONEINPUTPROCESSOR_H
