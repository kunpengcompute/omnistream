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

#ifndef OMNISTREAM_OMNISTREAMMULTIPLEINPUTPROCESSOR_H
#define OMNISTREAM_OMNISTREAMMULTIPLEINPUTPROCESSOR_H

#include "OmniStreamOneInputProcessor.h"
#include "MutipleInputSelectionHandler.h"
#include "MultipleFuturesAvailabilityHelper.h"

namespace omnistream {
class OmniStreamMultipleInputProcessor : public OmniStreamInputProcessor {
public:
    OmniStreamMultipleInputProcessor(
        std::vector<OmniStreamOneInputProcessor*>&& _processors,
        std::shared_ptr<MutipleInputSelectionHandler> inputSelectionHandler)
        : processors(_processors),
          inputSelectionHandler(inputSelectionHandler)
    {
        availabilityHelper = std::make_shared<MultipleFuturesAvailabilityHelper>(processors.size());
    }
    ~OmniStreamMultipleInputProcessor() override;

    DataInputStatus processInput() override;
    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;
    std::shared_ptr<CompletableFutureV2<void>> PrepareSnapshot(
        std::shared_ptr<ChannelStateWriter> writer, long checkpointID) override;
    void close() override;

private:
    std::vector<OmniStreamOneInputProcessor*> processors;
    std::shared_ptr<MutipleInputSelectionHandler> inputSelectionHandler;
    std::shared_ptr<MultipleFuturesAvailabilityHelper> availabilityHelper;
    bool isPrepared = false;
    int8_t suspendNum = 2;
    int32_t lastReadInputIndex;
    int selectFirstReadingInputIndex();
    int selectNextReadingInputIndex();
    void FullCheckAndSetAvailable();
};
} // namespace omnistream

#endif
