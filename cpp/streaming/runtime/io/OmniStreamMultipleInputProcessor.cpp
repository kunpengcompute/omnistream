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

#include "OmniStreamMultipleInputProcessor.h"
namespace omnistream {
OmniStreamMultipleInputProcessor::~OmniStreamMultipleInputProcessor()
{
    for (auto processor : processors) {
        if (processor != nullptr) {
            delete processor;
            processor = nullptr;
        }
    }
}
DataInputStatus OmniStreamMultipleInputProcessor::processInput()
{
    int readingInputIndex;
    if (isPrepared) {
        readingInputIndex = selectNextReadingInputIndex();
    } else {
        // the preparations here are not placed in the constructor because all work in it
        // must be executed after all operators are opened.
        readingInputIndex = selectFirstReadingInputIndex();
    }
    if (readingInputIndex == -1) {
        return DataInputStatus::NOTHING_AVAILABLE;
    }

    lastReadInputIndex = readingInputIndex;
    LOG("OmniStreamMultipleInputProcessor processInput reading index = " << readingInputIndex);
    DataInputStatus dataInputStatus = processors[readingInputIndex]->processInput();
    DataInputStatus status = inputSelectionHandler->updateStatusAndSelection(dataInputStatus, readingInputIndex);
    if (status == DataInputStatus::END_OF_RECOVERY) {
        suspendNum--;
        if (UNLIKELY(suspendNum < 0)) {
            LOG("Error: suspendNum should more than zero.");
            throw std::runtime_error("Error: suspendNum should more than zero.");
        }
        if (suspendNum != 0) {
            return DataInputStatus::MORE_AVAILABLE;
        }
    }
    return status;
}

std::shared_ptr<CompletableFutureV2<void>> OmniStreamMultipleInputProcessor::PrepareSnapshot(
    std::shared_ptr<ChannelStateWriter> writer, long checkpointID)
{
    LOG("MultipleInput prepare snapshot, checkpointID: " << checkpointID);
    //        std::vector<CompletableFutureV2<void>*> inputFutures;
    std::vector<std::shared_ptr<CompletableFutureV2<void>>> inputFutures;
    for (int index = 0; index < processors.size(); index++) {
        std::shared_ptr<CompletableFutureV2<void>> f = processors[index]->PrepareSnapshot(writer, checkpointID);
        if (f != nullptr) {
            inputFutures.push_back(f);
        } else {
            LOG_DEBUG(" inputFuture is null.");
        }
    }

    auto cf_ptr = std::make_shared<CompletableFutureV2<void>>();
    std::thread([cf_ptr, inputFutures]() mutable {
        try {
            for (auto& f : inputFutures) {
                f->Get();
            }
            cf_ptr->Complete();
        } catch (...) {
            cf_ptr->CompleteExceptionally(std::current_exception());
        }
    }).detach();

    return cf_ptr;
}

int OmniStreamMultipleInputProcessor::selectFirstReadingInputIndex()
{
    isPrepared = true;

    return selectNextReadingInputIndex();
}

void OmniStreamMultipleInputProcessor::FullCheckAndSetAvailable()
{
    for (size_t i = 0; i < processors.size(); i++) {
        auto inputProcessor = processors[i];
        // the input is constantly available and another is not, we will be checking this
        // volatile
        // once per every record. This might be optimized to only check once per processed
        // NetworkBuffer
        if (inputProcessor->isApproximatelyAvailable() || inputProcessor->isAvailable()) {
            inputSelectionHandler->SetAvailableInput(i);
        }
    }
}

int OmniStreamMultipleInputProcessor::selectNextReadingInputIndex()
{
    if (!inputSelectionHandler->isAnyInputAvailable()) {
        FullCheckAndSetAvailable();
    }

    int readingInputIndex = inputSelectionHandler->selectNextInputIndex(lastReadInputIndex);
    if (readingInputIndex == -1) {
        return -1;
    }

    if (inputSelectionHandler->ShouldSetAvailableForAnotherInput()) {
        FullCheckAndSetAvailable();
    }
    return readingInputIndex;
}

std::shared_ptr<CompletableFuture> OmniStreamMultipleInputProcessor::GetAvailableFuture()
{
    if (inputSelectionHandler->isAnyInputAvailable() || inputSelectionHandler->areAllInputsFinished()) {
        return AVAILABLE;
    }

    availabilityHelper->resetToUnAvailable();
    for (int i = 0; i < processors.size(); i++) {
        if (!inputSelectionHandler->isInputFinished(i) && inputSelectionHandler->isInputSelected(i)) {
            availabilityHelper->anyOf(i, processors[i]->GetAvailableFuture());
        }
    }
    return availabilityHelper->getAvailableFuture();
}

void OmniStreamMultipleInputProcessor::close()
{
    for (auto processor : processors) {
        if (processor != nullptr) {
            processor->close();
        }
    }
}
} // namespace omnistream
