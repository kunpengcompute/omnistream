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

#include "UnionInputGate.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>

#include "event/EndOfData.h"
#include "event/EndOfPartitionEvent.h"

namespace omnistream {

class QueueInputGateRunnable : public Runnable {
public:
    QueueInputGateRunnable(UnionInputGate* owner, std::shared_ptr<IndexedInputGate> inputGate, bool priority)
        : owner_(owner),
          inputGate_(std::move(inputGate)),
          priority_(priority)
    {
    }

    void run() override
    {
        owner_->queueInputGate(inputGate_, priority_);
    }

private:
    UnionInputGate* owner_;
    std::shared_ptr<IndexedInputGate> inputGate_;
    bool priority_;
};

UnionInputGate::UnionInputGate(const std::vector<std::shared_ptr<IndexedInputGate>>& inputGates)
{
    if (inputGates.size() <= 1) {
        throw std::invalid_argument("Union input gate requires at least two input gates.");
    }

    int maxGateIndex = 0;
    int totalNumberOfInputChannels = 0;
    std::unordered_set<int> gateIndexes;

    for (const auto& inputGate : inputGates) {
        if (!inputGate) {
            throw std::invalid_argument("Union input gate does not accept null input gates.");
        }
        if (!gateIndexes.insert(inputGate->GetGateIndex()).second) {
            throw std::invalid_argument("Union of input gates with duplicate gate index is not supported.");
        }
        maxGateIndex = std::max(maxGateIndex, inputGate->GetGateIndex());
        totalNumberOfInputChannels += inputGate->GetNumberOfInputChannels();
        inputGatesByGateIndex.emplace(inputGate->GetGateIndex(), inputGate);
        inputGatesWithRemainingData.emplace(inputGate->GetGateIndex());
        inputGatesWithRemainingUserData.emplace(inputGate->GetGateIndex());
    }

    inputGateChannelIndexOffsets.assign(maxGateIndex + 1, 0);
    inputChannelToInputGateIndex.assign(totalNumberOfInputChannels, 0);

    int currentNumberOfInputChannels = 0;
    for (const auto& inputGate : inputGates) {
        inputGateChannelIndexOffsets[inputGate->GetGateIndex()] = currentNumberOfInputChannels;
        const int previousNumberOfInputChannels = currentNumberOfInputChannels;
        currentNumberOfInputChannels += inputGate->GetNumberOfInputChannels();
        std::fill(
            inputChannelToInputGateIndex.begin() + previousNumberOfInputChannels,
            inputChannelToInputGateIndex.begin() + currentNumberOfInputChannels,
            inputGate->GetGateIndex());

        if (inputGate->GetAvailableFuture()->isDone()) {
            queueInputGate(inputGate, false);
        } else {
            armDataAvailableCallback(inputGate);
        }

        if (inputGate->getPriorityEventAvailableFuture()->isDone()) {
            queueInputGate(inputGate, true);
        } else {
            armPriorityEventAvailableCallback(inputGate);
        }
    }
}

int UnionInputGate::GetNumberOfInputChannels()
{
    return static_cast<int>(inputChannelToInputGateIndex.size());
}

bool UnionInputGate::IsFinished()
{
    std::lock_guard<std::recursive_mutex> lock(inputGatesWithDataMutex);
    return inputGatesWithRemainingData.empty();
}

bool UnionInputGate::HasReceivedEndOfData()
{
    std::lock_guard<std::recursive_mutex> lock(inputGatesWithDataMutex);
    return inputGatesWithRemainingUserData.empty();
}

BufferOrEvent* UnionInputGate::GetNext()
{
    auto next = waitAndGetNextData(true);
    if (!next.has_value()) {
        return nullptr;
    }

    auto inputWithData = next.value();
    handleEndOfPartitionEvent(inputWithData.data, inputWithData.input);
    handleEndOfUserDataEvent(inputWithData.data, inputWithData.input);
    if (!inputWithData.data->moreAvailable()) {
        inputWithData.data->setMoreAvailable(inputWithData.moreAvailable);
    }
    return inputWithData.data;
}

BufferOrEvent* UnionInputGate::PollNext()
{
    auto next = waitAndGetNextData(false);
    if (!next.has_value()) {
        return nullptr;
    }

    auto inputWithData = next.value();
    handleEndOfPartitionEvent(inputWithData.data, inputWithData.input);
    handleEndOfUserDataEvent(inputWithData.data, inputWithData.input);
    if (!inputWithData.data->moreAvailable()) {
        inputWithData.data->setMoreAvailable(inputWithData.moreAvailable);
    }
    return inputWithData.data;
}

void UnionInputGate::sendTaskEvent(const std::shared_ptr<TaskEvent>& event)
{
    for (const auto& gateEntry : inputGatesByGateIndex) {
        gateEntry.second->sendTaskEvent(event);
    }
}

void UnionInputGate::ResumeConsumption(const InputChannelInfo& channelInfo)
{
    const auto gateIt = inputGatesByGateIndex.find(channelInfo.getGateIdx());
    if (gateIt == inputGatesByGateIndex.end()) {
        throw std::invalid_argument("Unknown input gate index when resuming consumption.");
    }
    std::static_pointer_cast<InputGate>(gateIt->second)->ResumeConsumption(channelInfo);
}

void UnionInputGate::acknowledgeAllRecordsProcessed(const InputChannelInfo& channelInfo)
{
    const auto gateIt = inputGatesByGateIndex.find(channelInfo.getGateIdx());
    if (gateIt == inputGatesByGateIndex.end()) {
        throw std::invalid_argument("Unknown input gate index when acknowledging records.");
    }
    gateIt->second->acknowledgeAllRecordsProcessed(channelInfo);
}

std::shared_ptr<InputChannel> UnionInputGate::getChannel(int channelIndex)
{
    if (channelIndex < 0 || channelIndex >= static_cast<int>(inputChannelToInputGateIndex.size())) {
        throw std::out_of_range("Union input gate channel index out of range.");
    }

    const int gateIndex = inputChannelToInputGateIndex[channelIndex];
    const auto gateIt = inputGatesByGateIndex.find(gateIndex);
    if (gateIt == inputGatesByGateIndex.end()) {
        throw std::runtime_error("Union input gate mapping points to an unknown gate.");
    }

    return gateIt->second->getChannel(channelIndex - inputGateChannelIndexOffsets[gateIndex]);
}

void UnionInputGate::setup()
{
}

void UnionInputGate::RequestPartitions()
{
    for (const auto& gateEntry : inputGatesByGateIndex) {
        gateEntry.second->RequestPartitions();
    }
}

std::shared_ptr<CompletableFutureV2<void>> UnionInputGate::getStateConsumedFuture()
{
    std::vector<std::shared_ptr<CompletableFutureV2<void>>> futures;
    futures.reserve(inputGatesByGateIndex.size());
    for (const auto& gateEntry : inputGatesByGateIndex) {
        futures.emplace_back(gateEntry.second->getStateConsumedFuture());
    }
    return CompletableFutureV2<void>::AllOf(futures);
}

std::vector<bool> UnionInputGate::getStateConsumedFuture1()
{
    std::vector<bool> results;
    for (const auto& gateEntry : inputGatesByGateIndex) {
        auto gateResults = gateEntry.second->getStateConsumedFuture1();
        results.insert(results.end(), gateResults.begin(), gateResults.end());
    }
    return results;
}

void UnionInputGate::FinishReadRecoveredState()
{
    for (const auto& gateEntry : inputGatesByGateIndex) {
        gateEntry.second->FinishReadRecoveredState();
    }
}

std::string UnionInputGate::toString()
{
    std::stringstream ss;
    ss << "UnionInputGate{";
    bool first = true;
    for (const auto& gateEntry : inputGatesByGateIndex) {
        if (!first) {
            ss << ", ";
        }
        first = false;
        ss << gateEntry.second->toString();
    }
    ss << "}";
    return ss.str();
}

void UnionInputGate::armDataAvailableCallback(const std::shared_ptr<IndexedInputGate>& inputGate)
{
    auto runnable = std::make_shared<QueueInputGateRunnable>(this, inputGate, false);
    inputGate->GetAvailableFuture()->thenRun(runnable);
}

void UnionInputGate::armPriorityEventAvailableCallback(const std::shared_ptr<IndexedInputGate>& inputGate)
{
    auto runnable = std::make_shared<QueueInputGateRunnable>(this, inputGate, true);
    inputGate->getPriorityEventAvailableFuture()->thenRun(runnable);
}

void UnionInputGate::queueInputGate(const std::shared_ptr<IndexedInputGate>& inputGate, bool priority)
{
    class GateNotificationHelper {
    public:
        GateNotificationHelper(UnionInputGate* inputGate, std::condition_variable_any& condition)
            : inputGate_(inputGate),
              condition_(&condition)
        {
        }

        ~GateNotificationHelper()
        {
            if (toNotifyPriority_ != nullptr) {
                toNotifyPriority_->complete();
            }
            if (toNotify_ != nullptr) {
                toNotify_->complete();
            }
        }

        void notifyPriority()
        {
            toNotifyPriority_ = inputGate_->priorityAvailabilityHelper.getUnavailableToResetAvailable();
        }

        void notifyDataAvailable()
        {
            condition_->notify_all();
            toNotify_ = inputGate_->availabilityHelper.getUnavailableToResetAvailable();
        }

    private:
        UnionInputGate* inputGate_;
        std::condition_variable_any* condition_;
        std::shared_ptr<CompletableFuture> toNotifyPriority_ = nullptr;
        std::shared_ptr<CompletableFuture> toNotify_ = nullptr;
    };

    if (!inputGate) {
        throw std::invalid_argument("Union input gate cannot enqueue a null gate.");
    }

    GateNotificationHelper notification(this, inputGatesWithDataCondition);

    std::unique_lock<std::recursive_mutex> lock(inputGatesWithDataMutex);
    if (inputGatesWithRemainingData.find(inputGate->GetGateIndex()) == inputGatesWithRemainingData.end()) {
        return;
    }

    const bool alreadyEnqueued = inputGatesWithData.contains(inputGate);
    if (alreadyEnqueued && (!priority || inputGatesWithData.containsPriorityElement(inputGate))) {
        return;
    }

    if (priority && !inputGate->getPriorityEventAvailableFuture()->isDone()) {
        return;
    }

    inputGatesWithData.add(inputGate, priority, alreadyEnqueued);
    if (priority && inputGatesWithData.getNumPriorityElements() == 1) {
        notification.notifyPriority();
    }
    if (inputGatesWithData.size() == 1) {
        notification.notifyDataAvailable();
    }
}

std::optional<std::shared_ptr<IndexedInputGate>> UnionInputGate::getInputGate(
    bool blocking, std::unique_lock<std::recursive_mutex>& lock)
{
    while (inputGatesWithData.isEmpty()) {
        if (inputGatesWithRemainingData.empty()) {
            return std::nullopt;
        }

        if (blocking) {
            inputGatesWithDataCondition.wait(
                lock, [this] { return !inputGatesWithData.isEmpty() || inputGatesWithRemainingData.empty(); });
        } else {
            availabilityHelper.resetUnavailable();
            return std::nullopt;
        }
    }

    return inputGatesWithData.poll();
}

std::optional<UnionInputGate::GateWithData> UnionInputGate::waitAndGetNextData(bool blocking)
{
    while (true) {
        std::unique_lock<std::recursive_mutex> lock(inputGatesWithDataMutex);
        auto inputGateOpt = getInputGate(blocking, lock);
        if (!inputGateOpt.has_value()) {
            return std::nullopt;
        }

        auto inputGate = inputGateOpt.value();
        BufferOrEvent* next = inputGate->PollNext();
        if (next == nullptr) {
            if (!inputGate->IsFinished()) {
                armDataAvailableCallback(inputGate);
            }
            continue;
        }

        return processBufferOrEvent(inputGate, next);
    }
}

UnionInputGate::GateWithData UnionInputGate::processBufferOrEvent(
    const std::shared_ptr<IndexedInputGate>& inputGate, BufferOrEvent* bufferOrEvent)
{
    if (bufferOrEvent->moreAvailable()) {
        inputGatesWithData.add(inputGate, bufferOrEvent->morePriorityEvents(), false);
    } else if (!inputGate->IsFinished()) {
        armDataAvailableCallback(inputGate);
    }

    if (bufferOrEvent->hasPriority() && !bufferOrEvent->morePriorityEvents()) {
        armPriorityEventAvailableCallback(inputGate);
    }

    const bool morePriorityEvents = inputGatesWithData.getNumPriorityElements() > 0;
    if (bufferOrEvent->hasPriority() && !morePriorityEvents) {
        priorityAvailabilityHelper.resetUnavailable();
    }

    return GateWithData{inputGate, bufferOrEvent, !inputGatesWithData.isEmpty(), morePriorityEvents};
}

void UnionInputGate::handleEndOfPartitionEvent(
    BufferOrEvent* bufferOrEvent, const std::shared_ptr<IndexedInputGate>& inputGate)
{
    if (!bufferOrEvent->isEvent()) {
        return;
    }

    if (dynamic_cast<EndOfPartitionEvent*>(bufferOrEvent->getEvent().get()) == nullptr || !inputGate->IsFinished()) {
        return;
    }

    if (bufferOrEvent->moreAvailable()) {
        throw std::logic_error("Union input gate received EndOfPartitionEvent with moreAvailable=true.");
    }

    std::lock_guard<std::recursive_mutex> lock(inputGatesWithDataMutex);
    if (inputGatesWithRemainingData.erase(inputGate->GetGateIndex()) == 0) {
        throw std::logic_error("Union input gate could not find finished input gate in remaining-data set.");
    }
    if (inputGatesWithRemainingData.empty()) {
        markAvailable();
    }
}

void UnionInputGate::handleEndOfUserDataEvent(
    BufferOrEvent* bufferOrEvent, const std::shared_ptr<IndexedInputGate>& inputGate)
{
    if (!bufferOrEvent->isEvent()) {
        return;
    }

    if (dynamic_cast<EndOfData*>(bufferOrEvent->getEvent().get()) == nullptr || !inputGate->HasReceivedEndOfData()) {
        return;
    }

    std::lock_guard<std::recursive_mutex> lock(inputGatesWithDataMutex);
    shouldDrainOnEndOfData &= inputGate->HasReceivedEndOfData();
    if (inputGatesWithRemainingUserData.erase(inputGate->GetGateIndex()) == 0) {
        throw std::logic_error("Union input gate could not find finished input gate in remaining-user-data set.");
    }
}

void UnionInputGate::markAvailable()
{
    auto toNotify = availabilityHelper.getUnavailableToResetAvailable();
    toNotify->complete();
    inputGatesWithDataCondition.notify_all();
}

} // namespace omnistream
