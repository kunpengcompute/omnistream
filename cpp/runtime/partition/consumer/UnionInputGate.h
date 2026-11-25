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

#ifndef OMNISTREAM_UNIONINPUTGATE_H
#define OMNISTREAM_UNIONINPUTGATE_H

#include <condition_variable>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "IndexedInputGate.h"
#include "partition/PrioritizedDeque.h"

namespace omnistream {

class QueueInputGateRunnable;

class UnionInputGate : public InputGate {
public:
    explicit UnionInputGate(const std::vector<std::shared_ptr<IndexedInputGate>>& inputGates);
    ~UnionInputGate() override = default;

    int GetNumberOfInputChannels() override;
    bool IsFinished() override;
    bool HasReceivedEndOfData() override;
    BufferOrEvent* GetNext() override;
    BufferOrEvent* PollNext() override;
    void sendTaskEvent(const std::shared_ptr<TaskEvent>& event) override;
    void ResumeConsumption(const InputChannelInfo& channelInfo) override;
    void acknowledgeAllRecordsProcessed(const InputChannelInfo& channelInfo) override;
    std::shared_ptr<InputChannel> getChannel(int channelIndex) override;
    void setup() override;
    void RequestPartitions() override;
    std::shared_ptr<CompletableFutureV2<void>> getStateConsumedFuture() override;
    std::vector<bool> getStateConsumedFuture1() override;
    void FinishReadRecoveredState() override;
    std::string toString() override;

private:
    friend class QueueInputGateRunnable;

    struct GateWithData {
        std::shared_ptr<IndexedInputGate> input;
        BufferOrEvent* data;
        bool moreAvailable;
        bool morePriorityEvents;
    };

    void armDataAvailableCallback(const std::shared_ptr<IndexedInputGate>& inputGate);
    void armPriorityEventAvailableCallback(const std::shared_ptr<IndexedInputGate>& inputGate);
    void queueInputGate(const std::shared_ptr<IndexedInputGate>& inputGate, bool priority);
    std::optional<std::shared_ptr<IndexedInputGate>> getInputGate(
        bool blocking, std::unique_lock<std::recursive_mutex>& lock);
    std::optional<GateWithData> waitAndGetNextData(bool blocking);
    GateWithData processBufferOrEvent(const std::shared_ptr<IndexedInputGate>& inputGate, BufferOrEvent* bufferOrEvent);
    void handleEndOfPartitionEvent(BufferOrEvent* bufferOrEvent, const std::shared_ptr<IndexedInputGate>& inputGate);
    void handleEndOfUserDataEvent(BufferOrEvent* bufferOrEvent, const std::shared_ptr<IndexedInputGate>& inputGate);
    void markAvailable();

    std::unordered_map<int, std::shared_ptr<IndexedInputGate>> inputGatesByGateIndex;
    std::unordered_set<int> inputGatesWithRemainingData;
    std::unordered_set<int> inputGatesWithRemainingUserData;
    PrioritizedDeque<IndexedInputGate> inputGatesWithData;
    std::vector<int> inputChannelToInputGateIndex;
    std::vector<int> inputGateChannelIndexOffsets;
    bool shouldDrainOnEndOfData = true;

    std::recursive_mutex inputGatesWithDataMutex;
    std::condition_variable_any inputGatesWithDataCondition;
};

} // namespace omnistream

#endif // OMNISTREAM_UNIONINPUTGATE_H
