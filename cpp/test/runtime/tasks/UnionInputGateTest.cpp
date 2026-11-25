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

#include <deque>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "runtime/event/EndOfData.h"
#include "runtime/event/EndOfPartitionEvent.h"
#include "runtime/event/RuntimeEvent.h"
#include "runtime/event/TaskEvent.h"
#include "runtime/io/network/api/StopMode.h"
#include "runtime/metrics/Counter.h"
#include "runtime/partition/consumer/SingleInputGate.h"
#include "runtime/partition/consumer/UnionInputGate.h"

using namespace omnistream;

namespace {

class TestCounter : public Counter {
public:
    void Inc() override
    {
    }
    void Inc(long) override
    {
    }
    void Dec() override
    {
    }
    void Dec(long) override
    {
    }
    long GetCount() override
    {
        return 0;
    }
    MetricType GetMetricType() const override
    {
        return MetricType::COUNTER;
    }
};

class TestRuntimeEvent : public RuntimeEvent {
public:
    int GetEventClassID() override
    {
        return 1001;
    }
    std::string GetEventClassName() override
    {
        return "TestRuntimeEvent";
    }
};

class TestInputChannel : public InputChannel {
public:
    TestInputChannel(const std::shared_ptr<SingleInputGate>& gate, int channelIndex)
        : InputChannel(
              gate,
              channelIndex,
              ResultPartitionIDPOD(),
              0,
              0,
              std::make_shared<TestCounter>(),
              std::make_shared<TestCounter>())
    {
    }

    void resumeConsumption() override
    {
    }
    void TimeOutResumeConsumption() override
    {
    }
    void acknowledgeAllRecordsProcessed() override
    {
    }
    void requestSubpartition(int) override
    {
    }
    std::optional<BufferAndAvailability> getNextBuffer() override
    {
        return std::nullopt;
    }
    void sendTaskEvent(std::shared_ptr<TaskEvent>) override
    {
    }
    bool isReleased() override
    {
        return false;
    }
    void releaseAllResources() override
    {
    }
    void announceBufferSize(int) override
    {
    }
    int getBuffersInUseCount() override
    {
        return 0;
    }
    void SetChannelStateWriter(std::shared_ptr<ChannelStateWriter>) override
    {
    }
    void SetPersistenceFlag(bool) override
    {
    }
    bool IsNeedPersistence() override
    {
        return false;
    }
    void AddInputData(long, const InputChannelInfo&) override
    {
    }
    std::string toString() override
    {
        return "TestInputChannel";
    }
};

class TestIndexedInputGate : public IndexedInputGate {
public:
    TestIndexedInputGate(int gateIndex, int numChannels)
        : gateIndex_(gateIndex),
          numChannels_(numChannels),
          backingGate_(
              std::make_shared<SingleInputGate>(
                  "union-test",
                  gateIndex,
                  IntermediateDataSetIDPOD(),
                  0,
                  0,
                  numChannels,
                  nullptr,
                  std::function<std::shared_ptr<BufferPool>()>(),
                  nullptr,
                  1024)),
          stateConsumedFuture_(std::make_shared<CompletableFutureV2<void>>()),
          channelsWithEndOfPartition_(numChannels, false),
          channelsWithEndOfData_(numChannels, false)
    {
        channels_.reserve(numChannels);
        for (int channelIndex = 0; channelIndex < numChannels; ++channelIndex) {
            channels_.emplace_back(std::make_shared<TestInputChannel>(backingGate_, channelIndex));
        }
    }

    int GetGateIndex() override
    {
        return gateIndex_;
    }
    int GetNumberOfInputChannels() override
    {
        return numChannels_;
    }

    std::vector<InputChannelInfo> getUnfinishedChannels() override
    {
        std::vector<InputChannelInfo> infos;
        for (int channelIndex = 0; channelIndex < numChannels_; ++channelIndex) {
            if (!channelsWithEndOfPartition_[channelIndex]) {
                infos.emplace_back(gateIndex_, channelIndex);
            }
        }
        return infos;
    }

    int getBuffersInUseCount() override
    {
        return 0;
    }
    void announceBufferSize(int) override
    {
    }

    std::shared_ptr<InputChannel> getChannel(int channelIndex) override
    {
        return channels_.at(channelIndex);
    }

    bool IsFinished() override
    {
        return isFinished_;
    }
    bool HasReceivedEndOfData() override
    {
        return hasReceivedEndOfData_;
    }

    BufferOrEvent* GetNext() override
    {
        return pollInternal();
    }
    BufferOrEvent* PollNext() override
    {
        return pollInternal();
    }

    void sendTaskEvent(const std::shared_ptr<TaskEvent>& event) override
    {
        ++taskEventCalls_;
        lastTaskEvent_ = event;
    }

    void ResumeConsumption(const InputChannelInfo& channelInfo) override
    {
        resumedInfos_.push_back(channelInfo);
    }

    void acknowledgeAllRecordsProcessed(const InputChannelInfo& channelInfo) override
    {
        acknowledgedInfos_.push_back(channelInfo);
    }

    void setup() override
    {
    }

    void RequestPartitions() override
    {
        ++requestPartitionsCalls_;
    }

    std::shared_ptr<CompletableFutureV2<void>> getStateConsumedFuture() override
    {
        return stateConsumedFuture_;
    }

    std::vector<bool> getStateConsumedFuture1() override
    {
        return {stateConsumedFuture_->IsDone()};
    }

    void FinishReadRecoveredState() override
    {
        ++finishReadRecoveredStateCalls_;
    }

    void enqueueUserEvent(int channelIndex)
    {
        queuedEvents_.push_back({EventKind::USER, channelIndex});
        signalDataAvailable();
    }

    void enqueueEndOfData(int channelIndex)
    {
        queuedEvents_.push_back({EventKind::END_OF_DATA, channelIndex});
        signalDataAvailable();
    }

    void enqueueEndOfPartition(int channelIndex)
    {
        queuedEvents_.push_back({EventKind::END_OF_PARTITION, channelIndex});
        signalDataAvailable();
    }

    void signalAvailableWithoutData()
    {
        signalDataAvailable();
    }

    int requestPartitionsCalls() const
    {
        return requestPartitionsCalls_;
    }
    int finishReadRecoveredStateCalls() const
    {
        return finishReadRecoveredStateCalls_;
    }
    int taskEventCalls() const
    {
        return taskEventCalls_;
    }
    const std::vector<InputChannelInfo>& resumedInfos() const
    {
        return resumedInfos_;
    }
    const std::vector<InputChannelInfo>& acknowledgedInfos() const
    {
        return acknowledgedInfos_;
    }

private:
    enum class EventKind {
        USER,
        END_OF_DATA,
        END_OF_PARTITION
    };

    struct QueuedEvent {
        EventKind kind;
        int channelIndex;
    };

    void signalDataAvailable()
    {
        if (GetAvailableFuture()->isDone()) {
            return;
        }
        auto toNotify = availabilityHelper.getUnavailableToResetAvailable();
        toNotify->complete();
    }

    BufferOrEvent* pollInternal()
    {
        if (queuedEvents_.empty()) {
            availabilityHelper.resetUnavailable();
            return nullptr;
        }

        const QueuedEvent nextEvent = queuedEvents_.front();
        queuedEvents_.pop_front();
        if (queuedEvents_.empty()) {
            availabilityHelper.resetUnavailable();
        }

        const InputChannelInfo channelInfo(gateIndex_, nextEvent.channelIndex);
        const bool moreAvailable = !queuedEvents_.empty();

        switch (nextEvent.kind) {
            case EventKind::USER:
                return new BufferOrEvent(
                    std::make_shared<TestRuntimeEvent>(), false, channelInfo, moreAvailable, 0, false);
            case EventKind::END_OF_DATA:
                channelsWithEndOfData_[nextEvent.channelIndex] = true;
                hasReceivedEndOfData_ = std::all_of(
                    channelsWithEndOfData_.begin(), channelsWithEndOfData_.end(), [](bool done) { return done; });
                return new BufferOrEvent(
                    std::make_shared<EndOfData>(StopMode::DRAIN), false, channelInfo, moreAvailable, 0, false);
            case EventKind::END_OF_PARTITION:
                channelsWithEndOfPartition_[nextEvent.channelIndex] = true;
                isFinished_ =
                    std::all_of(channelsWithEndOfPartition_.begin(), channelsWithEndOfPartition_.end(), [](bool done) {
                        return done;
                    });
                return new BufferOrEvent(
                    EndOfPartitionEvent::getInstance(), false, channelInfo, moreAvailable, 0, false);
        }

        return nullptr;
    }

    int gateIndex_;
    int numChannels_;
    std::shared_ptr<SingleInputGate> backingGate_;
    std::shared_ptr<CompletableFutureV2<void>> stateConsumedFuture_;
    std::vector<std::shared_ptr<InputChannel>> channels_;
    std::deque<QueuedEvent> queuedEvents_;
    std::vector<bool> channelsWithEndOfPartition_;
    std::vector<bool> channelsWithEndOfData_;
    bool isFinished_ = false;
    bool hasReceivedEndOfData_ = false;
    int requestPartitionsCalls_ = 0;
    int finishReadRecoveredStateCalls_ = 0;
    int taskEventCalls_ = 0;
    std::shared_ptr<TaskEvent> lastTaskEvent_;
    std::vector<InputChannelInfo> resumedInfos_;
    std::vector<InputChannelInfo> acknowledgedInfos_;
};

TEST(UnionInputGateTest, MapsLogicalChannelsAcrossShiftedGateIndexes)
{
    auto gate1 = std::make_shared<TestIndexedInputGate>(2, 1);
    auto gate2 = std::make_shared<TestIndexedInputGate>(5, 1);
    gate1->enqueueUserEvent(0);
    gate2->enqueueUserEvent(0);
    gate1->enqueueEndOfData(0);
    gate2->enqueueEndOfData(0);
    gate1->enqueueEndOfPartition(0);
    gate2->enqueueEndOfPartition(0);

    UnionInputGate unionGate({gate1, gate2});

    ASSERT_EQ(unionGate.GetNumberOfInputChannels(), 2);
    EXPECT_EQ(unionGate.getChannel(0)->getChannelInfo(), InputChannelInfo(2, 0));
    EXPECT_EQ(unionGate.getChannel(1)->getChannelInfo(), InputChannelInfo(5, 0));

    std::unique_ptr<BufferOrEvent> first(unionGate.PollNext());
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->getChannelInfo(), unionGate.getChannel(0)->getChannelInfo());
    EXPECT_TRUE(first->moreAvailable());

    std::unique_ptr<BufferOrEvent> second(unionGate.PollNext());
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(second->getChannelInfo(), unionGate.getChannel(1)->getChannelInfo());
    EXPECT_TRUE(second->moreAvailable());

    std::unique_ptr<BufferOrEvent> third(unionGate.PollNext());
    ASSERT_NE(third, nullptr);
    EXPECT_TRUE(third->isEvent());
    EXPECT_TRUE(third->moreAvailable());
    EXPECT_FALSE(unionGate.HasReceivedEndOfData());

    std::unique_ptr<BufferOrEvent> fourth(unionGate.PollNext());
    ASSERT_NE(fourth, nullptr);
    EXPECT_TRUE(fourth->isEvent());
    EXPECT_TRUE(unionGate.HasReceivedEndOfData());
    EXPECT_TRUE(fourth->moreAvailable());

    std::unique_ptr<BufferOrEvent> fifth(unionGate.PollNext());
    ASSERT_NE(fifth, nullptr);
    EXPECT_TRUE(fifth->isEvent());
    EXPECT_TRUE(fifth->moreAvailable());
    EXPECT_FALSE(unionGate.IsFinished());

    std::unique_ptr<BufferOrEvent> sixth(unionGate.PollNext());
    ASSERT_NE(sixth, nullptr);
    EXPECT_TRUE(sixth->isEvent());
    EXPECT_FALSE(sixth->moreAvailable());
    EXPECT_TRUE(unionGate.IsFinished());
    EXPECT_EQ(unionGate.PollNext(), nullptr);
}

TEST(UnionInputGateTest, FansOutControlPlaneCallsToChildren)
{
    auto gate1 = std::make_shared<TestIndexedInputGate>(0, 1);
    auto gate2 = std::make_shared<TestIndexedInputGate>(3, 1);
    UnionInputGate unionGate({gate1, gate2});

    auto event = std::make_shared<TaskEvent>();
    unionGate.sendTaskEvent(event);
    unionGate.RequestPartitions();
    unionGate.ResumeConsumption(InputChannelInfo(3, 0));
    unionGate.acknowledgeAllRecordsProcessed(InputChannelInfo(0, 0));
    unionGate.FinishReadRecoveredState();

    EXPECT_EQ(gate1->taskEventCalls(), 1);
    EXPECT_EQ(gate2->taskEventCalls(), 1);
    EXPECT_EQ(gate1->requestPartitionsCalls(), 1);
    EXPECT_EQ(gate2->requestPartitionsCalls(), 1);
    ASSERT_EQ(gate2->resumedInfos().size(), 1U);
    EXPECT_EQ(gate2->resumedInfos().front(), InputChannelInfo(3, 0));
    ASSERT_EQ(gate1->acknowledgedInfos().size(), 1U);
    EXPECT_EQ(gate1->acknowledgedInfos().front(), InputChannelInfo(0, 0));
    EXPECT_EQ(gate1->finishReadRecoveredStateCalls(), 1);
    EXPECT_EQ(gate2->finishReadRecoveredStateCalls(), 1);
}

TEST(UnionInputGateTest, EmptyPollResetsAvailability)
{
    auto gate1 = std::make_shared<TestIndexedInputGate>(0, 1);
    auto gate2 = std::make_shared<TestIndexedInputGate>(1, 1);
    gate1->signalAvailableWithoutData();

    UnionInputGate unionGate({gate1, gate2});

    EXPECT_TRUE(unionGate.GetAvailableFuture()->isDone());
    EXPECT_EQ(unionGate.PollNext(), nullptr);
    EXPECT_FALSE(unionGate.GetAvailableFuture()->isDone());
}

} // namespace
