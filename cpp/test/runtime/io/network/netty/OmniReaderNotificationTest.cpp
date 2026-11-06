/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#include <gtest/gtest.h>
#include <array>
#include "runtime/io/network/netty/GlobalNettyBufferPool.h"
#include "runtime/io/network/netty/OmniCreditBasedSequenceNumberingViewReader.h"

using namespace omnistream;

namespace {
class OmniReaderNotificationTest : public testing::Test {
protected:
    GlobalNettyBufferPool pool{NettyBufferConf(10, 1024, 1, 10)};
    std::array<uint8_t, 256> descriptors{};
    std::vector<std::shared_ptr<NettyMemorySegment>> payloads;
    OutputBufferStatus status;
    std::unique_ptr<OmniCreditBasedSequenceNumberingViewReader> reader;

    void SetUp() override
    {
        status.outputBuffer_ = reinterpret_cast<uintptr_t>(descriptors.data());
        status.capacity_ = descriptors.size();
        reader = std::make_unique<OmniCreditBasedSequenceNumberingViewReader>(
            ResultPartitionIDPOD{}, 0, reinterpret_cast<long>(&status), true, pool.createLocalPool(1));
    }

    void Publish()
    {
        auto segment = std::make_shared<NettyMemorySegment>(new uint8_t[32]{}, 32);
        payloads.push_back(segment);
        auto info = std::make_shared<NettyBufferInfo>(segment);
        info->SetWrittenBytes(1);
        info->IncrementElementNum();
        reader->AddNettyBufferInfoToQueue(info);
        EXPECT_EQ(info, nullptr);
    }
};

TEST_F(OmniReaderNotificationTest, CoalescesUntilQueueIsDrained)
{
    int notifications = 0;
    reader->SetDataAvailableCallback([&] { ++notifications; });
    EXPECT_EQ(notifications, 0);
    Publish();
    Publish();
    EXPECT_EQ(notifications, 1);
    EXPECT_EQ(reader->getNextBuffer(), 2);
    EXPECT_EQ(reader->GetNextBufferType(), -1);
    Publish();
    EXPECT_EQ(notifications, 2);
}

TEST_F(OmniReaderNotificationTest, RegistrationAfterPublicationDoesNotLoseWakeup)
{
    Publish();
    int notifications = 0;
    reader->SetDataAvailableCallback([&] {
        EXPECT_EQ(reader->getAvailabilityAndBacklog(), 1);
        ++notifications;
    });
    EXPECT_EQ(notifications, 1);
    reader->SetDataAvailableCallback(nullptr);
    EXPECT_EQ(reader->getNextBuffer(), 1);
    Publish();
    EXPECT_EQ(notifications, 1);
}

TEST_F(OmniReaderNotificationTest, NativeBacklogSurvivesDescriptorBatchLimit)
{
    for (int i = 0; i < 12; ++i) {
        Publish();
    }
    EXPECT_EQ(reader->getNextBuffer(), 10);
    EXPECT_EQ(status.numberElement, 10);
    EXPECT_EQ(reader->getAvailabilityAndBacklog(), 2);
    EXPECT_NE(reader->GetNextBufferType(), -1);
    EXPECT_EQ(reader->getNextBuffer(), 2);
    EXPECT_EQ(reader->getNextBuffer(), 0);
}
} // namespace
