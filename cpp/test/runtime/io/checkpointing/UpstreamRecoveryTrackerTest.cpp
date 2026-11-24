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

#include <gtest/gtest.h>
#include <memory>

#include "runtime/io/checkpointing/UpstreamRecoveryTracker.h"
#include "runtime/partition/consumer/InputChannelInfo.h"
#include "runtime/partition/consumer/SingleInputGate.h"
#include "MockClasses.h"

using namespace omnistream;

TEST(UpstreamRecoveryTrackerImplTest, InitialAndNoOpBehavior)
{
    auto dummyGate = std::make_shared<SingleChannelDummyInputGate>(0);
    assert(dummyGate != nullptr);
    std::cout << "created SingleChannelDummyInputGate" << std::endl;

    auto tracker = UpstreamRecoveryTracker::forInputGate(dummyGate);
    std::cout << "created tracker" << std::endl;

    EXPECT_FALSE(tracker->allChannelsRecovered());
    std::cout << "Test1" << std::endl;

    // The NO_OP tracker should always report “all recovered” immediately
    auto tracker1 = UpstreamRecoveryTracker::NO_OP();
    std::cout << "Get first no_op" << std::endl;

    EXPECT_TRUE(tracker1->allChannelsRecovered());
    std::cout << "Test2" << std::endl;

    // And NO_OP should be a singleton: all calls return the same instance
    auto tracker2 = UpstreamRecoveryTracker::NO_OP();
    std::cout << "Get second no_op" << std::endl;

    EXPECT_EQ(tracker1.get(), tracker2.get());
    std::cout << "Test3" << std::endl;
}

TEST(UpstreamRecoveryTrackerImplTest, HandleSingleChannelRecovery)
{
    auto dummyGate = std::make_shared<SingleChannelDummyInputGate>(0);
    auto tracker = UpstreamRecoveryTracker::forInputGate(dummyGate);

    auto channels = dummyGate->getChannelInfos();
    ASSERT_EQ(channels.size(), 1);

    EXPECT_FALSE(tracker->allChannelsRecovered());
    tracker->handleEndOfRecovery(channels[0]);
    EXPECT_TRUE(tracker->allChannelsRecovered());
}

TEST(UpstreamRecoveryTrackerImplTest, DuplicateRecoveryThrows)
{
    auto gate = std::make_shared<MultiChannelDummyInputGate>();
    auto tracker = UpstreamRecoveryTracker::forInputGate(gate);

    auto infos = gate->getChannelInfos();
    ASSERT_EQ(infos.size(), 2);

    EXPECT_FALSE(tracker->allChannelsRecovered());
    // First recovery of channel 0: numUnrestoredChannels_ should decrease from 2 to 1
    tracker->handleEndOfRecovery(infos[0]);
    EXPECT_FALSE(tracker->allChannelsRecovered());

    // Attempt a second recovery of channel 0: numUnrestoredChannels_ is still > 0, but restoredChannels_ already
    // contains infos[0], so this should throw error
    EXPECT_THROW(tracker->handleEndOfRecovery(infos[0]), std::runtime_error);

    // Recover channel 1, now all channels have been recovered
    tracker->handleEndOfRecovery(infos[1]);
    EXPECT_TRUE(tracker->allChannelsRecovered());
}
