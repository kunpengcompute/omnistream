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
#include "runtime/io/network/api/CheckpointBarrier.h"
#include "runtime/io/network/api/CancelCheckpointMarker.h"
#include "runtime/io/network/api/serialization/EventSerializer.h"
#include "runtime/event/EndOfData.h"
#include <memory>
TEST(CheckpointBarrierTest, ConstructorTest)
{
    auto checkpointType = CheckpointType::CHECKPOINT;
    auto targetLocation = CheckpointStorageLocationReference::GetDefault();
    auto options = new CheckpointOptions(checkpointType, targetLocation);

    CheckpointBarrier barrier(1, 1000, options);

    EXPECT_EQ(barrier.GetId(), 1);
    EXPECT_EQ(barrier.GetTimestamp(), 1000);
    EXPECT_EQ(barrier.GetCheckpointOptions().get(), options);
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithDefaultLocation)
{
    CheckpointOptions* options =
        new CheckpointOptions(CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSpecLocation)
{
    std::vector<uint8_t> bytes = {0x01, 0x02, 0x03, 0x04};
    auto refBytes = std::make_shared<std::vector<uint8_t>>(bytes);
    auto location = std::make_shared<CheckpointStorageLocationReference>(refBytes);
    CheckpointOptions* options = new CheckpointOptions(CheckpointType::CHECKPOINT, location);
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithFullCheckpoint)
{
    std::vector<uint8_t> bytes = {0x01, 0x02, 0x03, 0x04};
    auto refBytes = std::make_shared<std::vector<uint8_t>>(bytes);
    auto location = std::make_shared<CheckpointStorageLocationReference>(refBytes);
    CheckpointOptions* options = new CheckpointOptions(CheckpointType::FULL_CHECKPOINT, location);
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointNoneCanonical)
{
    std::vector<uint8_t> bytes = {0x01, 0x02, 0x03, 0x04};
    auto refBytes = std::make_shared<std::vector<uint8_t>>(bytes);
    auto location = std::make_shared<CheckpointStorageLocationReference>(refBytes);
    auto savepoint = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    CheckpointOptions* options = new CheckpointOptions(savepoint, location);
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointNoneNative)
{
    std::vector<uint8_t> bytes = {0x01, 0x02, 0x03, 0x04};
    auto refBytes = std::make_shared<std::vector<uint8_t>>(bytes);
    auto location = std::make_shared<CheckpointStorageLocationReference>(refBytes);
    auto savepoint = SavepointType::savepoint(SavepointFormatType::NATIVE);
    CheckpointOptions* options = new CheckpointOptions(savepoint, location);
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointTerminateNative)
{
    std::vector<uint8_t> bytes = {0x01, 0x02, 0x03, 0x04};
    auto refBytes = std::make_shared<std::vector<uint8_t>>(bytes);
    auto location = std::make_shared<CheckpointStorageLocationReference>(refBytes);
    auto savepoint = SavepointType::terminate(SavepointFormatType::NATIVE);
    CheckpointOptions* options = new CheckpointOptions(savepoint, location);
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointTerminateCanonical)
{
    std::vector<uint8_t> bytes = {0x01, 0x02, 0x03, 0x04};
    auto refBytes = std::make_shared<std::vector<uint8_t>>(bytes);
    auto location = std::make_shared<CheckpointStorageLocationReference>(refBytes);
    auto savepoint = SavepointType::terminate(SavepointFormatType::CANONICAL);
    CheckpointOptions* options = new CheckpointOptions(savepoint, location);
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointSuspendCanonical)
{
    std::vector<uint8_t> bytes = {0x01, 0x02, 0x03, 0x04};
    auto refBytes = std::make_shared<std::vector<uint8_t>>(bytes);
    auto location = std::make_shared<CheckpointStorageLocationReference>(refBytes);
    auto savepoint = SavepointType::suspend(SavepointFormatType::CANONICAL);
    CheckpointOptions* options = new CheckpointOptions(savepoint, location);
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointSuspendNative)
{
    std::vector<uint8_t> bytes = {0x01, 0x02, 0x03, 0x04};
    auto refBytes = std::make_shared<std::vector<uint8_t>>(bytes);
    auto location = std::make_shared<CheckpointStorageLocationReference>(refBytes);
    auto savepoint = SavepointType::suspend(SavepointFormatType::NATIVE);
    CheckpointOptions* options = new CheckpointOptions(savepoint, location);
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeCancelCheckpointMarker)
{
    CancelCheckpointMarker marker(12345L);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CancelCheckpointMarker>(marker), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desMarker = std::dynamic_pointer_cast<CancelCheckpointMarker>(deserializedEvent);

    ASSERT_NE(desMarker, nullptr);
    EXPECT_EQ(marker.getCheckpointId(), desMarker->getCheckpointId());
}

TEST(CheckpointBarrierTest, serializeDeserializeEndOfDataKeepsStopMode)
{
    EndOfData endOfData(StopMode::NO_DRAIN);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<EndOfData>(endOfData), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desEndOfData = std::dynamic_pointer_cast<EndOfData>(deserializedEvent);

    ASSERT_NE(desEndOfData, nullptr);
    EXPECT_EQ(StopMode::NO_DRAIN, desEndOfData->getStopMode());
}
