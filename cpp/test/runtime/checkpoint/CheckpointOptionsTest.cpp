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
#include "runtime/checkpoint/CheckpointOptions.h"

TEST(CheckpointOptionsTest, ConstructorTest)
{
    auto checkpointType = CheckpointType::CHECKPOINT;
    auto targetLocation = CheckpointStorageLocationReference::GetDefault();
    CheckpointOptions* options = new CheckpointOptions(checkpointType, targetLocation);

    EXPECT_EQ(options->GetCheckpointType(), checkpointType.get());
    EXPECT_EQ(options->GetTargetLocation(), targetLocation);
    EXPECT_EQ(options->GetAlignment(), CheckpointOptions::AlignmentType::ALIGNED);
    EXPECT_EQ(options->GetAlignedCheckpointTimeout(), CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT);

    delete options;
}

TEST(CheckpointOptionsTest, NeedsAlignmentTest)
{
    auto location = CheckpointStorageLocationReference::GetDefault();

    EXPECT_FALSE(CheckpointOptions(
                     CheckpointType::CHECKPOINT,
                     location,
                     CheckpointOptions::AlignmentType::UNALIGNED,
                     CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT)
                     .NeedsAlignment());

    EXPECT_TRUE(CheckpointOptions(
                    CheckpointType::CHECKPOINT,
                    location,
                    CheckpointOptions::AlignmentType::ALIGNED,
                    CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT)
                    .NeedsAlignment());

    EXPECT_TRUE(CheckpointOptions(
                    CheckpointType::CHECKPOINT,
                    location,
                    CheckpointOptions::AlignmentType::FORCED_ALIGNED,
                    CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT)
                    .NeedsAlignment());

    EXPECT_FALSE(CheckpointOptions(
                     CheckpointType::CHECKPOINT,
                     location,
                     CheckpointOptions::AlignmentType::AT_LEAST_ONCE,
                     CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT)
                     .NeedsAlignment());
}

void AssertTimeoutable(const CheckpointOptions& options, bool isUnaligned, bool isTimeoutable, int64_t timeout)
{
    EXPECT_TRUE(options.IsExactlyOnceMode());
    EXPECT_EQ(!isUnaligned, options.NeedsAlignment());
    EXPECT_EQ(isUnaligned, options.IsUnalignedCheckpoint());
    EXPECT_EQ(isTimeoutable, options.IsTimeoutable());
    EXPECT_EQ(timeout, options.GetAlignedCheckpointTimeout());
}

void AssertReversable(CheckpointOptions& options, bool forceHasEffect)
{
    CheckpointOptions* unalignedSupported = options.WithUnalignedSupported();
    EXPECT_EQ(options, *unalignedSupported);

    CheckpointOptions* unalignedUnsupported = options.WithUnalignedUnsupported();
    if (forceHasEffect) {
        EXPECT_NE(options, *unalignedUnsupported);
    } else {
        EXPECT_EQ(options, *unalignedUnsupported);
    }
    CheckpointOptions* roundtrip = unalignedUnsupported->WithUnalignedSupported();
    EXPECT_EQ(options, *roundtrip);
}

TEST(CheckpointOptionsTest, CheckpointIsTimeoutableTest)
{
    auto location = CheckpointStorageLocationReference::GetDefault();

    CheckpointOptions* alignedWithTimeout =
        CheckpointOptions::AlignedWithTimeout(*CheckpointType::CHECKPOINT, location, 10);
    AssertTimeoutable(*alignedWithTimeout, false, true, 10);
    delete alignedWithTimeout;

    CheckpointOptions* unaligned = CheckpointOptions::Unaligned(*CheckpointType::CHECKPOINT, location);
    AssertTimeoutable(*unaligned, true, false, CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT);
    delete unaligned;

    CheckpointOptions* alignedWithTimeout2 =
        CheckpointOptions::AlignedWithTimeout(*CheckpointType::CHECKPOINT, location, 10);
    CheckpointOptions* alignedWithTimeout2UnalignedUnsupported = alignedWithTimeout2->WithUnalignedUnsupported();
    AssertTimeoutable(*alignedWithTimeout2UnalignedUnsupported, false, false, 10);
    delete alignedWithTimeout2;
    delete alignedWithTimeout2UnalignedUnsupported;

    CheckpointOptions* unaligned2 = CheckpointOptions::Unaligned(*CheckpointType::CHECKPOINT, location);
    CheckpointOptions* unaligned2UnalignedUnsupported = unaligned2->WithUnalignedUnsupported();
    AssertTimeoutable(*unaligned2UnalignedUnsupported, false, false, CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT);
    delete unaligned2;
    delete unaligned2UnalignedUnsupported;
}

TEST(CheckpointOptionsTest, ForceAlignmentIsReversable)
{
    auto location = CheckpointStorageLocationReference::GetDefault();

    CheckpointOptions* alignedWithTimeout =
        CheckpointOptions::AlignedWithTimeout(*CheckpointType::CHECKPOINT, location, 10);
    AssertReversable(*alignedWithTimeout, true);
    delete alignedWithTimeout;

    CheckpointOptions* unaligned = CheckpointOptions::Unaligned(*CheckpointType::CHECKPOINT, location);
    AssertReversable(*unaligned, true);
    delete unaligned;

    CheckpointOptions* alignedNoTimeout = CheckpointOptions::AlignedNoTimeout(*CheckpointType::CHECKPOINT, location);
    AssertReversable(*alignedNoTimeout, false);
    delete alignedNoTimeout;

    CheckpointOptions* notExactlyOnce = CheckpointOptions::NotExactlyOnce(*CheckpointType::CHECKPOINT, location);
    AssertReversable(*notExactlyOnce, false);
    delete notExactlyOnce;
}
