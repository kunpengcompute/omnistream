/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>

#include "runtime/checkpoint/SavepointType.h"
#include "runtime/checkpoint/CheckpointType.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/io/network/api/CheckpointBarrier.h"

using namespace omnistream;

// =========================================================================
// SavepointType tests
// =========================================================================

TEST(SavepointTypeTest, IsSavepointReturnsTrue)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    EXPECT_TRUE(sp->IsSavepoint());
}

TEST(SavepointTypeTest, SavepointIsSynchronousReturnsFalse)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    EXPECT_FALSE(sp->isSynchronous());
}

TEST(SavepointTypeTest, TerminateIsSynchronousReturnsTrue)
{
    auto sp = SavepointType::terminate(SavepointFormatType::NATIVE);
    EXPECT_TRUE(sp->isSynchronous());
}

TEST(SavepointTypeTest, SuspendIsSynchronousReturnsTrue)
{
    auto sp = SavepointType::suspend(SavepointFormatType::CANONICAL);
    EXPECT_TRUE(sp->isSynchronous());
}

TEST(SavepointTypeTest, ShouldDrainTrueForTerminateFalseForOthers)
{
    auto terminate = SavepointType::terminate(SavepointFormatType::NATIVE);
    auto savepoint = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    auto suspend = SavepointType::suspend(SavepointFormatType::CANONICAL);

    EXPECT_TRUE(terminate->shouldDrain());
    EXPECT_FALSE(savepoint->shouldDrain());
    EXPECT_FALSE(suspend->shouldDrain());
}

TEST(SavepointTypeTest, ShouldIgnoreEndOfInputTrueForSuspendFalseForOthers)
{
    auto terminate = SavepointType::terminate(SavepointFormatType::NATIVE);
    auto savepoint = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    auto suspend = SavepointType::suspend(SavepointFormatType::CANONICAL);

    EXPECT_FALSE(terminate->shouldIgnoreEndOfInput());
    EXPECT_FALSE(savepoint->shouldIgnoreEndOfInput());
    EXPECT_TRUE(suspend->shouldIgnoreEndOfInput());
}

TEST(SavepointTypeTest, GetSharingFilesStrategyReturnsNoSharing)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::NATIVE);
    EXPECT_EQ(sp->GetSharingFilesStrategy(), SnapshotType::SharingFilesStrategy::NO_SHARING);
}

TEST(SavepointTypeTest, GetFormatTypeReturnsCorrectType)
{
    auto canonical = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    auto native = SavepointType::savepoint(SavepointFormatType::NATIVE);

    EXPECT_EQ(canonical->getFormatType(), SavepointFormatType::CANONICAL);
    EXPECT_EQ(native->getFormatType(), SavepointFormatType::NATIVE);
}

TEST(SavepointTypeTest, OperatorEqualsWithSameTypeAndFormat)
{
    auto a = SavepointType::savepoint(SavepointFormatType::NATIVE);
    auto b = SavepointType::savepoint(SavepointFormatType::NATIVE);
    EXPECT_TRUE(*a == *b);
}

TEST(SavepointTypeTest, OperatorEqualsDifferentFormat)
{
    auto a = SavepointType::terminate(SavepointFormatType::CANONICAL);
    auto b = SavepointType::terminate(SavepointFormatType::NATIVE);
    EXPECT_FALSE(*a == *b);
}

TEST(SavepointTypeTest, OperatorEqualsCrossTypeWithCheckpoint)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    EXPECT_FALSE((*sp) == (*CheckpointType::CHECKPOINT));
}

TEST(SavepointTypeTest, FactorySavepointProducesCorrectName)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    EXPECT_EQ(sp->GetName(), "Savepoint");
}

TEST(SavepointTypeTest, FactoryTerminateProducesCorrectName)
{
    auto sp = SavepointType::terminate(SavepointFormatType::NATIVE);
    EXPECT_EQ(sp->GetName(), "Terminate Savepoint");
}

TEST(SavepointTypeTest, FactorySuspendProducesCorrectName)
{
    auto sp = SavepointType::suspend(SavepointFormatType::CANONICAL);
    EXPECT_EQ(sp->GetName(), "Suspend Savepoint");
}

TEST(SavepointTypeTest, ShouldAdvanceToEndOfTimeDelegatesToShouldDrain)
{
    auto terminate = SavepointType::terminate(SavepointFormatType::NATIVE);
    auto savepoint = SavepointType::savepoint(SavepointFormatType::CANONICAL);

    EXPECT_TRUE(terminate->shouldAdvanceToEndOfTime());
    EXPECT_FALSE(savepoint->shouldAdvanceToEndOfTime());
}

TEST(SavepointTypeTest, ToStringContainsKeyFields)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::NATIVE);
    std::string str = sp->ToString();
    EXPECT_NE(str.find("Savepoint"), std::string::npos);
}

// =========================================================================
// CheckpointType tests
// =========================================================================

TEST(CheckpointTypeTest, IsSavepointReturnsFalse)
{
    EXPECT_FALSE(CheckpointType::CHECKPOINT->IsSavepoint());
    EXPECT_FALSE(CheckpointType::FULL_CHECKPOINT->IsSavepoint());
}

TEST(CheckpointTypeTest, CheckpointSharingStrategyIsForwardBackward)
{
    EXPECT_EQ(
        CheckpointType::CHECKPOINT->GetSharingFilesStrategy(), SnapshotType::SharingFilesStrategy::FORWARD_BACKWARD);
}

TEST(CheckpointTypeTest, FullCheckpointSharingStrategyIsForward)
{
    EXPECT_EQ(CheckpointType::FULL_CHECKPOINT->GetSharingFilesStrategy(), SnapshotType::SharingFilesStrategy::FORWARD);
}

TEST(CheckpointTypeTest, OperatorEqualsSameType)
{
    EXPECT_TRUE(*CheckpointType::CHECKPOINT == *CheckpointType::CHECKPOINT);
}

TEST(CheckpointTypeTest, OperatorEqualsDifferentType)
{
    EXPECT_FALSE(*CheckpointType::CHECKPOINT == *CheckpointType::FULL_CHECKPOINT);
}

TEST(CheckpointTypeTest, OperatorEqualsCrossTypeWithSavepoint)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    EXPECT_FALSE(CheckpointType::CHECKPOINT->operator==(*sp));
}

TEST(CheckpointTypeTest, GetNameReturnsCorrect)
{
    EXPECT_EQ(CheckpointType::CHECKPOINT->GetName(), "Checkpoint");
    EXPECT_EQ(CheckpointType::FULL_CHECKPOINT->GetName(), "FullCheckpoint");
}

// =========================================================================
// CheckpointOptions - constructor guard tests
// =========================================================================

TEST(CheckpointOptionsTest, SavepointCannotBeUnaligned)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    EXPECT_THROW(
        CheckpointOptions(
            sp,
            CheckpointStorageLocationReference::GetDefault(),
            CheckpointOptions::AlignmentType::UNALIGNED,
            CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT),
        std::invalid_argument);
}

TEST(CheckpointOptionsTest, UnalignedCheckpointCannotHaveTimeout)
{
    EXPECT_THROW(
        CheckpointOptions(
            CheckpointType::CHECKPOINT,
            CheckpointStorageLocationReference::GetDefault(),
            CheckpointOptions::AlignmentType::UNALIGNED,
            10000L),
        std::invalid_argument);
}

TEST(CheckpointOptionsTest, CannotHaveNullTargetLocation)
{
    EXPECT_THROW(
        CheckpointOptions(
            CheckpointType::CHECKPOINT,
            nullptr,
            CheckpointOptions::AlignmentType::ALIGNED,
            CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT),
        std::invalid_argument);
}

TEST(CheckpointOptionsTest, CannotHaveNullCheckpointType)
{
    EXPECT_THROW(
        CheckpointOptions(
            nullptr,
            CheckpointStorageLocationReference::GetDefault(),
            CheckpointOptions::AlignmentType::ALIGNED,
            CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT),
        std::invalid_argument);
}

TEST(CheckpointOptionsTest, AlignedNoTimeoutCreatesCorrectDefaults)
{
    auto* options = CheckpointOptions::AlignedNoTimeout(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    EXPECT_EQ(options->GetAlignment(), CheckpointOptions::AlignmentType::ALIGNED);
    EXPECT_EQ(options->GetAlignedCheckpointTimeout(), CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT);
    EXPECT_TRUE(options->IsExactlyOnceMode());
    delete options;
}

TEST(CheckpointOptionsTest, NotExactlyOnceCreatesAtLeastOnce)
{
    auto* options = CheckpointOptions::NotExactlyOnce(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    EXPECT_EQ(options->GetAlignment(), CheckpointOptions::AlignmentType::AT_LEAST_ONCE);
    EXPECT_FALSE(options->IsExactlyOnceMode());
    delete options;
}

// =========================================================================
// CheckpointOptions - ForConfig decision tree tests
// =========================================================================

TEST(CheckpointOptionsTest, ForConfigNotExactlyOnceReturnsAtLeastOnce)
{
    auto* options = CheckpointOptions::ForConfig(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault(), false, true, 10000L);
    EXPECT_EQ(options->GetAlignment(), CheckpointOptions::AlignmentType::AT_LEAST_ONCE);
    delete options;
}

TEST(CheckpointOptionsTest, ForConfigSavepointReturnsAlignedNoTimeout)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    auto* options =
        CheckpointOptions::ForConfig(*sp, CheckpointStorageLocationReference::GetDefault(), true, true, 10000L);
    EXPECT_EQ(options->GetAlignment(), CheckpointOptions::AlignmentType::ALIGNED);
    EXPECT_EQ(options->GetAlignedCheckpointTimeout(), CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT);
    delete options;
}

TEST(CheckpointOptionsTest, ForConfigUnalignedDisabledReturnsAlignedNoTimeout)
{
    auto* options = CheckpointOptions::ForConfig(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault(), true, false, 10000L);
    EXPECT_EQ(options->GetAlignment(), CheckpointOptions::AlignmentType::ALIGNED);
    delete options;
}

TEST(CheckpointOptionsTest, ForConfigUnalignedZeroTimeoutReturnsUnaligned)
{
    auto* options = CheckpointOptions::ForConfig(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault(), true, true, 0L);
    EXPECT_TRUE(options->IsUnalignedCheckpoint());
    delete options;
}

TEST(CheckpointOptionsTest, ForConfigUnalignedMaxTimeoutReturnsUnaligned)
{
    auto* options = CheckpointOptions::ForConfig(
        *CheckpointType::CHECKPOINT,
        CheckpointStorageLocationReference::GetDefault(),
        true,
        true,
        CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT);
    EXPECT_TRUE(options->IsUnalignedCheckpoint());
    delete options;
}

TEST(CheckpointOptionsTest, ForConfigWithTimeoutReturnsAlignedWithTimeout)
{
    auto* options = CheckpointOptions::ForConfig(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault(), true, true, 5000L);
    EXPECT_EQ(options->GetAlignment(), CheckpointOptions::AlignmentType::ALIGNED);
    EXPECT_EQ(options->GetAlignedCheckpointTimeout(), 5000L);
    delete options;
}

// =========================================================================
// CheckpointOptions - IsTimeoutable / NeedsChannelState tests
// =========================================================================

TEST(CheckpointOptionsTest, AlignedWithTimeoutIsTimeoutable)
{
    auto* options = CheckpointOptions::AlignedWithTimeout(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault(), 5000L);
    EXPECT_TRUE(options->IsTimeoutable());
    EXPECT_TRUE(options->NeedsChannelState());
    delete options;
}

TEST(CheckpointOptionsTest, AlignedNoTimeoutIsNotTimeoutable)
{
    auto* options = CheckpointOptions::AlignedNoTimeout(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    EXPECT_FALSE(options->IsTimeoutable());
    delete options;
}

TEST(CheckpointOptionsTest, UnalignedNeedsChannelState)
{
    auto* options =
        CheckpointOptions::Unaligned(*CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    EXPECT_FALSE(options->IsTimeoutable());
    EXPECT_TRUE(options->IsUnalignedCheckpoint());
    EXPECT_TRUE(options->NeedsChannelState());
    delete options;
}

TEST(CheckpointOptionsTest, AtLeastOnceDoesNotNeedChannelState)
{
    auto* options = CheckpointOptions::NotExactlyOnce(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    EXPECT_FALSE(options->NeedsChannelState());
    delete options;
}

// =========================================================================
// CheckpointOptions - ToUnaligned / WithUnaligned tests
// =========================================================================

TEST(CheckpointOptionsTest, ToUnalignedOnAlignedReturnsUnaligned)
{
    auto* options = CheckpointOptions::AlignedNoTimeout(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    auto* unaligned = options->ToUnaligned();
    EXPECT_TRUE(unaligned->IsUnalignedCheckpoint());
    delete options;
    delete unaligned;
}

TEST(CheckpointOptionsTest, ToUnalignedOnUnalignedThrows)
{
    auto* options =
        CheckpointOptions::Unaligned(*CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    EXPECT_THROW(options->ToUnaligned(), std::logic_error);
    delete options;
}

TEST(CheckpointOptionsTest, NeedsAlignmentForCheckpointAligned)
{
    auto* options = CheckpointOptions::AlignedNoTimeout(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    EXPECT_TRUE(options->NeedsAlignment());
    delete options;
}

TEST(CheckpointOptionsTest, NeedsAlignmentForCheckpointUnaligned)
{
    auto* options =
        CheckpointOptions::Unaligned(*CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    EXPECT_FALSE(options->NeedsAlignment());
    delete options;
}

TEST(CheckpointOptionsTest, NeedsAlignmentForSavepoint)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    auto* options = CheckpointOptions::AlignedNoTimeout(*sp, CheckpointStorageLocationReference::GetDefault());
    EXPECT_TRUE(options->NeedsAlignment());
    delete options;
}

// =========================================================================
// CheckpointOptions - OperatorEquals tests
// =========================================================================

TEST(CheckpointOptionsTest, OperatorEqualsSameOptions)
{
    auto* a = CheckpointOptions::AlignedNoTimeout(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    auto* b = CheckpointOptions::AlignedNoTimeout(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    EXPECT_TRUE(*a == *b);
    delete a;
    delete b;
}

TEST(CheckpointOptionsTest, OperatorEqualsDifferentAlignment)
{
    auto* a = CheckpointOptions::AlignedNoTimeout(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    auto* b = CheckpointOptions::NotExactlyOnce(
        *CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    EXPECT_FALSE(*a == *b);
    delete a;
    delete b;
}

TEST(CheckpointOptionsTest, ForCheckpointWithDefaultLocationReturnsNonNull)
{
    auto* options = CheckpointOptions::ForCheckpointWithDefaultLocation();
    EXPECT_NE(options, nullptr);
    EXPECT_TRUE(options->IsExactlyOnceMode());
}

// =========================================================================
// CheckpointOptions - FromJson tests
// =========================================================================

TEST(CheckpointOptionsTest, FromJsonCheckpointAligned)
{
    nlohmann::json json;
    json["alignment"] = "ALIGNED";
    json["alignedCheckpointTimeout"] = CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT;
    json["checkpointType"]["name"] = "Checkpoint";
    json["checkpointType"]["sharingFilesStrategy"] = "FORWARD_BACKWARD";
    json["targetLocation"]["referenceBytes"] = nlohmann::json();

    auto* options = CheckpointOptions::FromJson(json);
    EXPECT_EQ(options->GetAlignment(), CheckpointOptions::AlignmentType::ALIGNED);
    EXPECT_FALSE(options->GetCheckpointType()->IsSavepoint());
    delete options;
}

TEST(CheckpointOptionsTest, FromJsonCheckpointUnaligned)
{
    nlohmann::json json;
    json["alignment"] = "UNALIGNED";
    json["alignedCheckpointTimeout"] = CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT;
    json["checkpointType"]["name"] = "Checkpoint";
    json["targetLocation"]["referenceBytes"] = nlohmann::json();

    auto* options = CheckpointOptions::FromJson(json);
    EXPECT_TRUE(options->IsUnalignedCheckpoint());
    delete options;
}

TEST(CheckpointOptionsTest, FromJsonFullCheckpoint)
{
    nlohmann::json json;
    json["alignment"] = "ALIGNED";
    json["alignedCheckpointTimeout"] = CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT;
    json["checkpointType"]["name"] = "Full Checkpoint";
    json["targetLocation"]["referenceBytes"] = nlohmann::json();

    auto* options = CheckpointOptions::FromJson(json);
    EXPECT_EQ(options->GetCheckpointType()->GetName(), "FullCheckpoint");
    delete options;
}

TEST(CheckpointOptionsTest, FromJsonSavepointCanonical)
{
    nlohmann::json json;
    json["alignment"] = "ALIGNED";
    json["alignedCheckpointTimeout"] = CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT;
    json["checkpointType"]["name"] = "Savepoint";
    json["checkpointType"]["formatType"] = "CANONICAL";
    json["targetLocation"]["referenceBytes"] = nlohmann::json();

    auto* options = CheckpointOptions::FromJson(json);
    EXPECT_TRUE(options->GetCheckpointType()->IsSavepoint());
    EXPECT_EQ(options->GetCheckpointType()->GetName(), "Savepoint");
    delete options;
}

TEST(CheckpointOptionsTest, FromJsonTerminateSavepointNative)
{
    nlohmann::json json;
    json["alignment"] = "ALIGNED";
    json["alignedCheckpointTimeout"] = CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT;
    json["checkpointType"]["name"] = "Terminate Savepoint";
    json["checkpointType"]["formatType"] = "NATIVE";
    json["targetLocation"]["referenceBytes"] = nlohmann::json();

    auto* options = CheckpointOptions::FromJson(json);
    EXPECT_TRUE(options->GetCheckpointType()->IsSavepoint());
    EXPECT_EQ(options->GetCheckpointType()->GetName(), "Terminate Savepoint");
    auto* sp = dynamic_cast<SavepointType*>(options->GetCheckpointType());
    EXPECT_TRUE(sp->shouldDrain());
    delete options;
}

TEST(CheckpointOptionsTest, FromJsonSuspendSavepoint)
{
    nlohmann::json json;
    json["alignment"] = "ALIGNED";
    json["alignedCheckpointTimeout"] = CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT;
    json["checkpointType"]["name"] = "Suspend Savepoint";
    json["checkpointType"]["formatType"] = "CANONICAL";
    json["targetLocation"]["referenceBytes"] = nlohmann::json();

    auto* options = CheckpointOptions::FromJson(json);
    EXPECT_TRUE(options->GetCheckpointType()->IsSavepoint());
    delete options;
}

TEST(CheckpointOptionsTest, FromJsonUnknownAlignmentThrows)
{
    nlohmann::json json;
    json["alignment"] = "INVALID";
    json["alignedCheckpointTimeout"] = 0;
    json["checkpointType"]["name"] = "Checkpoint";
    json["targetLocation"]["referenceBytes"] = nlohmann::json();

    EXPECT_THROW(CheckpointOptions::FromJson(json), std::invalid_argument);
}

TEST(CheckpointOptionsTest, FromJsonUnknownSavepointFormatThrows)
{
    nlohmann::json json;
    json["alignment"] = "ALIGNED";
    json["alignedCheckpointTimeout"] = CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT;
    json["checkpointType"]["name"] = "Savepoint";
    json["checkpointType"]["formatType"] = "INVALID_FORMAT";
    json["targetLocation"]["referenceBytes"] = nlohmann::json();

    EXPECT_THROW(CheckpointOptions::FromJson(json), std::invalid_argument);
}

TEST(CheckpointOptionsTest, FromJsonUnknownSavepointTypeThrows)
{
    nlohmann::json json;
    json["alignment"] = "ALIGNED";
    json["alignedCheckpointTimeout"] = CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT;
    json["checkpointType"]["name"] = "UnknownSavepointType";
    json["checkpointType"]["formatType"] = "CANONICAL";
    json["targetLocation"]["referenceBytes"] = nlohmann::json();

    EXPECT_THROW(CheckpointOptions::FromJson(json), std::invalid_argument);
}

TEST(CheckpointOptionsTest, FromJsonUnknownCheckpointTypeThrows)
{
    nlohmann::json json;
    json["alignment"] = "ALIGNED";
    json["alignedCheckpointTimeout"] = CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT;
    json["checkpointType"]["name"] = "InvalidCheckpoint";
    json["targetLocation"]["referenceBytes"] = nlohmann::json();

    EXPECT_THROW(CheckpointOptions::FromJson(json), std::invalid_argument);
}

// =========================================================================
// CheckpointBarrier tests
// =========================================================================

TEST(CheckpointBarrierTest, SharedPtrConstructorThrowsOnNullOptions)
{
    EXPECT_THROW(CheckpointBarrier(1, 1000, std::shared_ptr<CheckpointOptions>(nullptr)), std::invalid_argument);
}

TEST(CheckpointBarrierTest, IsCheckpointReturnsTrueForCheckpoint)
{
    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(1, 1000, options);
    EXPECT_TRUE(barrier.IsCheckpoint());
}

TEST(CheckpointBarrierTest, IsCheckpointReturnsFalseForSavepoint)
{
    auto sp = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    auto options = std::make_shared<CheckpointOptions>(sp, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(1, 1000, options);
    EXPECT_FALSE(barrier.IsCheckpoint());
}

TEST(CheckpointBarrierTest, WithOptionsSameReturnsThis)
{
    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(1, 1000, options);
    auto* result = barrier.WithOptions(options);
    EXPECT_EQ(result, &barrier);
}

TEST(CheckpointBarrierTest, GetIdReturnsCorrect)
{
    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(42, 999, options);
    EXPECT_EQ(barrier.GetId(), 42);
    EXPECT_EQ(barrier.GetTimestamp(), 999);
}

TEST(CheckpointBarrierTest, OperatorEqualsSameBarrier)
{
    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier a(1, 1000, options);
    CheckpointBarrier b(1, 1000, options);
    EXPECT_TRUE(a == b);
}

TEST(CheckpointBarrierTest, OperatorEqualsDifferentId)
{
    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier a(1, 1000, options);
    CheckpointBarrier b(2, 1000, options);
    EXPECT_FALSE(a == b);
}

TEST(CheckpointBarrierTest, OperatorEqualsDifferentTimestamp)
{
    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier a(1, 1000, options);
    CheckpointBarrier b(1, 2000, options);
    EXPECT_FALSE(a == b);
}

TEST(CheckpointBarrierTest, AsUnalignedAlreadyUnalignedReturnsThis)
{
    auto opts = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT,
        CheckpointStorageLocationReference::GetDefault(),
        CheckpointOptions::AlignmentType::UNALIGNED,
        CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT);
    CheckpointBarrier barrier(1, 1000, opts);
    bool isNeedDel = false;
    auto* result = barrier.AsUnaligned(isNeedDel);
    EXPECT_EQ(result, &barrier);
}

TEST(CheckpointBarrierTest, AsUnalignedCreatesNewWhenAligned)
{
    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(1, 1000, options);
    bool isNeedDel = false;
    auto* result = barrier.AsUnaligned(isNeedDel);
    EXPECT_NE(result, &barrier);
    EXPECT_TRUE(result->GetCheckpointOptions()->IsUnalignedCheckpoint());
    delete result;
}

TEST(CheckpointBarrierTest, GetEventClassNameReturnsCheckpointBarrier)
{
    auto options = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(1, 1000, options);
    EXPECT_EQ(barrier.GetEventClassName(), "CheckpointBarrier");
}

// =========================================================================
// CheckpointBarrier - WithOptions creates new when different
// =========================================================================

TEST(CheckpointBarrierTest, WithOptionsDifferentReturnsNewBarrier)
{
    auto opts1 = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    auto opts2 = std::make_shared<CheckpointOptions>(
        CheckpointType::CHECKPOINT,
        CheckpointStorageLocationReference::GetDefault(),
        CheckpointOptions::AlignmentType::AT_LEAST_ONCE,
        CheckpointOptions::NO_ALIGNED_CHECKPOINT_TIME_OUT);
    CheckpointBarrier barrier(1, 1000, opts1);

    auto* result = barrier.WithOptions(opts2);
    ASSERT_NE(result, &barrier);
    EXPECT_EQ(result->GetId(), 1);
    EXPECT_EQ(result->GetTimestamp(), 1000);
    EXPECT_FALSE(result->GetCheckpointOptions()->IsExactlyOnceMode());

    delete result;
}
