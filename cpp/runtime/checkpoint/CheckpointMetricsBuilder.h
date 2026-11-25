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

#ifndef OMNISTREAM_CHECKPOINTMETRICSBUILDER_H
#define OMNISTREAM_CHECKPOINTMETRICSBUILDER_H
#include <memory>

#include "CheckpointMetrics.h"
#include "core/utils/threads/CompletableFutureV2.h"

class CheckpointMetricsBuilder {
public:
    CheckpointMetricsBuilder()
    {
        bytesProcessedDuringAlignment_ = std::make_shared<CompletableFutureV2<long>>();
        alignmentDurationNanos_ = std::make_shared<CompletableFutureV2<long>>();
    };

    CheckpointMetricsBuilder* SetBytesProcessedDuringAlignment(long bytesProcessedDuringAlignment)
    {
        bytesProcessedDuringAlignment_->Complete(bytesProcessedDuringAlignment);
        return this;
    }

    CheckpointMetricsBuilder* SetBytesProcessedDuringAlignment(
        std::shared_ptr<CompletableFutureV2<long>> bytesProcessedDuringAlignment)
    {
        bytesProcessedDuringAlignment_ = bytesProcessedDuringAlignment;
        return this;
    }

    CheckpointMetricsBuilder* SetAlignmentDurationNanos(long alignmentDurationNanos)
    {
        alignmentDurationNanos_->Complete(alignmentDurationNanos);
        return this;
    }

    CheckpointMetricsBuilder* SetAlignmentDurationNanos(
        std::shared_ptr<CompletableFutureV2<long>> alignmentDurationNanos)
    {
        if (!alignmentDurationNanos_->IsDone()) {
            alignmentDurationNanos_ = alignmentDurationNanos;
        } else {
            THROW_LOGIC_EXCEPTION("alignmentDurationNanos has already been completed by someone else");
        }
        return this;
    }

    CheckpointMetricsBuilder* SetBytesPersistedDuringAlignment(long bytesPersistedDuringAlignment)
    {
        bytesPersistedDuringAlignment_ = bytesPersistedDuringAlignment;
        return this;
    }

    CheckpointMetricsBuilder* SetAsyncDurationMillis(long asyncDurationMillis)
    {
        asyncDurationMillis_ = asyncDurationMillis;
        return this;
    }

    CheckpointMetricsBuilder* SetSyncDurationMillis(long syncDurationMillis)
    {
        syncDurationMillis_ = syncDurationMillis;
        return this;
    }

    CheckpointMetricsBuilder* SetTotalBytesPersisted(long totalBytesPersisted)
    {
        totalBytesPersisted_ = totalBytesPersisted;
        return this;
    }

    CheckpointMetricsBuilder* SetBytesPersistedOfThisCheckpoint(long bytesPersistedOfThisCheckpoint)
    {
        bytesPersistedOfThisCheckpoint_ = bytesPersistedOfThisCheckpoint;
        return this;
    }

    CheckpointMetricsBuilder* SetCheckpointStartDelayNanos(long checkpointStartDelayNanos)
    {
        checkpointStartDelayNanos_ = checkpointStartDelayNanos;
        return this;
    }

    CheckpointMetrics* BuildIncomplete()
    {
        return new CheckpointMetrics(
            bytesProcessedDuringAlignment_->GetNow(-1),
            bytesPersistedDuringAlignment_,
            alignmentDurationNanos_->GetNow(-1),
            syncDurationMillis_,
            asyncDurationMillis_,
            checkpointStartDelayNanos_,
            bytesPersistedDuringAlignment_ > 0,
            bytesPersistedOfThisCheckpoint_,
            totalBytesPersisted_);
    }

    CheckpointMetrics* Build()
    {
        auto bpda = bytesProcessedDuringAlignment_->Get();
        auto adn = alignmentDurationNanos_->Get();
        return new CheckpointMetrics(
            bpda,
            bytesPersistedDuringAlignment_,
            adn,
            syncDurationMillis_,
            asyncDurationMillis_,
            checkpointStartDelayNanos_,
            bytesPersistedDuringAlignment_ > 0,
            bytesPersistedOfThisCheckpoint_,
            totalBytesPersisted_);
    }

private:
    long bytesPersistedDuringAlignment_ = -1;
    long asyncDurationMillis_ = -1;
    long syncDurationMillis_ = -1;
    long totalBytesPersisted_ = -1;
    long bytesPersistedOfThisCheckpoint_ = -1;
    long checkpointStartDelayNanos_ = -1;
    std::shared_ptr<CompletableFutureV2<long>> bytesProcessedDuringAlignment_;
    std::shared_ptr<CompletableFutureV2<long>> alignmentDurationNanos_;
};

#endif // OMNISTREAM_CHECKPOINTMETRICSBUILDER_H
