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

#ifndef FLINK_BENCHMARK_COMMITTER_H
#define FLINK_BENCHMARK_COMMITTER_H

#include <vector>
#include <exception>
#include <stdexcept>
#include <memory>
#include "connector/kafka/sink/KafkaCommittable.h"

/**
 * A request to Commit a specific committable.
 *
 * @param <CommT>
 */
template <typename CommT>
class CommitRequest {
public:
    virtual ~CommitRequest() = default;

    /**
     * Returns the committable.
     */
    virtual CommT GetCommittable() const = 0;

    /**
     * Returns how many times this particular committable has been retried. Starts at 0 for the
     * first attempt.
     */
    virtual int GetNumberOfRetries() const = 0;

    /**
     * The Commit failed for known reason and should not be retried.
     */
    virtual void signalFailedWithKnownReason(const std::exception& t) = 0;

    /**
     * The Commit failed for unknown reason and should not be retried.
     */
    virtual void signalFailedWithUnknownReason(const std::exception& t) = 0;

    /**
     * The Commit failed for a retriable reason. If the sink supports a retry maximum, this may
     * permanently fail after reaching that maximum. Else the committable will be retried as
     * long as this method is invoked after each attempt.
     */
    virtual void RetryLater() = 0;

    /**
     * Updates the underlying committable and retries later. This method can be used if a
     * committable partially succeeded.
     */
    virtual void UpdateAndRetryLater(const CommT& committable) = 0;

    /**
     * Signals that a committable is skipped as it was committed already in a previous run.
     */
    virtual void SignalAlreadyCommitted() = 0;

    /**
     * Marks the committable as selected for commit.
     */
    virtual void SetSelected() = 0;

    /**
     * Marks the committable as committed if no error occurred.
     */
    virtual void SetCommittedIfNoError() = 0;
};

/**
 * The Committer is responsible for committing the data staged by the SinkWriter in the second step
 * of a two-phase Commit protocol.
 *
 * @param <CommT> The type of information needed to Commit the staged data
 */
template <typename CommT>
class Committer {
public:
    virtual ~Committer() = default;

    /**
     * Commit the given list of CommitRequests.
     *
     * @param committables A list of Commit requests staged by the sink writer.
     * @throws std::exception for reasons that may yield a complete restart of the job.
     */
    virtual void Commit(std::vector<std::shared_ptr<CommitRequest<CommT>>>& committables) = 0;

    /**
     * Close the committer.
     */
    virtual void Close() = 0;
};
#endif // FLINK_BENCHMARK_COMMITTER_H
