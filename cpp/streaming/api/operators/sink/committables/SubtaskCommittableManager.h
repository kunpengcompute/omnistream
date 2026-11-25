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

#ifndef FLINK_BENCHMARK_SUBTASKCOMMITTABLEMANAGER_H
#define FLINK_BENCHMARK_SUBTASKCOMMITTABLEMANAGER_H
#include <deque>
#include <memory>
#include <optional>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include "CommittableWithLineage.h"
#include "CommitRequestImpl.h"

template <typename CommT>
class SubtaskCommittableManager {
public:
    SubtaskCommittableManager(int numExpectedCommittables, int subtaskId, std::optional<long> checkpointId)
        : numExpectedCommittables(numExpectedCommittables),
          checkpointId(checkpointId),
          subtaskId(subtaskId),
          numDrained(0),
          numFailed(0),
          requests()
    {
    }

    explicit SubtaskCommittableManager(
        const std::vector<std::shared_ptr<CommitRequestImpl<CommT>>>& requests,
        int numExpectedCommittables,
        int numDrained,
        int numFailed,
        int subtaskId,
        std::optional<long> checkpointId)
        : numExpectedCommittables(numExpectedCommittables),
          checkpointId(checkpointId),
          subtaskId(subtaskId),
          numDrained(numDrained),
          numFailed(numFailed),
          requests(requests.begin(), requests.end())
    {
    }

    void Add(const CommittableWithLineage<CommT>& committable)
    {
        Add(committable.GetCommittable());
    }

    void Add(const CommT& committable)
    {
        if (requests.size() >= numExpectedCommittables) {
            throw std::runtime_error("Already received all committables.");
        }
        requests.push_back(std::make_shared<CommitRequestImpl<CommT>>(committable));
    }

    bool HasReceivedAll() const
    {
        return GetNumCommittables() == numExpectedCommittables;
    }

    int GetNumCommittables() const
    {
        return requests.size() + numDrained + numFailed;
    }

    int GetNumPending() const
    {
        return numExpectedCommittables - (numDrained + numFailed);
    }

    int GetNumFailed() const
    {
        return numFailed;
    }

    bool IsFinished() const
    {
        return GetNumPending() == 0;
    }

    std::vector<std::shared_ptr<CommitRequestImpl<CommT>>> GetPendingRequests() const
    {
        std::vector<std::shared_ptr<CommitRequestImpl<CommT>>> pendingRequests;
        for (const auto& request : requests) {
            if (!request->IsFinished()) {
                pendingRequests.push_back(request);
            }
        }
        return pendingRequests;
    }

    std::vector<CommittableWithLineage<CommT>> DrainCommitted()
    {
        std::vector<CommittableWithLineage<CommT>> committed;
        auto it = requests.begin();
        while (it != requests.end()) {
            if ((*it)->IsFinished()) {
                if ((*it)->GetState() == CommitRequestState::FAILED) {
                    numFailed += 1;
                    it = requests.erase(it);
                    continue;
                } else {
                    committed.push_back(CommittableWithLineage((*it)->GetCommittable(), checkpointId, subtaskId));
                }
            }
            ++it;
        }
        numDrained += committed.size();
        return committed;
    }

    int GetNumDrained() const
    {
        return numDrained;
    }

    int GetSubtaskId() const
    {
        return subtaskId;
    }

    std::optional<long> GetCheckpointId() const
    {
        return checkpointId;
    }

    std::deque<std::shared_ptr<CommitRequestImpl<CommT>>> GetRequests() const
    {
        return requests;
    }

    SubtaskCommittableManager<CommT> Merge(const SubtaskCommittableManager<CommT>& other)
    {
        if (other.GetSubtaskId() != this->GetSubtaskId()) {
            throw std::invalid_argument("Subtask IDs do not match");
        }
        this->numExpectedCommittables += other.numExpectedCommittables;
        this->requests.insert(this->requests.end(), other.requests.begin(), other.requests.end());
        this->numDrained += other.numDrained;
        this->numFailed += other.numFailed;
        return *this;
    }

    SubtaskCommittableManager<CommT> Copy() const
    {
        std::vector<std::shared_ptr<CommitRequestImpl<CommT>>> copiedRequests;
        for (const auto& request : requests) {
            copiedRequests.push_back(request->Copy());
        }
        return SubtaskCommittableManager<CommT>(
            copiedRequests, numExpectedCommittables, numDrained, numFailed, subtaskId, checkpointId);
    }

private:
    std::deque<std::shared_ptr<CommitRequestImpl<CommT>>> requests;
    int numExpectedCommittables;
    std::optional<long> checkpointId;
    int subtaskId;
    int numDrained;
    int numFailed;
};
#endif // FLINK_BENCHMARK_SUBTASKCOMMITTABLEMANAGER_H
