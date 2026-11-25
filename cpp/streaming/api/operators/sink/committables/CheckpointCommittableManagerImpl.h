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

#ifndef FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGERIMPL_H
#define FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGERIMPL_H

#include <map>
#include <memory>
#include <optional>
#include <vector>
#include <stdexcept>
#include <functional>
#include <algorithm>
#include <iostream>
#include <limits>
#include <sstream>
#include "CommittableMessage.h"
#include "CommittableSummary.h"
#include "CommittableWithLineage.h"
#include "CheckpointCommittableManager.h"
#include "SubtaskCommittableManager.h"
#include "CommitRequestImpl.h"

template <typename CommT>
class CheckpointCommittableManagerImpl : public CheckpointCommittableManager<CommT> {
public:
    using SubtaskCommittableManagers = std::map<int, std::shared_ptr<SubtaskCommittableManager<CommT>>>;

    CheckpointCommittableManagerImpl(int subtaskId, int numberOfSubtasks, std::optional<long> checkpointId)
        : subtaskId(subtaskId),
          numberOfSubtasks(numberOfSubtasks),
          checkpointId(checkpointId),
          subtasksCommittableManagers()
    {
    }

    CheckpointCommittableManagerImpl(
        const SubtaskCommittableManagers& subtasksCommittableManagers,
        int subtaskId,
        int numberOfSubtasks,
        std::optional<long> checkpointId)
        : subtasksCommittableManagers(subtasksCommittableManagers),
          subtaskId(subtaskId),
          numberOfSubtasks(numberOfSubtasks),
          checkpointId(checkpointId)
    {
    }

    long GetCheckpointId() const
    {
        if (!checkpointId) {
            throw std::invalid_argument("Checkpoint ID is not set");
        }
        return *checkpointId;
    }

    std::vector<std::shared_ptr<SubtaskCommittableManager<CommT>>> getSubCM() const
    {
        std::vector<std::shared_ptr<SubtaskCommittableManager<CommT>>> managers;
        for (const auto& entry : subtasksCommittableManagers) {
            managers.push_back(entry.second);
        }
        return managers;
    }

    void UpsertSummary(const CommittableSummary<CommT>& summary)
    {
        auto it = subtasksCommittableManagers.find(summary.GetSubtaskId());
        if (it == subtasksCommittableManagers.end()) {
            subtasksCommittableManagers[summary.GetSubtaskId()] = std::make_shared<SubtaskCommittableManager<CommT>>(
                summary.GetNumberOfCommittables(), subtaskId, summary.GetCheckpointId());
        } else {
            throw std::runtime_error("Updating CommittableSummary for the same subtask is not supported");
        }
    }

    void AddCommittable(const CommittableWithLineage<CommT>& committable)
    {
        getSubtaskCommittableManager(committable.GetSubtaskId())->Add(committable);
    }

    std::shared_ptr<SubtaskCommittableManager<CommT>> getSubtaskCommittableManager(int subtaskId) const
    {
        auto it = subtasksCommittableManagers.find(subtaskId);
        if (it == subtasksCommittableManagers.end()) {
            throw std::invalid_argument("Unknown subtask ID: " + std::to_string(subtaskId));
        }
        return it->second;
    }

    CommittableSummary<CommT> GetSummary() const
    {
        int totalCommittables = 0;
        int totalPending = 0;
        int totalFailed = 0;
        for (const auto& entry : subtasksCommittableManagers) {
            totalCommittables += entry.second->GetNumCommittables();
            totalPending += entry.second->GetNumPending();
            totalFailed += entry.second->GetNumFailed();
        }
        return CommittableSummary<CommT>(
            subtaskId, numberOfSubtasks, checkpointId, totalCommittables, totalPending, totalFailed);
    }

    bool IsFinished() const
    {
        for (const auto& entry : subtasksCommittableManagers) {
            if (!entry.second->IsFinished()) {
                return false;
            }
        }
        return true;
    }

    std::vector<CommittableWithLineage<CommT>> commit(bool fullyReceived, Committer<CommT>& committer)
    {
        std::vector<std::shared_ptr<CommitRequest<CommT>>> requests = getPendingRequests(fullyReceived);
        for (auto& request : requests) {
            request->SetSelected();
        }
        committer.Commit(requests);
        for (auto& request : requests) {
            request->SetCommittedIfNoError();
        }
        return drainFinished();
    }

    std::vector<std::shared_ptr<CommitRequest<CommT>>> getPendingRequests(bool fullyReceived) const
    {
        std::vector<std::shared_ptr<CommitRequest<CommT>>> requests;
        for (const auto& entry : subtasksCommittableManagers) {
            if (!fullyReceived || entry.second->HasReceivedAll()) {
                for (const auto& request : entry.second->GetPendingRequests()) {
                    requests.push_back(request);
                }
            }
        }
        return requests;
    }

    std::vector<CommittableWithLineage<CommT>> drainFinished()
    {
        std::vector<CommittableWithLineage<CommT>> finished;
        for (const auto& entry : subtasksCommittableManagers) {
            for (const auto& committable : entry.second->DrainCommitted()) {
                finished.push_back(committable);
            }
        }
        return finished;
    }

    CheckpointCommittableManagerImpl<CommT> Merge(const CheckpointCommittableManagerImpl<CommT>& other)
    {
        if (checkpointId != other.checkpointId) {
            throw std::invalid_argument("Checkpoint IDs do not match");
        }
        for (const auto& entry : other.subtasksCommittableManagers) {
            auto it = subtasksCommittableManagers.find(entry.first);
            if (it != subtasksCommittableManagers.end()) {
                it->second->Merge(*entry.second);
            } else {
                subtasksCommittableManagers.insert({entry.first, entry.second});
            }
        }
        return *this;
    }

    CheckpointCommittableManagerImpl<CommT> Copy() const
    {
        return CheckpointCommittableManagerImpl<CommT>(
            subtasksCommittableManagers, subtaskId, numberOfSubtasks, checkpointId);
    }

private:
    SubtaskCommittableManagers subtasksCommittableManagers;
    std::optional<long> checkpointId;
    int subtaskId;
    int numberOfSubtasks;
};

#endif // FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGERIMPL_H
