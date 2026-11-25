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

#ifndef FLINK_BENCHMARK_COMMITTABLECOLLECTOR_H
#define FLINK_BENCHMARK_COMMITTABLECOLLECTOR_H

#include <map>
#include <memory>
#include <optional>
#include <vector>
#include <stdexcept>
#include <algorithm>
#include <functional>
#include <iostream>
#include <limits>
#include <sstream>
#include "CommittableMessage.h"
#include "CommittableSummary.h"
#include "CommittableWithLineage.h"
#include "CheckpointCommittableManagerImpl.h"

template <typename CommT>
class CommittableCollector {
public:
    using CheckpointCommittableMap = std::map<long, std::shared_ptr<CheckpointCommittableManagerImpl<CommT>>>;

    CommittableCollector(int subtaskId, int numberOfSubtasks)
        : subtaskId(subtaskId),
          numberOfSubtasks(numberOfSubtasks),
          checkpointCommittables()
    {
    }

    CommittableCollector(const CheckpointCommittableMap& checkpointCommittables, int subtaskId, int numberOfSubtasks)
        : checkpointCommittables(checkpointCommittables),
          subtaskId(subtaskId),
          numberOfSubtasks(numberOfSubtasks)
    {
    }

    static CommittableCollector<CommT> Of(int subtaskId, int numberOfSubtasks)
    {
        return CommittableCollector<CommT>(subtaskId, numberOfSubtasks);
    }

    static CommittableCollector<CommT> ofLegacy(const std::vector<CommT>& committables)
    {
        CommittableCollector<CommT> committableCollector(0, 1);
        CommittableSummary<CommT> summary(0, 1, 0, committables.size(), committables.size(), 0);
        committableCollector.AddSummary(summary);
        for (const auto& c : committables) {
            CommittableWithLineage<CommT> committableWithLineage(c, 0, 0);
            committableCollector.AddCommittable(committableWithLineage);
        }
        return committableCollector;
    }

    void AddMessage(const CommittableMessage<CommT>& message)
    {
        if (const auto* summary = dynamic_cast<const CommittableSummary<CommT>*>(&message)) {
            AddSummary(*summary);
        } else if (const auto* committable = dynamic_cast<const CommittableWithLineage<CommT>*>(&message)) {
            AddCommittable(*committable);
        } else {
            throw std::invalid_argument("Unknown message type");
        }
    }

    std::vector<std::shared_ptr<CheckpointCommittableManager<CommT>>> getChkComUp(long checkpointId)
    {
        std::vector<std::shared_ptr<CheckpointCommittableManager<CommT>>> result;
        for (const auto& entry : checkpointCommittables) {
            if (entry.first <= checkpointId) {
                result.push_back(entry.second);
            }
        }
        return result;
    }

    std::shared_ptr<CommittableManager<CommT>> getEndOfInputCommittable()
    {
        auto it = checkpointCommittables.find(eoi);
        if (it != checkpointCommittables.end()) {
            return it->second;
        }
        return nullptr;
    }

    bool IsFinished() const
    {
        for (const auto& entry : checkpointCommittables) {
            if (!entry.second->IsFinished()) {
                return false;
            }
        }
        return true;
    }

    void Merge(const CommittableCollector<CommT>& cc)
    {
        for (const auto& entry : cc.checkpointCommittables) {
            long checkpointId = entry.first;
            const auto& otherManager = entry.second;

            auto it = checkpointCommittables.find(checkpointId);
            if (it != checkpointCommittables.end()) {
                it->second->Merge(*otherManager);
            } else {
                checkpointCommittables[checkpointId] = otherManager;
            }
        }
    }

    int GetNumberOfSubtasks() const
    {
        return numberOfSubtasks;
    }

    int GetSubtaskId() const
    {
        return subtaskId;
    }

    CommittableCollector<CommT> Copy() const
    {
        CheckpointCommittableMap newCheckpointCommittables;
        for (const auto& entry : checkpointCommittables) {
            newCheckpointCommittables[entry.first] =
                std::make_shared<CheckpointCommittableManagerImpl<CommT>>(entry.second->Copy());
        }
        return CommittableCollector<CommT>(newCheckpointCommittables, subtaskId, numberOfSubtasks);
    }

    std::vector<std::shared_ptr<CheckpointCommittableManagerImpl<CommT>>> getChkCom() const
    {
        std::vector<std::shared_ptr<CheckpointCommittableManagerImpl<CommT>>> result;
        for (const auto& entry : checkpointCommittables) {
            result.push_back(entry.second);
        }
        return result;
    }

private:
    static const long eoi;

    CheckpointCommittableMap checkpointCommittables;
    int subtaskId;
    int numberOfSubtasks;

    void AddSummary(const CommittableSummary<CommT>& summary)
    {
        auto it = checkpointCommittables.find(summary.GetCheckpointId().value_or(eoi));
        if (it == checkpointCommittables.end()) {
            auto cv = std::make_shared<CheckpointCommittableManagerImpl<CommT>>(
                subtaskId, numberOfSubtasks, summary.GetCheckpointId().value_or(eoi));
            checkpointCommittables[summary.GetCheckpointId().value_or(eoi)] = cv;
            cv->UpsertSummary(summary);
        } else {
            it->second->UpsertSummary(summary);
        }
    }

    void AddCommittable(const CommittableWithLineage<CommT>& committable)
    {
        GetCheckpointCommittables(committable)->AddCommittable(committable);
    }

    std::shared_ptr<CheckpointCommittableManagerImpl<CommT>> GetCheckpointCommittables(
        const CommittableMessage<CommT>& committable)
    {
        const long checkpointId = committable.GetCheckpointId().value_or(eoi);
        auto it = checkpointCommittables.find(checkpointId);
        if (it != checkpointCommittables.end()) {
            return it->second;
        }

        INFO_RELEASE("Exception: Missing committable manager for checkpointId " << checkpointId);
        throw std::runtime_error(
            "Exception: Missing committable manager for checkpointId " + std::to_string(checkpointId));
    }
};

template <typename CommT>
const long CommittableCollector<CommT>::eoi = std::numeric_limits<long>::max();

#endif // FLINK_BENCHMARK_COMMITTABLECOLLECTOR_H
