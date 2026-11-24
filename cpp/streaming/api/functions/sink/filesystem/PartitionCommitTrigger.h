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

#ifndef PARTITION_COMMIT_TRIGGER_H
#define PARTITION_COMMIT_TRIGGER_H

#include <set>
#include <map>
#include <string>
#include <vector>
#include <algorithm>
#include <chrono>
#include <cstdint>

/**
 * Context for partition commit predicate evaluation.
 */
struct PredicateContext {
    std::string partition;
    int64_t createProcTime = 0;
    int64_t currentProcTime = 0;
    int64_t currentWatermark = 0;
};

/**
 * Predicate that determines whether a partition is ready to be committed.
 */
class PartitionCommitPredicate {
public:
    virtual ~PartitionCommitPredicate() = default;
    virtual bool isPartitionCommittable(const PredicateContext& ctx) = 0;
};

/**
 * Commit predicated by partition time and watermark.
 * If 'watermark' > 'partitionTime' + 'delay', the partition is committable.
 */
class PartitionTimeCommitPredicate : public PartitionCommitPredicate {
public:
    PartitionTimeCommitPredicate(int64_t commitDelayMs) : commitDelayMs_(commitDelayMs)
    {
    }

    bool isPartitionCommittable(const PredicateContext& ctx) override
    {
        return ctx.currentWatermark > ctx.createProcTime + commitDelayMs_;
    }

private:
    int64_t commitDelayMs_;
};

/**
 * Commit predicated by processing time and creation time + delay.
 * If 'currentProcTime' > 'createProcTime' + 'delay', the partition is committable.
 */
class ProcTimeCommitPredicate : public PartitionCommitPredicate {
public:
    ProcTimeCommitPredicate(int64_t commitDelayMs) : commitDelayMs_(commitDelayMs)
    {
    }

    bool isPartitionCommittable(const PredicateContext& ctx) override
    {
        if (commitDelayMs_ == 0) {
            return true;
        }
        return ctx.currentProcTime > ctx.createProcTime + commitDelayMs_;
    }

private:
    int64_t commitDelayMs_;
};

/**
 * Interface for partition commit trigger.
 * Determines which partitions are committable based on watermark or processing time.
 */
class PartitionCommitTrigger {
public:
    virtual ~PartitionCommitTrigger() = default;

    virtual void addPartition(const std::string& partition) = 0;

    virtual std::vector<std::string> committablePartitions(long checkpointId) = 0;

    virtual std::vector<std::string> endInput() = 0;

    virtual void snapshotState(long checkpointId, long watermark) = 0;
};

/**
 * Commit trigger by partition time and watermark.
 * Stores pending partitions and per-checkpoint watermarks.
 * A partition is committable when watermark passes partition creation time + delay.
 */
class PartitionTimeCommitTrigger : public PartitionCommitTrigger {
public:
    PartitionTimeCommitTrigger(PartitionCommitPredicate* predicate) : predicate_(predicate)
    {
    }

    void addPartition(const std::string& partition) override
    {
        if (!partition.empty()) {
            pendingPartitions_.insert(partition);
        }
    }

    std::vector<std::string> committablePartitions(long checkpointId) override
    {
        if (watermarks_.find(checkpointId) == watermarks_.end()) {
            return {};
        }

        long watermark = watermarks_[checkpointId];

        auto end = watermarks_.upper_bound(checkpointId);
        watermarks_.erase(watermarks_.begin(), end);

        std::vector<std::string> needCommit;
        auto it = pendingPartitions_.begin();
        while (it != pendingPartitions_.end()) {
            PredicateContext ctx;
            ctx.partition = *it;
            ctx.currentWatermark = watermark;
            if (predicate_ && predicate_->isPartitionCommittable(ctx)) {
                needCommit.push_back(*it);
                it = pendingPartitions_.erase(it);
            } else {
                ++it;
            }
        }
        return needCommit;
    }

    std::vector<std::string> endInput() override
    {
        std::vector<std::string> result(pendingPartitions_.begin(), pendingPartitions_.end());
        pendingPartitions_.clear();
        return result;
    }

    void snapshotState(long checkpointId, long watermark) override
    {
        watermarks_[checkpointId] = watermark;
    }

protected:
    std::set<std::string> pendingPartitions_;
    std::map<long, long> watermarks_;
    PartitionCommitPredicate* predicate_;
};

/**
 * Commit trigger by processing time.
 * Stores pending partitions with their creation (first-seen) processing time.
 * A partition is committable when (currentProcTime > createProcTime + delay).
 */
class ProcTimeCommitTrigger : public PartitionCommitTrigger {
public:
    ProcTimeCommitTrigger(PartitionCommitPredicate* predicate) : predicate_(predicate)
    {
    }

    void addPartition(const std::string& partition) override
    {
        if (!partition.empty() && pendingPartitions_.find(partition) == pendingPartitions_.end()) {
            pendingPartitions_[partition] = currentProcTimeMs();
        }
    }

    std::vector<std::string> committablePartitions(long /*checkpointId*/) override
    {
        std::vector<std::string> needCommit;
        int64_t now = currentProcTimeMs();
        auto it = pendingPartitions_.begin();
        while (it != pendingPartitions_.end()) {
            PredicateContext ctx;
            ctx.partition = it->first;
            ctx.createProcTime = it->second;
            ctx.currentProcTime = now;
            if (predicate_ && predicate_->isPartitionCommittable(ctx)) {
                needCommit.push_back(it->first);
                it = pendingPartitions_.erase(it);
            } else {
                ++it;
            }
        }
        return needCommit;
    }

    std::vector<std::string> endInput() override
    {
        std::vector<std::string> result;
        for (const auto& pair : pendingPartitions_) {
            result.push_back(pair.first);
        }
        pendingPartitions_.clear();
        return result;
    }

    void snapshotState(long /*checkpointId*/, long /*watermark*/) override
    {
        // ProcTimeCommitTrigger does not need to snapshot watermarks
    }

private:
    static int64_t currentProcTimeMs()
    {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    std::map<std::string, int64_t> pendingPartitions_;
    PartitionCommitPredicate* predicate_;
};

/**
 * Factory to create the appropriate PartitionCommitTrigger based on config.
 */
PartitionCommitTrigger* createPartitionCommitTrigger(const std::string& triggerType, int64_t commitDelayMs);

#endif // PARTITION_COMMIT_TRIGGER_H
