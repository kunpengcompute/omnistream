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

#ifndef PARTITION_COMMITTER_H
#define PARTITION_COMMITTER_H

#include <map>
#include <set>
#include <string>
#include <vector>
#include <memory>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/utils/PartitionPathUtils.h"
#include "FileSystemCommitter.h"
#include "TaskTracker.h"
#include "PartitionCommitTrigger.h"
#include "PartitionCommitPolicy.h"
#include "core/fs/Path.h"

namespace fs = std::filesystem;

int64_t parseDurationMs(const std::string& durationStr);

/**
 * Message sent from upstream writer tasks to the PartitionCommitter.
 * Carries checkpoint coordination info and the list of ready partition paths.
 */
struct PartitionCommitInfo {
    long checkpointId;
    int taskId;
    int numberOfTasks;
    std::vector<std::string> partitions;

    PartitionCommitInfo() : checkpointId(0), taskId(0), numberOfTasks(0)
    {
    }

    PartitionCommitInfo(long checkpointId, int taskId, int numberOfTasks, const std::vector<std::string>& partitions)
        : checkpointId(checkpointId),
          taskId(taskId),
          numberOfTasks(numberOfTasks),
          partitions(partitions)
    {
    }
};

/**
 * Committer operator for partitions. This is the single (non-parallel) task.
 * It collects all partition information sent from upstream, and triggers the
 * partition commit decision when all upstream tasks for a checkpoint have reported.
 *
 * Supports two modes:
 * 1. PartitionCommitInfo mode: Receives structured PartitionCommitInfo messages
 *    from upstream, uses TaskTracker to coordinate multi-task commits.
 * 2. Legacy VectorBatch mode: Extracts partition info from raw records, uses
 *    watermark-based triggering.
 *
 * Processing steps:
 * 1. Partitions are sent from upstream. Add partition to trigger.
 * 2. TaskTracker determines if all upstream tasks have sent data for a checkpoint.
 * 3. Extract committable partitions from PartitionCommitTrigger.
 * 4. Use PartitionCommitPolicy chain to commit partitions.
 */
class PartitionCommitter : public OneInputStreamOperator, public AbstractStreamOperator<void*> {
public:
    PartitionCommitter();
    explicit PartitionCommitter(const nlohmann::json& config);
    ~PartitionCommitter() override;

    void setup();
    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override;
    void open() override;
    void processElement(StreamRecord* element) override;
    void processBatch(StreamRecord* element) override;
    void ProcessWatermark(Watermark* mark) override;
    void notifyCheckpointComplete(long checkpointId) override;

    OperatorSnapshotFutures* SnapshotState(
        long checkpointId,
        long timestamp,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<CheckpointStreamFactory> storageLocation,
        const std::shared_ptr<OmniTaskBridge>& bridge) override;

    void endInput();
    std::string getTypeName() override;

protected:
    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override;

private:
    // ---- config fields ----
    std::string basePath;
    std::vector<std::string> partitionKeys;
    std::vector<int> partitionIndexes;
    int64_t commitDelayMs;
    std::string triggerType;
    std::string policyKind;
    std::string tableName;

    // ---- runtime state ----
    long currentWatermark;
    bool commitsEnabled;
    std::set<std::string> committedPartitions;

    // ---- partition-time trigger mode state (legacy) ----
    std::map<std::string, int64_t> pendingPartitionsLegacy;

    // ---- Flink-style task coordination ----
    std::unique_ptr<PartitionCommitTrigger> trigger_;
    std::unique_ptr<TaskTracker> taskTracker_;
    std::vector<std::unique_ptr<PartitionCommitPolicy>> policies_;

    // ---- helpers ----
    void parseConfig(const nlohmann::json& config);
    std::string extractPartitionPath(omnistream::VectorBatch* batch, int rowId);
    int64_t extractPartitionTime(omnistream::VectorBatch* batch, int rowId);

    void scanExistingPartitions();
    bool hasPartitionFiles(const std::string& dirPath);

    // legacy watermark-based commit
    void checkAndCommitPartitionsLegacy();
    void commitAllPendingPartitionsLegacy();

    // Flink-style commit
    void commitPartitions(long checkpointId);
    void executeCommitPartition(const std::string& partitionPath, const std::vector<std::string>& partitionValues);

    void commitPartition(const std::string& partitionPath);
    void commitAllPendingPartitions();
};

#endif // PARTITION_COMMITTER_H
