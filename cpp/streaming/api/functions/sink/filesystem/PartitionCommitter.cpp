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

#include "PartitionCommitter.h"
#include <cctype>
#include <cstdint>
#include <limits>
#include <stdexcept>

namespace {
int64_t multiplyDurationMs(int64_t value, int64_t factor)
{
    if (factor == 0) {
        return 0;
    }
    if ((value > 0 && value > std::numeric_limits<int64_t>::max() / factor) ||
        (value < 0 && value < std::numeric_limits<int64_t>::min() / factor)) {
        throw std::out_of_range("partition commit delay overflows int64_t milliseconds");
    }
    return value * factor;
}
} // namespace

int64_t parseDurationMs(const std::string& durationStr)
{
    if (durationStr.empty()) return 0;
    size_t pos = 0;
    int64_t value = std::stoll(durationStr, &pos);
    std::string unit;
    while (pos < durationStr.size() && std::isspace(static_cast<unsigned char>(durationStr[pos]))) {
        pos++;
    }
    for (; pos < durationStr.size(); pos++) {
        unit += static_cast<char>(std::tolower(static_cast<unsigned char>(durationStr[pos])));
    }
    if (unit.empty() || unit == "ms") {
        return value;
    } else if (unit == "s" || unit == "sec" || unit == "secs") {
        return multiplyDurationMs(value, 1000);
    } else if (unit == "m" || unit == "min" || unit == "mins") {
        return multiplyDurationMs(value, 60 * 1000);
    } else if (unit == "h" || unit == "hr" || unit == "hrs") {
        return multiplyDurationMs(value, 60 * 60 * 1000);
    } else {
        return value;
    }
}

// ---------------------------------------------------------------------------
// PartitionCommitter
// ---------------------------------------------------------------------------

PartitionCommitter::PartitionCommitter()
{
    currentWatermark = LONG_MIN;
    commitsEnabled = false;
}

PartitionCommitter::PartitionCommitter(const nlohmann::json& config)
{
    currentWatermark = LONG_MIN;
    commitsEnabled = false;
    parseConfig(config);
}

PartitionCommitter::~PartitionCommitter() = default;

void PartitionCommitter::setup()
{
    AbstractStreamOperator<void*>::setup();
}

void PartitionCommitter::initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer)
{
    AbstractStreamOperator<void*>::initializeState(initializer, keySerializer);
    currentWatermark = LONG_MIN;

    // Create the commit trigger based on config
    trigger_.reset(createPartitionCommitTrigger(triggerType, commitDelayMs));

    // Create the policy chain
    policies_ = createPolicyChain(policyKind);

    LOG("PartitionCommitter::initializeState trigger=" << triggerType << " delay=" << commitDelayMs
                                                       << "ms policy=" << policyKind);
}

void PartitionCommitter::open()
{
    LOG("PartitionCommitter::open basePath=" << basePath);

    if (fs::exists(basePath)) {
        for (const auto& entry : fs::directory_iterator(basePath)) {
            if (entry.is_directory()) {
                auto dirPath = entry.path().string();
                if (FileSystemCommitter::hasSuccessFile(dirPath)) {
                    committedPartitions.insert(dirPath);
                }
            }
        }
    }
}

// ---- element processing ----

void PartitionCommitter::processElement(StreamRecord* element)
{
    processBatch(element);
}

void PartitionCommitter::processBatch(StreamRecord* element)
{
    if (!element) return;

    // Try Flink-style: PartitionCommitInfo wrapped in StreamRecord
    auto* info = dynamic_cast<PartitionCommitInfo*>(reinterpret_cast<PartitionCommitInfo*>(element->getValue()));
    if (info && !info->partitions.empty()) {
        // --- Flink-style processing ---
        for (const auto& partition : info->partitions) {
            if (trigger_) {
                trigger_->addPartition(partition);
            }
        }

        if (!taskTracker_) {
            taskTracker_ = std::make_unique<TaskTracker>(info->numberOfTasks);
        }
        bool needCommit = taskTracker_->add(info->checkpointId, info->taskId);
        if (needCommit) {
            commitPartitions(info->checkpointId);
        }
        return;
    }

    // Legacy mode: extract partition info from VectorBatch
    auto batch = reinterpret_cast<omnistream::VectorBatch*>(element->getValue());
    if (!batch) return;

    int rowCount = batch->GetRowCount();
    for (int rowId = 0; rowId < rowCount; rowId++) {
        std::string partitionPath = extractPartitionPath(batch, rowId);
        if (!partitionPath.empty()) {
            auto it = pendingPartitionsLegacy.find(partitionPath);
            if (it == pendingPartitionsLegacy.end()) {
                int64_t partitionTime = extractPartitionTime(batch, rowId);
                if (partitionTime > 0) {
                    pendingPartitionsLegacy[partitionPath] = partitionTime;
                    LOG("PartitionCommitter: new partition " << partitionPath << " partitionTime=" << partitionTime);
                }
            }
        }
    }
}

// ---- watermark ----

void PartitionCommitter::ProcessWatermark(Watermark* mark)
{
    if (!mark) return;

    long newWatermark = mark->getTimestamp();
    if (newWatermark > currentWatermark) {
        currentWatermark = newWatermark;
        checkAndCommitPartitionsLegacy();
    }

    if (this->output) {
        this->output->emitWatermark(mark);
    }
}

// ---- checkpoint ----

void PartitionCommitter::notifyCheckpointComplete(long checkpointId)
{
    AbstractStreamOperator<void*>::notifyCheckpointComplete(checkpointId);
    commitAllPendingPartitions();
}

OperatorSnapshotFutures* PartitionCommitter::SnapshotState(
    long checkpointId,
    long timestamp,
    CheckpointOptions* checkpointOptions,
    std::shared_ptr<CheckpointStreamFactory> storageLocation,
    const std::shared_ptr<OmniTaskBridge>& bridge)
{
    if (trigger_) {
        trigger_->snapshotState(checkpointId, currentWatermark);
    }
    return AbstractStreamOperator<void*>::SnapshotState(
        checkpointId, timestamp, checkpointOptions, storageLocation, bridge);
}

// ---- end input ----

void PartitionCommitter::endInput()
{
    commitAllPendingPartitions();
}

std::string PartitionCommitter::getTypeName()
{
    return "PartitionCommitter";
}

void PartitionCommitter::processWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    AbstractStreamOperator<void*>::processWatermarkStatus(watermarkStatus);
}

// ---- config ----

void PartitionCommitter::parseConfig(const nlohmann::json& config)
{
    if (config.contains("path")) {
        basePath = config["path"].get<std::string>();
    }
    if (config.contains("partitionKeys")) {
        partitionKeys = config["partitionKeys"].get<std::vector<std::string>>();
    }
    if (config.contains("partitionIndexes")) {
        partitionIndexes = config["partitionIndexes"].get<std::vector<int>>();
    }
    if (config.contains("table-name")) {
        tableName = config["table-name"].get<std::string>();
    }

    commitDelayMs = 0;
    triggerType = "partition-time";
    policyKind = "success-file";

    if (config.contains("partition-commit")) {
        const auto& commitConfig = config["partition-commit"];
        if (commitConfig.contains("delay")) {
            commitDelayMs = parseDurationMs(commitConfig["delay"].get<std::string>());
        }
        if (commitConfig.contains("trigger")) {
            triggerType = commitConfig["trigger"].get<std::string>();
        }
        if (commitConfig.contains("policy") && commitConfig["policy"].contains("kind")) {
            policyKind = commitConfig["policy"]["kind"].get<std::string>();
        }
    }

    commitsEnabled = true;

    LOG("PartitionCommitter::config basePath=" << basePath << " delay=" << commitDelayMs << "ms"
                                               << " trigger=" << triggerType << " policy=" << policyKind);
}

// ---- partition path extraction ----

std::string PartitionCommitter::extractPartitionPath(omnistream::VectorBatch* batch, int rowId)
{
    if (partitionKeys.empty() || partitionIndexes.empty()) return "";

    std::vector<std::pair<std::string, std::string>> partitionSpec;
    for (size_t i = 0; i < partitionKeys.size() && i < partitionIndexes.size(); ++i) {
        auto val = VectorBatchUtil::getValueAtAsStr(batch, partitionIndexes[i], rowId);
        partitionSpec.emplace_back(partitionKeys[i], std::string(val));
    }
    auto partitionSubPath = PartitionPathUtils::generatePartitionPath(partitionSpec);
    return basePath + "/" + partitionSubPath;
}

int64_t PartitionCommitter::extractPartitionTime(omnistream::VectorBatch* /*batch*/, int /*rowId*/)
{
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
}

// ---- existing partitions scan ----

void PartitionCommitter::scanExistingPartitions()
{
    if (!fs::exists(basePath)) return;

    for (const auto& entry : fs::directory_iterator(basePath)) {
        if (entry.is_directory()) {
            auto dirPath = entry.path().string();
            if (FileSystemCommitter::hasSuccessFile(dirPath)) {
                committedPartitions.insert(dirPath);
            } else {
                if (hasPartitionFiles(dirPath)) {
                    int64_t modTime = FileSystemCommitter::getFileModificationTime(dirPath);
                    pendingPartitionsLegacy[dirPath] = modTime;
                }
            }
        }
    }
}

bool PartitionCommitter::hasPartitionFiles(const std::string& dirPath)
{
    for (const auto& entry : fs::directory_iterator(dirPath)) {
        if (entry.is_regular_file() && entry.path().filename() != "_SUCCESS") {
            return true;
        }
    }
    return false;
}

// ---- Flink-style commits via Trigger + Policy ----

void PartitionCommitter::commitPartitions(long checkpointId)
{
    std::vector<std::string> partitions;
    if (trigger_) {
        partitions = (checkpointId == LONG_MAX) ? trigger_->endInput() : trigger_->committablePartitions(checkpointId);
    }

    for (const auto& partition : partitions) {
        // Extract partition values from path for the policy context
        std::vector<std::string> partitionValues;
        {
            fs::path p(partition);
            std::string filename = p.filename().string();
            if (!filename.empty()) {
                // Parse key=value from directory name
                size_t eqPos = filename.find('=');
                if (eqPos != std::string::npos) {
                    partitionValues.push_back(filename.substr(eqPos + 1));
                }
            }
        }
        executeCommitPartition(partition, partitionValues);
    }
}

void PartitionCommitter::executeCommitPartition(
    const std::string& partitionPath, const std::vector<std::string>& partitionValues)
{
    if (committedPartitions.find(partitionPath) != committedPartitions.end()) {
        return;
    }

    LOG("PartitionCommitter: committing partition " << partitionPath);

    PolicyContext ctx("default", "default", tableName, partitionKeys, partitionValues, partitionPath);

    for (auto& policy : policies_) {
        policy->commit(ctx);
    }

    committedPartitions.insert(partitionPath);
}

// ---- Legacy commit (watermark-driven) ----

void PartitionCommitter::checkAndCommitPartitionsLegacy()
{
    if (!commitsEnabled) return;

    std::vector<std::string> toCommit;
    for (auto it = pendingPartitionsLegacy.begin(); it != pendingPartitionsLegacy.end();) {
        int64_t partitionTime = it->second;
        int64_t readyTime = partitionTime + commitDelayMs;

        bool shouldCommit = false;
        if (triggerType == "partition-time") {
            shouldCommit = (currentWatermark > readyTime);
        } else if (triggerType == "processing-time") {
            auto now = std::chrono::system_clock::now();
            auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            shouldCommit = (nowMs > readyTime);
        }

        if (shouldCommit && currentWatermark != LONG_MAX) {
            toCommit.push_back(it->first);
            it = pendingPartitionsLegacy.erase(it);
        } else {
            ++it;
        }
    }

    for (const auto& partitionPath : toCommit) {
        commitPartition(partitionPath);
    }

    if (currentWatermark == LONG_MAX) {
        commitAllPendingPartitionsLegacy();
    }
}

void PartitionCommitter::commitPartition(const std::string& partitionPath)
{
    if (committedPartitions.find(partitionPath) != committedPartitions.end()) {
        return;
    }

    LOG("PartitionCommitter: committing partition " << partitionPath);

    // Execute policies for legacy mode
    PolicyContext ctx("default", "default", tableName, partitionKeys, {}, partitionPath);

    for (auto& policy : policies_) {
        policy->commit(ctx);
    }

    // Legacy: always create success file in addition to policy chain
    if (policyKind == "success-file") {
        FileSystemCommitter::createSuccessFile(partitionPath);
    }

    committedPartitions.insert(partitionPath);
}

void PartitionCommitter::commitAllPendingPartitions()
{
    // Flink-style: use trigger endInput
    if (trigger_) {
        auto partitions = trigger_->endInput();
        for (const auto& partition : partitions) {
            executeCommitPartition(partition, {});
        }
    }

    // Legacy pending partitions
    commitAllPendingPartitionsLegacy();
}

void PartitionCommitter::commitAllPendingPartitionsLegacy()
{
    for (const auto& pair : pendingPartitionsLegacy) {
        commitPartition(pair.first);
    }
    pendingPartitionsLegacy.clear();
}
