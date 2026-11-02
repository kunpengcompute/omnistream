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

#ifndef TASKINFORMATIONPOD_H
#define TASKINFORMATIONPOD_H

#include <string>
#include <filesystem>
#include <cstdint>
#include <stdexcept>
#include <nlohmann/json.hpp>
#include "StreamConfigPOD.h"
#include "CheckpointConfigPOD.h"
#include "ExecutionCheckpointConfigPOD.h"
#include "operatorchain/OperatorChainPOD.h"
#include "state/RocksDBMemoryConfiguration.h"

namespace omnistream {

class TaskInformationPOD {
public:
    static constexpr uint64_t DEFAULT_ROCKSDB_MANAGED_MEMORY_SIZE = 64ULL * 1024ULL * 1024ULL;

    TaskInformationPOD()
        : taskName(""),
          numberOfSubtasks(1),
          maxNumberOfSubtasks(1),
          indexOfSubtask(0),
          streamConfig(StreamConfigPOD()),
          chainedConfig(),
          stateBackend(""),
          rocksdbStorePaths(),
          stateBackendManagedMemoryFraction(0.0),
          stateBackendManagedMemorySize(DEFAULT_ROCKSDB_MANAGED_MEMORY_SIZE),
          numberOfTransferThreads(4),
          rocksDBMemoryConfiguration(),
          taskType(1),
          checkpointConfig(CheckpointConfigPOD()),
          executionCheckpointConfig(ExecutionCheckpointConfigPOD()),
          localRecoveryConfig(""),
          tmpWorkingDirectory("/tmp")
    {
    }

    TaskInformationPOD(
        const std::string& taskName,
        int numberOfSubtasks,
        int maxNumberOfSubtasks,
        int indexOfSubtask,
        const StreamConfigPOD& streamConfigPOD,
        const OperatorChainPOD& operatorChain,
        const std::vector<StreamConfigPOD>& chainedConfig_,
        const int taskType = 1)
        : taskName(taskName),
          numberOfSubtasks(numberOfSubtasks),
          maxNumberOfSubtasks(maxNumberOfSubtasks),
          indexOfSubtask(indexOfSubtask),
          streamConfig(streamConfigPOD),
          chainedConfig(chainedConfig_),
          stateBackend(""),
          rocksdbStorePaths(),
          stateBackendManagedMemoryFraction(0.0),
          stateBackendManagedMemorySize(DEFAULT_ROCKSDB_MANAGED_MEMORY_SIZE),
          numberOfTransferThreads(4),
          rocksDBMemoryConfiguration(),
          taskType(taskType),
          checkpointConfig(CheckpointConfigPOD()),
          executionCheckpointConfig(ExecutionCheckpointConfigPOD()),
          localRecoveryConfig(""),
          tmpWorkingDirectory("/tmp")
    {
    }

    TaskInformationPOD(
        const std::string& taskName,
        int numberOfSubtasks,
        int maxNumberOfSubtasks,
        int indexOfSubtask,
        const StreamConfigPOD& streamConfigPOD,
        const OperatorChainPOD& operatorChain,
        const std::vector<StreamConfigPOD>& chainedConfig_,
        std::string& stateBackend,
        std::vector<std::string> rocksdbStorePaths)
        : taskName(taskName),
          numberOfSubtasks(numberOfSubtasks),
          maxNumberOfSubtasks(maxNumberOfSubtasks),
          indexOfSubtask(indexOfSubtask),
          streamConfig(streamConfigPOD),
          chainedConfig(chainedConfig_),
          stateBackend(stateBackend),
          rocksdbStorePaths(rocksdbStorePaths),
          stateBackendManagedMemoryFraction(0.0),
          stateBackendManagedMemorySize(DEFAULT_ROCKSDB_MANAGED_MEMORY_SIZE),
          numberOfTransferThreads(4),
          rocksDBMemoryConfiguration(),
          taskType(1),
          checkpointConfig(CheckpointConfigPOD()),
          executionCheckpointConfig(ExecutionCheckpointConfigPOD()),
          localRecoveryConfig(""),
          tmpWorkingDirectory("/tmp")
    {
    }

    TaskInformationPOD& operator=(const TaskInformationPOD&) = default;
    TaskInformationPOD(const TaskInformationPOD& other) = default;

    std::string getTaskName() const
    {
        return taskName;
    }

    void setTaskName(const std::string& taskName_)
    {
        this->taskName = taskName_;
    }

    int getNumberOfSubtasks() const
    {
        return numberOfSubtasks;
    }

    void setNumberOfSubtasks(int numberOfSubtasks_)
    {
        this->numberOfSubtasks = numberOfSubtasks_;
    }

    int getIndexOfSubtask() const
    {
        return indexOfSubtask;
    }

    void setIndexOfSubtask(int indexOfSubtask_)
    {
        this->indexOfSubtask = indexOfSubtask_;
    }

    int getMaxNumberOfSubtasks() const
    {
        return maxNumberOfSubtasks;
    }

    void setMaxNumberOfSubtasks(int maxNumberOfSubtasks_)
    {
        this->maxNumberOfSubtasks = maxNumberOfSubtasks_;
    }

    const StreamConfigPOD& getStreamConfigPOD() const
    {
        return streamConfig;
    }

    void setStreamConfigPOD(const StreamConfigPOD& streamConfigPOD)
    {
        this->streamConfig = streamConfigPOD;
    }

    [[nodiscard]] std::vector<StreamConfigPOD> getChainedConfig() const
    {
        return chainedConfig;
    }

    [[nodiscard]] std::unordered_map<int, StreamConfigPOD> getChainedConfigMap()
    {
        if (chainedConfigMap.empty()) {
            for (auto streamConfig : chainedConfig) {
                chainedConfigMap[streamConfig.getOperatorDescription().getVertexID()] = streamConfig;
            }
        }
        return chainedConfigMap;
    }

    void setChainedConfig(const std::vector<StreamConfigPOD>& chained_config)
    {
        chainedConfig = chained_config;
    }

    const std::string& getStateBackend() const
    {
        return stateBackend;
    }

    void setStateBackend(const std::string& stateBackend_)
    {
        stateBackend = stateBackend_;
    }

    const std::vector<std::string>& getRocksdbStorePaths() const
    {
        return rocksdbStorePaths;
    }

    void setRocksdbStorePaths(const std::vector<std::string>& rocksdbStorePaths_)
    {
        rocksdbStorePaths = rocksdbStorePaths_;
    }

    double getStateBackendManagedMemoryFraction() const
    {
        return stateBackendManagedMemoryFraction;
    }

    uint64_t getStateBackendManagedMemorySize() const
    {
        return stateBackendManagedMemorySize;
    }

    uint32_t getNumberOfTransferThreads() const
    {
        return numberOfTransferThreads;
    }

    const RocksDBMemoryConfiguration& getRocksDBMemoryConfiguration() const
    {
        return rocksDBMemoryConfiguration;
    }

    std::string getPriorityQueueStateType() const
    {
        return priorityQueueStateType;
    }

    std::string toString() const
    {
        std::stringstream ss;
        ss << "{";

        bool first = true;
        for (const auto& streamConfig : chainedConfig) {
            if (!first) {
                ss << ", ";
            }
            ss << streamConfig.getOperatorDescription().getVertexID() << ": " << streamConfig.toString();
            first = false;
        }

        ss << "}";

        return "TaskInformationPOD{ taskName='" + taskName + '\'' +
               ", numberOfSubtasks=" + std::to_string(numberOfSubtasks) +
               ", maxNumberOfSubtasks=" + std::to_string(maxNumberOfSubtasks) +
               ", indexOfSubtask=" + std::to_string(indexOfSubtask) + ", streamConfigPOD=" + streamConfig.toString() +
               ", stateBackend=" + stateBackend + ", chainedConfig= " + ss.str() + '}';
    }

    bool operator==(const TaskInformationPOD& other) const
    {
        return taskName == other.taskName && numberOfSubtasks == other.numberOfSubtasks &&
               maxNumberOfSubtasks == other.maxNumberOfSubtasks && indexOfSubtask == other.indexOfSubtask &&
               streamConfig == other.streamConfig && chainedConfig == other.chainedConfig &&
               stateBackend == other.stateBackend && rocksdbStorePaths == other.rocksdbStorePaths &&
               upstreamNative == other.upstreamNative;
    }

    bool GetSplitWatermark() const
    {
        return splitWatermark;
    }

    void SetSplitWatermark(const bool& splitWatermark)
    {
        this->splitWatermark = splitWatermark;
    }

    int GetTaskType() const
    {
        return taskType;
    }

    void SetTaskType(const int& taskType)
    {
        this->taskType = taskType;
    }

    const CheckpointConfigPOD& getCheckpointConfig() const
    {
        return checkpointConfig;
    }

    void setCheckpointConfig(const CheckpointConfigPOD& cfg)
    {
        checkpointConfig = cfg;
    }

    const ExecutionCheckpointConfigPOD& getExecutionCheckpointConfig() const
    {
        return executionCheckpointConfig;
    }

    void setExecutionCheckpointConfig(const ExecutionCheckpointConfigPOD& cfg)
    {
        executionCheckpointConfig = cfg;
    }

    const std::string& getLocalRecoveryConfig() const
    {
        return localRecoveryConfig;
    }

    std::filesystem::path getTmpWorkingDirectory() const
    {
        return tmpWorkingDirectory;
    }

    const std::vector<bool>& getUpstreamNative() const
    {
        return upstreamNative;
    }

    void validateInputGateCount(size_t gateCount) const
    {
        if (upstreamNative.size() != gateCount) {
            throw std::invalid_argument("upstreamNative count does not match deployed input gates");
        }
    }

    friend void to_json(nlohmann::json& json, const TaskInformationPOD& taskInfo)
    {
        json = nlohmann::json{
            {"taskName", taskInfo.taskName},
            {"numberOfSubtasks", taskInfo.numberOfSubtasks},
            {"maxNumberOfSubtasks", taskInfo.maxNumberOfSubtasks},
            {"indexOfSubtask", taskInfo.indexOfSubtask},
            {"stateBackend", taskInfo.stateBackend},
            {"rocksdbStorePaths", taskInfo.rocksdbStorePaths},
            {"stateBackendManagedMemoryFraction", taskInfo.stateBackendManagedMemoryFraction},
            {"stateBackendManagedMemorySize", taskInfo.stateBackendManagedMemorySize},
            {"priorityQueueStateType", taskInfo.priorityQueueStateType},
            {"numberOfTransferThreads", taskInfo.numberOfTransferThreads},
            {"rocksDBMemoryConfiguration", taskInfo.rocksDBMemoryConfiguration},
            {"streamConfig", taskInfo.streamConfig},
            {"chainedConfig", taskInfo.chainedConfig},
            {"taskType", taskInfo.taskType},
            {"upstreamNative", nlohmann::json(taskInfo.upstreamNative).dump()},
            {"checkpointConfig", taskInfo.checkpointConfig},
            {"executionCheckpointConfig", taskInfo.executionCheckpointConfig},
            {"localRecoveryConfig", taskInfo.localRecoveryConfig},
            {"tmpWorkingDirectory", taskInfo.tmpWorkingDirectory},
            {"splitWatermark", taskInfo.splitWatermark}};
    }

    friend void from_json(const nlohmann::json& json, TaskInformationPOD& taskInfo)
    {
        taskInfo.taskName = json.value("taskName", "");
        taskInfo.numberOfSubtasks = json.value("numberOfSubtasks", 1);
        taskInfo.maxNumberOfSubtasks = json.value("maxNumberOfSubtasks", 1);
        taskInfo.indexOfSubtask = json.value("indexOfSubtask", 0);
        taskInfo.stateBackend = json.value("stateBackend", "");
        taskInfo.rocksdbStorePaths = json.value("rocksdbStorePaths", std::vector<std::string>());
        taskInfo.stateBackendManagedMemoryFraction = json.value("stateBackendManagedMemoryFraction", 0.0);
        taskInfo.stateBackendManagedMemorySize =
            json.value("stateBackendManagedMemorySize", TaskInformationPOD::DEFAULT_ROCKSDB_MANAGED_MEMORY_SIZE);
        taskInfo.priorityQueueStateType = json.value("priorityQueueStateType", "HEAP");
        taskInfo.numberOfTransferThreads = json.value("numberOfTransferThreads", static_cast<uint32_t>(4));
        taskInfo.rocksDBMemoryConfiguration =
            json.contains("rocksDBMemoryConfiguration") && !json.at("rocksDBMemoryConfiguration").is_null()
                ? json.at("rocksDBMemoryConfiguration").get<RocksDBMemoryConfiguration>()
                : RocksDBMemoryConfiguration();
        taskInfo.streamConfig = json.contains("streamConfig") && !json.at("streamConfig").is_null()
                                    ? json.at("streamConfig").get<StreamConfigPOD>()
                                    : StreamConfigPOD();
        taskInfo.chainedConfig = json.value("chainedConfig", std::vector<StreamConfigPOD>());
        taskInfo.taskType = json.value("taskType", 1);
        const auto upstream = nlohmann::json::parse(json.value("upstreamNative", std::string("[]")));
        if (!upstream.is_array()) {
            throw std::invalid_argument("upstreamNative must encode a boolean array");
        }
        taskInfo.upstreamNative.clear();
        for (const auto& flag : upstream) {
            if (!flag.is_boolean()) {
                throw std::invalid_argument("upstreamNative entries must be booleans");
            }
            taskInfo.upstreamNative.push_back(flag.get<bool>());
        }
        taskInfo.checkpointConfig = json.contains("checkpointConfig") && !json.at("checkpointConfig").is_null()
                                        ? json.at("checkpointConfig").get<CheckpointConfigPOD>()
                                        : CheckpointConfigPOD();
        taskInfo.executionCheckpointConfig =
            json.contains("executionCheckpointConfig") && !json.at("executionCheckpointConfig").is_null()
                ? json.at("executionCheckpointConfig").get<ExecutionCheckpointConfigPOD>()
                : ExecutionCheckpointConfigPOD();
        taskInfo.localRecoveryConfig = json.value("localRecoveryConfig", "");
        taskInfo.tmpWorkingDirectory = json.value("tmpWorkingDirectory", std::string("/tmp"));
        taskInfo.splitWatermark = json.value("splitWatermark", false);
    }

private:
    std::vector<bool> upstreamNative;
    std::string taskName;
    int numberOfSubtasks;
    int maxNumberOfSubtasks;
    int indexOfSubtask;
    StreamConfigPOD streamConfig;
    std::vector<StreamConfigPOD> chainedConfig;

    std::string stateBackend;

    // rocksdb related config
    std::vector<std::string> rocksdbStorePaths;
    double stateBackendManagedMemoryFraction;
    uint64_t stateBackendManagedMemorySize;
    uint32_t numberOfTransferThreads;
    RocksDBMemoryConfiguration rocksDBMemoryConfiguration;
    std::string priorityQueueStateType;

    std::unordered_map<int, StreamConfigPOD> chainedConfigMap;
    int taskType;
    CheckpointConfigPOD checkpointConfig;
    ExecutionCheckpointConfigPOD executionCheckpointConfig;
    std::string localRecoveryConfig;
    std::string tmpWorkingDirectory;
    bool splitWatermark;
};

} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::TaskInformationPOD> {
    size_t operator()(const omnistream::TaskInformationPOD& obj) const
    {
        size_t h1 = std::hash<std::string>()(obj.getTaskName());
        size_t h2 = std::hash<int>()(obj.getNumberOfSubtasks());
        size_t h3 = std::hash<int>()(obj.getMaxNumberOfSubtasks());
        size_t h4 = std::hash<int>()(obj.getIndexOfSubtask());
        size_t h5 = std::hash<omnistream::StreamConfigPOD>()(obj.getStreamConfigPOD());

        // 对 vector<StreamConfigPOD> 做 hash，逐个元素合并
        size_t h6 = 0;
        for (const auto& cfg : obj.getChainedConfig()) {
            size_t h = std::hash<omnistream::StreamConfigPOD>()(cfg);
            h6 ^= h + 0x9e3779b9 + (h6 << 6) + (h6 >> 2);
        }

        size_t seed = 0;
        seed ^= h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h4 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h5 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= h6 + 0x9e3779b9 + (seed << 6) + (seed >> 2);

        return seed;
    }
};
} // namespace std

#endif // TASKINFORMATIONPOD_H
