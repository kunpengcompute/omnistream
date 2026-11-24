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

#pragma once

#include <nlohmann/json.hpp>
#include "UUID.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/state/StateBackend.h"
#include "runtime/execution/OmniEnvironment.h"
#include "RocksDBKeyedStateBackendBuilder.h"
#include "RocksDBMemoryControllerUtils.h"
#include "DefaultOperatorStateBackendBuilder.h"

using json = nlohmann::json;

enum class TernaryBoolean {
    FALSE,
    TRUE,
    UNDEFINED
};

class EmbeddedRocksDBStateBackend : public StateBackend {
public:
    explicit EmbeddedRocksDBStateBackend(TernaryBoolean boolean) {};
    ~EmbeddedRocksDBStateBackend() {};

    fs::path getNextStoragePath()
    {
        size_t ni = nextDirectory + 1;
        ni = ni >= initializedDbBasePaths.size() ? 0 : ni;
        nextDirectory = ni;

        return initializedDbBasePaths[ni];
    }

    template <typename K>
    AbstractKeyedStateBackend<K>* createKeyedStateBackend(
        omnistream::EnvironmentV2* env,
        std::string operatorIdentifier,
        std::set<std::shared_ptr<KeyedStateHandle>> stateHandles,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        int numberOfKeyGroups,
        int alternativeIdx,
        const FlinkSavepointAdaptorInfo& adaptorInfo = FlinkSavepointAdaptorInfo{},
        RestoreSavepointMode restoreMode = RestoreSavepointMode::OMNI_INTERNAL,
        const nlohmann::json& operatorDescription = nlohmann::json{})
    {
        std::string tempDir = env->taskConfiguration().getTmpWorkingDirectory().string();

        std::string fileCompatibleIdentifier = operatorIdentifier;
        std::replace_if(
            fileCompatibleIdentifier.begin(),
            fileCompatibleIdentifier.end(),
            [](char c) { return !std::isalnum(c) && c != '-'; },
            '_');

        lazyInitializeForJob(env, fileCompatibleIdentifier);

        fs::path instanceBasePath =
            getNextStoragePath() / ("job_" + jobID.AbstractIDPOD::toString() + "_op_" + fileCompatibleIdentifier +
                                    "_uuid_" + UUID::randomUUID().ToString());

        auto localRecoveryConfig = env->getTaskStateManager()->createLocalRecoveryConfig();

        auto bridge = env->getTaskStateManager()->getTaskStateManagerBridge();

        auto omniTaskBridge = env->getTaskStateManager()->getOmniTaskBridge();

        auto operatorIdStr = env->taskConfiguration().getStreamConfigPOD().getOperatorDescription().getOperatorId();
        std::string upperHex = operatorIdStr.substr(0, 16);
        std::string lowerHex = operatorIdStr.substr(16);
        uint64_t uLower = std::stoull(lowerHex, nullptr, 16);
        uint64_t uUpper = std::stoull(upperHex, nullptr, 16);
        auto operatorId = std::make_shared<OperatorID>(uUpper, uLower);

        auto sharedResources = env->getRocksDBSharedResources();
        if (!sharedResources) {
            throw std::runtime_error("Omni RocksDB requires a slot-level shared-resource lease");
        }

        auto resourceContainer = std::make_shared<RocksDBResourceContainer>(sharedResources, instanceBasePath, false);

        std::vector<std::shared_ptr<KeyedStateHandle>> stateVec(stateHandles.begin(), stateHandles.end());

        auto priorityQueueStateType = env->taskConfiguration().getPriorityQueueStateType() == "ROCKSDB"
                                          ? RocksDBKeyedStateBackendBuilder<K>::PriorityQueueStateType::ROCKSDB
                                          : RocksDBKeyedStateBackendBuilder<K>::PriorityQueueStateType::HEAP;

        RocksDBKeyedStateBackendBuilder<K> builder(
            operatorIdentifier,
            instanceBasePath,
            resourceContainer,
            keySerializer,
            numberOfKeyGroups,
            keyGroupRange,
            localRecoveryConfig,
            priorityQueueStateType,
            stateVec,
            bridge,
            omniTaskBridge,
            operatorId,
            alternativeIdx);

        bool incrementalCheckpointing = enableIncrementalCheckpointing == TernaryBoolean::TRUE ? true : false;
        int taskType = env->taskConfiguration().GetTaskType();
        builder.setEnableIncrementalCheckpointing(incrementalCheckpointing)
            .setNumberOfTransferringThreads(numberOfTransferThreads)
            .setWriteBatchSize(writeBatchSize)
            .setFlinkSavepointAdaptorInfo(adaptorInfo)
            .setRestoreSavepointMode(restoreMode)
            .setTaskType(taskType)
            .setOperatorDescription(operatorDescription);

        return builder.build();
    }

    OperatorStateBackend* createOperatorStateBackend(
        omnistream::EnvironmentV2* env,
        std::string operatorIdentifier,
        std::set<std::shared_ptr<OperatorStateHandle>> stateHandles)
    {
        std::vector<std::shared_ptr<OperatorStateHandle>> stateVector(stateHandles.begin(), stateHandles.end());
        auto bridge = env->getTaskStateManager()->getTaskStateManagerBridge();
        auto omniTaskBridge = env->getTaskStateManager()->getOmniTaskBridge();

        const bool asynchronousSnapshots = true;
        DefaultOperatorStateBackendBuilder builder(
            asynchronousSnapshots, operatorIdentifier, stateVector, bridge, omniTaskBridge);

        return builder.build();
    }

    explicit EmbeddedRocksDBStateBackend(TaskInformationPOD taskConfiguration)
        : enableIncrementalCheckpointing(TernaryBoolean::UNDEFINED),
          numberOfTransferThreads(UNDEFINED_NUMBER_OF_TRANSFER_THREADS),
          nextDirectory(0),
          isInitialized(false),
          writeBatchSize(UNDEFINED_WRITE_BATCH_SIZE),
          overlapFractionThreshold(UNDEFINED_OVERLAP_FRACTION_THRESHOLD)
    {
        enableIncrementalCheckpointing = taskConfiguration.getCheckpointConfig().getIncrementalCheckpoints()
                                             ? TernaryBoolean::TRUE
                                             : TernaryBoolean::FALSE;
        configureStoragePaths(taskConfiguration.getRocksdbStorePaths());
        configureOtherParameters(taskConfiguration);
    }

    void handleDirectories()
    {
        std::vector<fs::path> validDirs;
        std::string errorMessage;

        for (const auto& dir : localRocksDbDirectories) {
            fs::path testDir = dir / UUID::randomUUID().ToString();

            try {
                if (!fs::create_directories(testDir)) {
                    std::string msg =
                        "Local DB files directory '" + dir.string() + "' does not exist and cannot be created.";
                    errorMessage += msg + "\n";
                } else {
                    validDirs.push_back(dir);
                }

                fs::remove_all(testDir);
            } catch (const fs::filesystem_error& e) {
                std::string msg = "Failed to create test directory in '" + dir.string() + "': " + e.what();
                errorMessage += msg + "\n";
            }
        }

        if (validDirs.empty()) {
            throw std::runtime_error("No valid local storage directories available. " + errorMessage);
        } else {
            initializedDbBasePaths = std::move(validDirs);
        }
    }

    void lazyInitializeForJob(omnistream::EnvironmentV2* env, const std::string& operatorIdentifier)
    {
        if (isInitialized) {
            return;
        }

        jobID = env->getTaskStateManager()->getJobId();

        if (localRocksDbDirectories.empty()) {
            initializedDbBasePaths = {env->taskConfiguration().getTmpWorkingDirectory()};
        } else {
            handleDirectories();
        }

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dist(0, initializedDbBasePaths.size() - 1);
        nextDirectory = dist(gen);

        isInitialized = true;
    }

private:
    void setDbStoragePaths(const std::vector<std::string>& paths)
    {
        if (paths.empty()) {
            return;
        }

        std::vector<fs::path> pp;
        pp.reserve(paths.size());
        for (const auto& rawPath : paths) {
            if (rawPath.empty()) {
                throw std::invalid_argument("null path");
            }
            std::string processedPath = rawPath;

            const std::string filePrefix = "file://";
            if (rawPath.find(filePrefix) == 0) {
                processedPath = rawPath.substr(filePrefix.length());
                while (!processedPath.empty() && processedPath[0] == '/' && processedPath[1] == '/') {
                    processedPath = processedPath.substr(1);
                }

                if (!processedPath.empty() && processedPath[0] != '/') {
                    processedPath.insert(0, 1, '/');
                }

                if (processedPath.empty()) {
                    throw std::invalid_argument("Invalid file URI: " + rawPath);
                }
            } else if (rawPath.find("://") != std::string::npos) {
                throw std::invalid_argument("Path " + rawPath + " has a non-local scheme");
            }

            fs::path pathObj(processedPath);
            if (!pathObj.is_absolute()) {
                INFO_RELEASE("Error: Relative paths are not supported: " << processedPath);
                throw std::invalid_argument("Relative paths are not supported: " + processedPath);
            }
            pp.emplace_back(std::move(pathObj));
        }
        localRocksDbDirectories = std::move(pp);
    }

    void configureStoragePaths(std::vector<std::string> rocksDbDirectories)
    {
        setDbStoragePaths(rocksDbDirectories);
    }

    void configureOtherParameters(TaskInformationPOD taskConfiguration)
    {
        numberOfTransferThreads = taskConfiguration.getNumberOfTransferThreads();
        if (numberOfTransferThreads <= 0) {
            INFO_RELEASE("Error: Invalid number of transfer threads");
            THROW_LOGIC_EXCEPTION("Invalid number of transfer threads");
        }
        writeBatchSize = 2 * 1024 * 1024;
        overlapFractionThreshold = 0.0;
    }

    static const long serialVersionUID = 1L;
    static constexpr int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;
    static std::atomic<bool> rocksDbInitialized;
    static constexpr int UNDEFINED_NUMBER_OF_TRANSFER_THREADS = -1;
    static constexpr long UNDEFINED_WRITE_BATCH_SIZE = -1;
    static constexpr double UNDEFINED_OVERLAP_FRACTION_THRESHOLD = -1;

    std::vector<fs::path> localRocksDbDirectories;
    TernaryBoolean enableIncrementalCheckpointing;
    int numberOfTransferThreads;
    std::vector<fs::path> initializedDbBasePaths;
    JobIDPOD jobID;
    int nextDirectory;
    bool isInitialized;
    long writeBatchSize;
    double overlapFractionThreshold;

    std::once_flag rocksdb_init_flag_;
    bool rocksDbInitialized_ = false;
};
