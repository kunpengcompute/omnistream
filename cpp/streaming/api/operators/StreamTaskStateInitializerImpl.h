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
#include "StreamOperatorStateContext.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/RocksdbKeyedStateBackend.h"
#include "table/runtime/operators/InternalTimeServiceManager.h"
#include "KeyContext.h"
#include "runtime/state/OperatorStateBackend.h"
#include "BackendRestorerProcedure.h"
#include "../../../core/include/common.h"
#include "streaming/runtime/metrics/MetricGroup.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "runtime/state/hashmap/HashMapStateBackend.h"
#ifdef WITH_OMNISTATESTORE
#include "runtime/state/BssKeyedStateBackend.h"
#endif
#include "runtime/state/RocksDBStateBackend.h"
#include "runtime/state/UUID.h"
#include "runtime/checkpoint/StateObjectCollection.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"

// We want to ultimately return this
template <typename K>
class StreamOperatorStateContextImpl {
public:
    StreamOperatorStateContextImpl(
        std::optional<uint64_t> restoredCheckpointId_,
        CheckpointableKeyedStateBackend<K>* backend_,
        OperatorStateBackend* osBackend_,
        InternalTimeServiceManager<K>* internalTimeServiceManager_)
        : restoredCheckpointId(restoredCheckpointId_),
          backend(backend_),
          osBackend(osBackend_),
          internalTimeServiceManager(internalTimeServiceManager_)
    {
    }

    ~StreamOperatorStateContextImpl()
    {
        if (backend != nullptr) {
            delete backend;
            backend = nullptr;
        }

        if (osBackend != nullptr) {
            delete osBackend;
            osBackend = nullptr;
        }

        if (internalTimeServiceManager != nullptr) {
            delete internalTimeServiceManager;
            internalTimeServiceManager = nullptr;
        }
    }

    CheckpointableKeyedStateBackend<K>* keyedStateBackend()
    {
        return backend;
    }

    OperatorStateBackend* operatorStateBackend()
    {
        return osBackend;
    }

    InternalTimeServiceManager<K>* getInternalTimeServiceManager()
    {
        return internalTimeServiceManager;
    }

    std::optional<uint64_t> getRestoredCheckpointId() const
    {
        return restoredCheckpointId;
    }

private:
    std::optional<uint64_t> restoredCheckpointId;
    CheckpointableKeyedStateBackend<K>* backend = nullptr;
    OperatorStateBackend* osBackend = nullptr;
    InternalTimeServiceManager<K>* internalTimeServiceManager = nullptr;
};

class StreamTaskStateInitializerImpl {
public:
    omnistream::EnvironmentV2* getEnvironment() const
    {
        return env;
    }

    explicit StreamTaskStateInitializerImpl(omnistream::EnvironmentV2* env)
        : stateBackend(
              env && env->taskConfiguration().getStateBackend() == "HashMapStateBackend" ? (new HashMapStateBackend())
                                                                                         : nullptr),
          env(env)
    {
    }

    explicit StreamTaskStateInitializerImpl(StateBackend* stateBackend, omnistream::EnvironmentV2* env)
        : stateBackend(stateBackend),
          env(env) {};

    template <typename K>
    StreamOperatorStateContextImpl<K>* streamOperatorStateContext(
        TypeSerializer* keySerializer,
        KeyContext<K>* keyContext,
        ProcessingTimeService* processingTimeService,
        OperatorID* operatorID = nullptr,
        const FlinkSavepointAdaptorInfo& adaptorInfo = FlinkSavepointAdaptorInfo{},
        const nlohmann::json& operatorDescription = nlohmann::json{},
        const std::string& operatorName = std::string{})
    {
        CheckpointableKeyedStateBackend<K>* keyedStatedBackend = nullptr;
        OperatorStateBackend* osBackend = nullptr;

        PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates = getPrioritizedOperatorSubtaskStates();
        auto restoreCheckpointId = prioritizedOperatorSubtaskStates.getRestoredCheckpointId();

        std::string operatorIdentifierText = getOperatorSubtaskDescriptionText();

        auto taskInfo = env->taskConfiguration();

        // This KeyedStateBackend is a function name, not the base class. See function below.
        RestoreSavepointMode restoreMode = getRestoreSavepointMode();
        keyedStatedBackend = this->keyedStatedBackend<K>(
            keySerializer,
            taskInfo.getMaxNumberOfSubtasks(),
            taskInfo.getNumberOfSubtasks(),
            taskInfo.getIndexOfSubtask(),
            taskInfo.getStateBackend(),
            operatorIdentifierText,
            operatorID,
            adaptorInfo,
            restoreMode,
            operatorDescription);
        osBackend = operatorStateBackend(operatorIdentifierText, operatorID);

        InternalTimeServiceManager<K>* timeServiceManager = nullptr;
        if (keyedStatedBackend != nullptr) {
            keyedStatedBackend->SetOperatorStateMetricGroup(resolveOperatorStateMetricGroup(operatorName));

            int maxNumberOfSubtasks = taskInfo.getMaxNumberOfSubtasks();
            auto rawKeyedStateHandles = collectRawKeyedStateHandles(operatorID);
            auto omniTaskBridge = env != nullptr && env->getTaskStateManager() != nullptr
                                      ? env->getTaskStateManager()->getOmniTaskBridge()
                                      : nullptr;
            timeServiceManager = new InternalTimeServiceManager<K>(
                keyedStatedBackend->getKeyGroupRange(),
                keyContext,
                keyedStatedBackend,
                processingTimeService,
                maxNumberOfSubtasks,
                rawKeyedStateHandles,
                omniTaskBridge);
        }
        return new StreamOperatorStateContextImpl<K>(
            restoreCheckpointId, keyedStatedBackend, osBackend, timeServiceManager);
    }

protected:
    template <typename K>
    AbstractKeyedStateBackend<K>* keyedStatedBackend(
        TypeSerializer* keySerializer, int maxParallelism, int parallelism, int operatorIndex);

    template <typename K>
    AbstractKeyedStateBackend<K>* keyedStatedBackend(
        TypeSerializer* keySerializer,
        int maxParallelism,
        int parallelism,
        int operatorIndex,
        std::string backendType,
        std::string operatorIdentifierText,
        OperatorID* operatorID,
        const FlinkSavepointAdaptorInfo& adaptorInfo = FlinkSavepointAdaptorInfo{},
        RestoreSavepointMode restoreMode = RestoreSavepointMode::OMNI_INTERNAL,
        const nlohmann::json& operatorDescription = nlohmann::json{});

    // This is the one for restore
    template <typename K>
    CheckpointableKeyedStateBackend<K>* keyedStatedBackend(
        TypeSerializer* keySerializer,
        std::string operatorIdentifierText,
        MetricGroup* metricGroup,
        OperatorID* operatorID,
        const FlinkSavepointAdaptorInfo& adaptorInfo = FlinkSavepointAdaptorInfo{},
        RestoreSavepointMode restoreMode = RestoreSavepointMode::OMNI_INTERNAL,
        const nlohmann::json& operatorDescription = nlohmann::json{});

    OperatorStateBackend* operatorStateBackend(std::string operatorIdentifierText, OperatorID* operatorID);

    RestoreSavepointMode getRestoreSavepointMode() const
    {
        if (env == nullptr) {
            return RestoreSavepointMode::OMNI_INTERNAL;
        }
        // SP-INTEROP: 恢复模式单一来源为 JobInformationPOD（经 JobInformationPOJO 传递），
        // 不从 TaskInformationPOD 读取，避免第二来源。
        // jobConfiguration() 返回临时对象，须按值拷贝，避免悬垂引用。
        const std::string modeStr = env->jobConfiguration().getRecoverySavepointFormat();
        auto mode = RestoreSavepointMode::OMNI_INTERNAL;
        if (modeStr == "FLINK_COMPATIBLE" || modeStr == "compatible") {
            mode = RestoreSavepointMode::FLINK_COMPATIBLE;
        }
        return mode;
    }

    std::string getOperatorSubtaskDescriptionText()
    {
        return UUID::randomUUID().ToString();
    }

    PrioritizedOperatorSubtaskState getPrioritizedOperatorSubtaskStates()
    {
        auto operatorIdStr = env->taskConfiguration().getStreamConfigPOD().getOperatorDescription().getOperatorId();
        auto operatorId = TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>(operatorIdStr);
        return env->getTaskStateManager()->prioritizedOperatorState(operatorId);
    }

private:
    KeyGroupRange* computeKeyGroupRangeForOperatorIndex(int maxParallelism, int parallelism, int operatorIndex);

    std::vector<std::shared_ptr<KeyedStateHandle>> collectRawKeyedStateHandles(OperatorID* operatorID = nullptr);

    // Resolves the per-operator state metric group by the operator's own name (matches
    // OperatorPOD::getName()). The name is supplied by each operator via streamOperatorStateContext.
    omnistream::OperatorStateMetricGroup* resolveOperatorStateMetricGroup(const std::string& operatorName)
    {
        if (env == nullptr || operatorName.empty()) {
            return nullptr;
        }
        auto taskMetricGroup = env->taskMetricGroup();
        if (taskMetricGroup == nullptr) {
            return nullptr;
        }
        return taskMetricGroup->GetTaskBackendStateMetricGroup()->GetOrCreateOperatorGroup(operatorName).get();
    }

    StateBackend* stateBackend;
    omnistream::EnvironmentV2* env;
};

inline std::vector<std::shared_ptr<KeyedStateHandle>> StreamTaskStateInitializerImpl::collectRawKeyedStateHandles(
    OperatorID* operatorID)
{
    std::vector<std::shared_ptr<KeyedStateHandle>> result;
    if (env == nullptr || env->getTaskStateManager() == nullptr) {
        return result;
    }

    PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates;
    std::string operatorIdStr;
    if (operatorID) {
        operatorIdStr = operatorID->toString();
        prioritizedOperatorSubtaskStates = env->getTaskStateManager()->prioritizedOperatorState(*operatorID);
    } else {
        operatorIdStr = env->taskConfiguration().getStreamConfigPOD().getOperatorDescription().getOperatorId();
        auto operatorId = TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>(operatorIdStr);
        prioritizedOperatorSubtaskStates = env->getTaskStateManager()->prioritizedOperatorState(operatorId);
    }

    const auto& handleVector = prioritizedOperatorSubtaskStates.getPrioritizedRawKeyedState();
    for (const auto& collection : handleVector) {
        for (const auto& handle : collection) {
            if (handle != nullptr) {
                result.push_back(handle);
            }
        }
        if (!result.empty()) {
            break;
        }
    }

    INFO_RELEASE(
        "[RocksDB-HEAP-PQ-CP-restore] operatorId=" << operatorIdStr << " rawKeyedStateHandles=" << result.size());
    return result;
}

template <typename K>
AbstractKeyedStateBackend<K>* StreamTaskStateInitializerImpl::keyedStatedBackend(
    TypeSerializer* keySerializer, int maxParallelism, int parallelism, int operatorIndex)
{
    if (keySerializer == nullptr) {
        return nullptr;
    }

    // maxParallelism should be greater than parallelism
    int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
    int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
    KeyGroupRange* keyGroupRange = new KeyGroupRange(start, end);

    INFO_RELEASE(
        "operatorIndex " << operatorIndex << " maxParallelism " << maxParallelism << " parallelism " << parallelism
                         << " start " << start << " end " << end);
    // Not sure about maxParallelism being the input here
    InternalKeyContextImpl<K>* keyContext = new InternalKeyContextImpl<K>(keyGroupRange, maxParallelism);
    keyContext->setCurrentKeyGroupIndex(start);

    return new HeapKeyedStateBackend<K>(keySerializer, keyContext);
}

template <typename K>
AbstractKeyedStateBackend<K>* StreamTaskStateInitializerImpl::keyedStatedBackend(
    TypeSerializer* keySerializer,
    int maxParallelism,
    int parallelism,
    int operatorIndex,
    std::string backendType,
    std::string operatorIdentifierText,
    OperatorID* operatorID,
    const FlinkSavepointAdaptorInfo& adaptorInfo,
    RestoreSavepointMode restoreMode,
    const nlohmann::json& operatorDescription)
{
    if (keySerializer == nullptr) {
        return nullptr;
    }

    // maxParallelism should be greater than parallelism
    int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
    int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
    KeyGroupRange* keyGroupRange = new KeyGroupRange(start, end);

    INFO_RELEASE(
        "operatorIndex " << operatorIndex << " maxParallelism " << maxParallelism << " parallelism " << parallelism
                         << " start " << start << " end " << end);
    // Not sure about maxParallelism being the input here
    InternalKeyContextImpl<K>* keyContext = new InternalKeyContextImpl<K>(keyGroupRange, maxParallelism);
    keyContext->setCurrentKeyGroupIndex(start);

    if (backendType == "HashMapStateBackend") {
        delete keyContext; // builder creates its own InternalKeyContext

        HeapKeyedStateBackendBuilder<K> builder(keySerializer, maxParallelism, keyGroupRange);
        builder.setFlinkSavepointAdaptorInfo(adaptorInfo)
            .setRestoreSavepointMode(restoreMode)
            .setTaskType(env->taskConfiguration().GetTaskType())
            .setOperatorDescription(operatorDescription);
        auto taskStateManager = env == nullptr ? nullptr : env->getTaskStateManager();
        if (taskStateManager == nullptr) {
            INFO_RELEASE("HashMapStateBackend: no TaskStateManager, starting with empty heap state");
            return builder.build();
        }

        auto omniTaskBridge = taskStateManager->getOmniTaskBridge();
        if (omniTaskBridge) {
            builder.setOmniTaskBridge(omniTaskBridge);
        }
        if (!taskStateManager->hasJobManagerTaskRestore()) {
            INFO_RELEASE("HashMapStateBackend: no JobManagerTaskRestore, starting with empty heap state");
            return builder.build();
        }

        PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates;
        if (operatorID) {
            prioritizedOperatorSubtaskStates = taskStateManager->prioritizedOperatorState(*operatorID);
        } else {
            auto operatorIdStr = env->taskConfiguration().getStreamConfigPOD().getOperatorDescription().getOperatorId();
            auto operatorId = TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>(operatorIdStr);
            prioritizedOperatorSubtaskStates = taskStateManager->prioritizedOperatorState(operatorId);
        }

        auto handleVector = prioritizedOperatorSubtaskStates.getPrioritizedManagedKeyedState();
        if (!handleVector.empty()) {
            // Use the first (highest priority) alternative
            std::set<std::shared_ptr<KeyedStateHandle>> stateHandles;
            for (const auto& collection : handleVector) {
                for (const auto& handle : collection) {
                    if (handle) {
                        stateHandles.insert(handle);
                    }
                }
                if (!stateHandles.empty()) {
                    break; // Use the highest priority alternative
                }
            }
            if (!stateHandles.empty()) {
                builder.setStateHandles(stateHandles);
            }
        }

        return builder.build();
    } else if (backendType == "EmbeddedRocksDBStateBackend") {
        return static_cast<AbstractKeyedStateBackend<K>*>(keyedStatedBackend<K>(
            keySerializer, operatorIdentifierText, nullptr, operatorID, adaptorInfo, restoreMode, operatorDescription));
    }
#ifdef WITH_OMNISTATESTORE
    if (backendType == "EmbeddedOckStateBackend") {
        return new BssKeyedStateBackend<K>(keySerializer, keyContext, start, end, maxParallelism);
    }

#endif
    return nullptr;
}

template <typename K>
inline CheckpointableKeyedStateBackend<K>* StreamTaskStateInitializerImpl::keyedStatedBackend(
    TypeSerializer* keySerializer,
    std::string operatorIdentifierText,
    MetricGroup* metricGroup,
    OperatorID* operatorID,
    const FlinkSavepointAdaptorInfo& adaptorInfo,
    RestoreSavepointMode restoreMode,
    const nlohmann::json& operatorDescription)
{
    if (keySerializer == nullptr) {
        return nullptr;
    }

    std::string logDescription = "keyed state backend for " + operatorIdentifierText;

    auto taskInfo = env->taskConfiguration();

    PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates;
    if (operatorID) {
        prioritizedOperatorSubtaskStates = env->getTaskStateManager()->prioritizedOperatorState(*operatorID);
    } else {
        prioritizedOperatorSubtaskStates = getPrioritizedOperatorSubtaskStates();
    }

    KeyGroupRange* keyGroupRange = computeKeyGroupRangeForOperatorIndex(
        taskInfo.getMaxNumberOfSubtasks(), taskInfo.getNumberOfSubtasks(), taskInfo.getIndexOfSubtask());

    auto backendRestorer =
        BackendRestorerProcedure<CheckpointableKeyedStateBackend<K>*, std::shared_ptr<KeyedStateHandle>>(
            [this,
             operatorIdentifierText,
             keyGroupRange,
             keySerializer,
             taskInfo,
             adaptorInfo,
             restoreMode,
             operatorDescription](std::set<std::shared_ptr<KeyedStateHandle>> stateHandles, int alternativeIdx) {
                auto rocksdbStateBackend = dynamic_cast<EmbeddedRocksDBStateBackend*>(this->stateBackend);
                if (rocksdbStateBackend == nullptr) {
                    INFO_RELEASE("Error: stateBackend is not EmbeddedRocksDBStateBackend.");
                    THROW_RUNTIME_ERROR("stateBackend is not EmbeddedRocksDBStateBackend.");
                }
                return reinterpret_cast<CheckpointableKeyedStateBackend<K>*>(
                    rocksdbStateBackend->template createKeyedStateBackend<K>(
                        env,
                        operatorIdentifierText,
                        stateHandles,
                        keyGroupRange,
                        keySerializer,
                        taskInfo.getMaxNumberOfSubtasks(),
                        alternativeIdx,
                        adaptorInfo,
                        restoreMode,
                        operatorDescription));
            },
            logDescription);

    try {
        std::vector<StateObjectCollection<KeyedStateHandle>> handleVector =
            prioritizedOperatorSubtaskStates.getPrioritizedManagedKeyedState();
        std::vector<std::set<std::shared_ptr<KeyedStateHandle>>> handleSet;
        handleSet.reserve(handleVector.size());
        for (size_t i = 0; i < handleVector.size(); i++) {
            std::set<std::shared_ptr<KeyedStateHandle>> set;
            for (const auto& handle : handleVector[i]) {
                set.insert(handle);
            }
            handleSet.push_back(std::move(set));
        }
        return backendRestorer.createAndRestore(handleSet);
    } catch (const std::exception& ex) {
        INFO_RELEASE("Error:create keyedStatedBackend failed: " + std::string(ex.what()));
        THROW_RUNTIME_ERROR("create keyedStatedBackend failed: " + std::string(ex.what()));
    }
}

inline OperatorStateBackend* StreamTaskStateInitializerImpl::operatorStateBackend(
    std::string operatorIdentifierText, OperatorID* operatorID)
{
    std::string logDescription = "operator state backend for " + operatorIdentifierText;

    auto backendRestorer = new BackendRestorerProcedure<OperatorStateBackend*, std::shared_ptr<OperatorStateHandle>>(
        [this, operatorIdentifierText](
            std::set<std::shared_ptr<OperatorStateHandle>> stateHandles, int alternativeIdx) {
            auto embeddedRocksDBStateBackend = dynamic_cast<EmbeddedRocksDBStateBackend*>(this->stateBackend);
            if (embeddedRocksDBStateBackend) {
                return reinterpret_cast<OperatorStateBackend*>(
                    embeddedRocksDBStateBackend->createOperatorStateBackend(env, operatorIdentifierText, stateHandles));
            }
            auto hashMapStateBackend = dynamic_cast<HashMapStateBackend*>(this->stateBackend);
            if (hashMapStateBackend) {
                return reinterpret_cast<OperatorStateBackend*>(
                    hashMapStateBackend->createOperatorStateBackend(env, operatorIdentifierText, stateHandles));
            }
        },
        logDescription);

    try {
        PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates;
        if (operatorID) {
            prioritizedOperatorSubtaskStates = env->getTaskStateManager()->prioritizedOperatorState(*operatorID);
        } else {
            prioritizedOperatorSubtaskStates = getPrioritizedOperatorSubtaskStates();
        }
        std::vector<StateObjectCollection<OperatorStateHandle>> handleVector =
            prioritizedOperatorSubtaskStates.getPrioritizedManagedOperatorState();
        std::vector<std::set<std::shared_ptr<OperatorStateHandle>>> handleSet;
        handleSet.reserve(handleVector.size());
        for (const auto& collection : handleVector) {
            std::set<std::shared_ptr<OperatorStateHandle>> set;
            for (const auto& handle : collection) {
                set.insert(handle);
            }

            handleSet.push_back(std::move(set));
        }
        return backendRestorer->createAndRestore(handleSet);
    } catch (const std::exception& e) {
        GErrorLog("create OperatorStateHandle exception : " + std::string(e.what()));
        throw std::runtime_error("create OperatorStateHandle failed.");
    }
}
