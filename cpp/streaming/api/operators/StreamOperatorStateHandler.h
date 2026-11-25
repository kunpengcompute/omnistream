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

#ifndef FLINK_TNEL_STREAMOPERATORSTATEHANDLER_H
#define FLINK_TNEL_STREAMOPERATORSTATEHANDLER_H

#include <string>
#include <iostream>
#include <future>
#include <memory>
#include <stdexcept>
#include <utility>
#include <nlohmann/json.hpp>
#include "runtime/state/AbstractKeyedStateBackend.h"
#include "StreamOperatorStateContext.h"
#include "runtime/state/DefaultKeyedStateStore.h"
#include "StreamTaskStateInitializerImpl.h"
#include "state/CheckpointableKeyedStateBackend.h"
#include "state/FullSnapshotResources.h"
#include "state/SnapshotStrategy.h"
#include "state/bridge/OmniTaskBridge.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/OperatorSavepointAdaptorFactory.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/CompatibleSavepointSnapshotResources.h"
#include "runtime/state/CompatibleSavepointSnapshotStrategy.h"
#include "runtime/state/StateSnapshotContextSynchronousImpl.h"
#include "runtime/state/OperatorStateBackend.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/SnapshotStrategyRunner.h"
#include "bridge/OmniTaskBridgeImpl2.h"
#include "state/SavepointSnapshotStrategy.h"
#include "runtime/state/StateInitializationContextImpl.h"

using omnistream::OmniTaskBridge;

template <typename K>
class StreamOperatorStateHandler {
public:
    StreamOperatorStateHandler(StreamOperatorStateContextImpl<K>* context)
    {
        this->context = context;
        this->keyedStateBackend = context->keyedStateBackend();
        this->operatorStateBackend = context->operatorStateBackend();
        if (keyedStateBackend != nullptr) {
            keyedStateStore =
                new DefaultKeyedStateStore<K>(dynamic_cast<AbstractKeyedStateBackend<K>*>(keyedStateBackend));
        } else {
            keyedStateStore = nullptr;
        }
    };

    ~StreamOperatorStateHandler()
    {
        LOG("StreamOperatorStateHandler::~StreamOperatorStateHandler - delete keyedStateBackend");
        if (context != nullptr) {
            delete context;
            context = nullptr;
        }
        if (keyedStateStore != nullptr) {
            delete keyedStateStore;
            keyedStateStore = nullptr;
        }
    }
    void setCurrentKey(K key)
    {
        keyedStateBackend->setCurrentKey(key);
    };
    K getCurrentKey()
    {
        return keyedStateBackend->getCurrentKey();
    };
    AbstractKeyedStateBackend<K>* getKeyedStateBackend()
    {
        // Downcasting from CheckpointableKeyedStateBackend. This is temporarily OK because AbstractKeyedStateBackend
        // is currently the only direct inheritor of CheckpointableStateBackend
        return dynamic_cast<AbstractKeyedStateBackend<K>*>(keyedStateBackend);
    };
    DefaultKeyedStateStore<K>* getKeyedStateStore()
    {
        return keyedStateStore;
    };

    OperatorStateBackend* getOperatorStateBackend()
    {
        return operatorStateBackend;
    }

    void dispose()
    {
        if (keyedStateBackend != nullptr) {
            keyedStateBackend->dispose();
        }
        if (operatorStateBackend != nullptr) {
            operatorStateBackend->dispose();
        }
    }

    OperatorSnapshotFutures* snapshotState();

    void notifyCheckpointComplete(long checkpointId)
    {
        auto backend = dynamic_cast<RocksdbKeyedStateBackend<K>*>(keyedStateBackend);
        if (backend) {
            backend->notifyCheckpointComplete(checkpointId);
        }
    }

    void notifyCheckpointAborted(long checkpointId)
    {
        INFO_RELEASE("notifyCheckpointAborted");
        auto backend = dynamic_cast<RocksdbKeyedStateBackend<K>*>(keyedStateBackend);
        if (backend) {
            backend->notifyCheckpointAborted(checkpointId);
        }
    }

    class CheckpointedStreamOperator {
    public:
        virtual void snapshotState(StateSnapshotContextSynchronousImpl* context)
        {
        }
        virtual void initializeState(StateInitializationContextImpl* context)
        {
        }
        virtual FlinkSavepointAdaptorInfo getSavepointAdaptorInfo() const
        {
            return {};
        }

        virtual nlohmann::json getOperatorDescription() const
        {
            return {};
        }
    };

    void initializeOperatorState(CheckpointedStreamOperator* streamOperator)
    {
        StateInitializationContextImpl* initializationContext = nullptr;
        try {
            // Get restored checkpoint id from context
            std::optional<uint64_t> checkpointId = context->getRestoredCheckpointId();

            // Create StateInitializationContextImpl with correct template parameter
            initializationContext = new StateInitializationContextImpl(
                checkpointId,
                this->operatorStateBackend, // access to operator state backend
                this->keyedStateStore       // access to keyed state store
            );
            streamOperator->initializeState(initializationContext);
            delete initializationContext; // 释放内存，避免泄漏
        } catch (const std::exception& e) {
            if (initializationContext) {
                delete initializationContext; // 释放内存，避免泄漏
            }
            INFO_RELEASE("Error in initializeOperatorState: " << e.what());
            throw; // 重新抛出异常
        }
    }

    OperatorSnapshotFutures* SnapshotState(
        CheckpointedStreamOperator* streamOperator,
        InternalTimeServiceManager<K>* timeServiceManager,
        std::string operatorName,
        long checkpointId,
        long timestamp,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
        bool isUsingCustomRawKeyedState,
        const std::shared_ptr<OmniTaskBridge>& bridge)
    {
        KeyGroupRange* keyGroupRange = KeyGroupRange::EMPTY_KEY_GROUP_RANGE();
        if (keyedStateBackend != nullptr) {
            keyGroupRange = keyedStateBackend->getKeyGroupRange();
        }

        auto snapshotInProgress = std::make_unique<OperatorSnapshotFutures>();

        auto snapshotContext = std::make_unique<StateSnapshotContextSynchronousImpl>(
            checkpointId, timestamp, checkpointStreamFactory, keyGroupRange, bridge, checkpointOptions);

        snapshotState(
            streamOperator,
            timeServiceManager,
            operatorName,
            checkpointId,
            timestamp,
            checkpointOptions,
            checkpointStreamFactory,
            snapshotInProgress.get(),
            snapshotContext.get(),
            isUsingCustomRawKeyedState,
            bridge);

        return snapshotInProgress.release();
    }

    void snapshotState(
        CheckpointedStreamOperator* streamOperator,
        InternalTimeServiceManager<K>* timeServiceManager,
        std::string operatorName,
        long checkpointId,
        long timestamp,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
        OperatorSnapshotFutures* snapshotInProgress,
        StateSnapshotContextSynchronousImpl* snapshotContext,
        bool isUsingCustomRawKeyedState,
        const std::shared_ptr<OmniTaskBridge>& bridge)
    {
        try {
            if (timeServiceManager != nullptr) {
                if (keyedStateBackend == nullptr) {
                    THROW_LOGIC_EXCEPTION("keyedStateBackend should be available with timeServiceManager");
                }

                AbstractKeyedStateBackend<K>* abstractBackend =
                    dynamic_cast<AbstractKeyedStateBackend<K>*>(keyedStateBackend);

                bool requiresLegacyRawKeyedStateSnapshots =
                    abstractBackend &&
                    abstractBackend->requiresLegacySynchronousTimerSnapshots(checkpointOptions->GetCheckpointType());
                if (requiresLegacyRawKeyedStateSnapshots) {
                    if (isUsingCustomRawKeyedState) {
                        THROW_LOGIC_EXCEPTION(
                            "Attempting to snapshot timers to raw keyed state, but this operator has "
                            "custom raw keyed state to write.");
                    }

                    timeServiceManager->snapshotToRawKeyedState(
                        snapshotContext->getRawKeyedOperatorStateOutput(), operatorName, checkpointId);
                }
            }
            INFO_RELEASE(
                "savepoint: StreamOperatorStateHandler::snapshotState streamOperator type="
                << typeid(*streamOperator).name());
            streamOperator->snapshotState(snapshotContext);
            snapshotInProgress->setKeyedStateRawFuture(snapshotContext->getKeyedStateStreamFuture());
            snapshotInProgress->setOperatorStateRawFuture(snapshotContext->getOperatorStateStreamFuture());

            if (operatorStateBackend) {
                snapshotInProgress->setOperatorStateManagedFuture(operatorStateBackend->snapshot(
                    checkpointId, timestamp, checkpointStreamFactory, checkpointOptions));
            }

            if (keyedStateBackend) {
                // Set bridge on Heap backend for checkpoint (RocksDB gets it via constructor)
                auto heapBackend = dynamic_cast<HeapKeyedStateBackend<K>*>(keyedStateBackend);
                if (heapBackend && bridge) {
                    heapBackend->setOmniTaskBridge(bridge);
                }

                auto keySerializer = keyedStateBackend->getKeySerializer();
                if (isCanonicalSavepoint(checkpointOptions->GetCheckpointType())) {
                    auto snapshotRunner = prepareCanonicalSavepoint(keyedStateBackend);
                    snapshotInProgress->setKeyedStateManagedFuture(snapshotRunner->snapshot(
                        checkpointId,
                        timestamp,
                        checkpointStreamFactory,
                        checkpointOptions,
                        bridge,
                        keySerializer->toJson()));
                } else if (isCompatibleSavepoint(checkpointOptions->GetCheckpointType())) {
                    const auto& adaptorInfo = streamOperator->getSavepointAdaptorInfo();
                    INFO_RELEASE(
                        "StreamOperatorStateHandler::snapshotState entering flink savepoint compatible save flow - "
                        "operator="
                        << operatorName << ", checkpointId=" << checkpointId << ", adaptorInfo type="
                        << static_cast<int>(adaptorInfo.type) << ", reason=" << adaptorInfo.reason);
                    snapshotInProgress->setKeyedStateManagedFuture(prepareCompatibleSavepoint(
                        streamOperator,
                        keyedStateBackend,
                        checkpointId,
                        timestamp,
                        checkpointStreamFactory,
                        checkpointOptions,
                        bridge,
                        keySerializer->toJson()));
                } else {
                    snapshotInProgress->setKeyedStateManagedFuture(keyedStateBackend->snapshot(
                        checkpointId, timestamp, checkpointStreamFactory, checkpointOptions));
                }
            }
        } catch (const std::exception& e) {
            INFO_RELEASE(
                "Error:StreamOperatorStateHandler::snapshotState operator="
                << operatorName << ", checkpointId=" << checkpointId << ", exception=" << e.what());
            try {
                snapshotInProgress->cancel(e.what());
            } catch (...) {
                // Do nothing
            }
            try {
                snapshotContext->closeExceptionally();
            } catch (...) {
                // Do nothing
            }
        } catch (...) {
            INFO_RELEASE(
                "Error:StreamOperatorStateHandler::snapshotState operator=" << operatorName << ", checkpointId="
                                                                            << checkpointId << ", exception=unknown");
            try {
                snapshotInProgress->cancel("unknown exception");
            } catch (...) {
                // Do nothing
            }
            try {
                snapshotContext->closeExceptionally();
            } catch (...) {
                // Do nothing
            }
        }
    };

private:
    // own backend
    StreamOperatorStateContextImpl<K>* context = nullptr;
    static bool isCanonicalSavepoint(SnapshotType* snapshotType)
    {
        return snapshotType->IsSavepoint() &&
               dynamic_cast<SavepointType*>(snapshotType)->getFormatType() == SavepointFormatType::CANONICAL;
    };

    static bool isCompatibleSavepoint(SnapshotType* snapshotType)
    {
        return snapshotType->IsSavepoint() &&
               dynamic_cast<SavepointType*>(snapshotType)->getFormatType() == SavepointFormatType::COMPATIBLE;
    };

    template <typename T>
    static std::shared_ptr<SnapshotStrategyRunner<KeyedStateHandle, FullSnapshotResources>> prepareCanonicalSavepoint(
        CheckpointableKeyedStateBackend<T>* keyedStateBackend)
    {
        auto savepointResources = keyedStateBackend->savepoint();
        auto savepointSnapshotStrategy =
            std::make_shared<SavepointSnapshotStrategy>(savepointResources->getSnapshotResources());
        return std::make_shared<SnapshotStrategyRunner<KeyedStateHandle, FullSnapshotResources>>(
            "Asynchronous full Savepoint",
            std::move(savepointSnapshotStrategy),
            savepointResources->getPreferredSnapshotExecutionType());
    }

    template <typename T>
    static std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>
    prepareCompatibleSavepoint(
        CheckpointedStreamOperator* streamOperator,
        CheckpointableKeyedStateBackend<T>* keyedStateBackend,
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
        CheckpointOptions* checkpointOptions,
        const std::shared_ptr<OmniTaskBridge>& bridge,
        const std::string& keySerializer)
    {
        auto adaptorInfo = streamOperator->getSavepointAdaptorInfo();
        if (adaptorInfo.type == FlinkSavepointAdaptorType::None) {
            INFO_RELEASE(
                "Error:StreamOperatorStateHandler::prepareCompatibleSavepoint cp="
                << checkpointId << " unsupported operator reason=" << adaptorInfo.reason);
            throw std::runtime_error("Compatible savepoint is not supported for this operator: " + adaptorInfo.reason);
        }
        if (adaptorInfo.type == FlinkSavepointAdaptorType::OmniIsCompatible) {
            return prepareCanonicalSavepoint(keyedStateBackend)
                ->snapshot(checkpointId, timestamp, checkpointStreamFactory, checkpointOptions, bridge, keySerializer);
        }

        auto adaptor = omnistream::OperatorSavepointAdaptorFactory::createAdaptor(adaptorInfo.type);
        if (adaptor == nullptr) {
            INFO_RELEASE(
                "Error:StreamOperatorStateHandler::prepareCompatibleSavepoint cp="
                << checkpointId << " adaptor factory returned null type=" << static_cast<int>(adaptorInfo.type));
            throw std::runtime_error("Compatible savepoint adaptor factory returned null");
        }
        adaptor->prepareForSave(streamOperator->getOperatorDescription());
        auto savepointResources = keyedStateBackend->compatibleSavepoint();
        auto compatibleResources = std::make_shared<CompatibleSavepointSnapshotResources>(
            savepointResources->getSnapshotResources(), std::move(adaptor), adaptorInfo);
        auto compatibleSnapshotStrategy = std::make_shared<CompatibleSavepointSnapshotStrategy>(compatibleResources);
        auto snapshotRunner =
            std::make_shared<SnapshotStrategyRunner<KeyedStateHandle, CompatibleSavepointSnapshotResources>>(
                "Asynchronous compatible Savepoint",
                std::move(compatibleSnapshotStrategy),
                savepointResources->getPreferredSnapshotExecutionType());
        return snapshotRunner->snapshot(
            checkpointId, timestamp, checkpointStreamFactory, checkpointOptions, bridge, keySerializer);
    }

    // own backend
    CheckpointableKeyedStateBackend<K>* keyedStateBackend;
    DefaultKeyedStateStore<K>* keyedStateStore;
    OperatorStateBackend* operatorStateBackend;
};

#endif // FLINK_TNEL_STREAMOPERATORSTATEHANDLER_H
