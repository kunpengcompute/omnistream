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

#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "common.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/OperatorSavepointAdaptor.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/state/heap/HeapRestoreBackendDelegate.h"
#include "runtime/state/restore/FullSnapshotRestoreOperation.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
#include "typeutils/TypeSerializer.h"

// Heap compatible full restore operation 在 construction backend 发布前消费已经 prepared 的 adaptor。该类不直接写入
// Heap state table，也不拥有 backend 生命周期；它只负责 metadata validate/restore 的顺序调度，
// prepareForRestore 必须由 builder 在创建 adaptor 后完成，失败时向 builder 抛出异常，避免发布部分恢复的 backend。
template <typename K>
class HeapCompatibleFullRestoreOperation {
public:
    HeapCompatibleFullRestoreOperation(
        HeapKeyedStateBackend<K>* constructionBackend,
        KeyGroupRange* keyGroupRange,
        std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles,
        std::shared_ptr<TypeSerializer> keySerializerProvider,
        int numberOfKeyGroups,
        std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge,
        FlinkSavepointAdaptorInfo adaptorInfo,
        std::unique_ptr<omnistream::OperatorSavepointAdaptor> preparedAdaptor,
        const omnistream::OperatorRuntimeStateSchemaProvider* runtimeSchemaProvider = nullptr)
        : constructionBackend_(constructionBackend),
          keyGroupRange_(keyGroupRange),
          restoreStateHandles_(std::move(restoreStateHandles)),
          keySerializerProvider_(std::move(keySerializerProvider)),
          numberOfKeyGroups_(numberOfKeyGroups),
          omniTaskBridge_(std::move(omniTaskBridge)),
          adaptorInfo_(std::move(adaptorInfo)),
          preparedAdaptor_(std::move(preparedAdaptor)),
          runtimeSchemaProvider_(runtimeSchemaProvider)
    {
    }

    void restore()
    {
        if (restoreStateHandles_.empty()) {
            return;
        }
        if (omniTaskBridge_ == nullptr) {
            INFO_RELEASE(
                "Error:HeapCompatibleFullRestoreOperation::restore missing OmniTaskBridge, adaptorType="
                << static_cast<int>(adaptorInfo_.type) << ", reason=" << adaptorInfo_.reason
                << ", stateHandleCount=" << restoreStateHandles_.size());
            throw std::invalid_argument("Heap compatible restore requires OmniTaskBridge when state handles exist");
        }
        if (preparedAdaptor_ == nullptr) {
            INFO_RELEASE(
                "Error:HeapCompatibleFullRestoreOperation::restore missing prepared adaptor, adaptorType="
                << static_cast<int>(adaptorInfo_.type) << ", reason=" << adaptorInfo_.reason
                << ", stateHandleCount=" << restoreStateHandles_.size());
            throw std::runtime_error(
                "Heap compatible restore requires prepared adaptor, type=" +
                std::to_string(static_cast<int>(adaptorInfo_.type)) +
                ", builder/factory did not provide a prepared adaptor");
        }

        validateRestoreStateHandles();

        FullSnapshotRestoreOperation<K> restoreOperation(
            keyGroupRange_, restoreStateHandles_, keySerializerProvider_, omniTaskBridge_);
        auto restoreIterator = restoreOperation.restore();
        omnistream::HeapRestoreBackendDelegate<K> backendDelegate(
            constructionBackend_,
            keySerializerProvider_,
            CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups_),
            runtimeSchemaProvider_);
        preparedAdaptor_->restore(*restoreIterator, backendDelegate);
    }

private:
    void validateRestoreStateHandles()
    {
        // State metadata and kvStateId are local to each KeyGroupsStateHandle.
        // Validate one handle at a time. Keep this probe independent from the iterator consumed by restore().
        SavepointRestoreResultIterator metaProbe(restoreStateHandles_, omniTaskBridge_);
        while (metaProbe.hasNext()) {
            auto restoreResult = metaProbe.next();
            const auto& handleMetaInfos = restoreResult->getStateMetaInfoSnapshots();
            std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos;
            metaInfos.reserve(handleMetaInfos.size());
            for (const auto& metaInfo : handleMetaInfos) {
                metaInfos.push_back(std::make_shared<StateMetaInfoSnapshot>(metaInfo));
            }
            preparedAdaptor_->validateForRestore(metaInfos);
        }
    }

    HeapKeyedStateBackend<K>* constructionBackend_;
    KeyGroupRange* keyGroupRange_;
    std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles_;
    std::shared_ptr<TypeSerializer> keySerializerProvider_;
    int numberOfKeyGroups_;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge_;
    FlinkSavepointAdaptorInfo adaptorInfo_;
    std::unique_ptr<omnistream::OperatorSavepointAdaptor> preparedAdaptor_;
    const omnistream::OperatorRuntimeStateSchemaProvider* runtimeSchemaProvider_;
};
