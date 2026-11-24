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

#ifndef OMNISTREAM_SNAPSHOTSTRATEGYRUNNER
#define OMNISTREAM_SNAPSHOTSTRATEGYRUNNER

#include "SnapshotExecutionType.h"
#include "SnapshotResult.h"
#include "CheckpointStreamFactory.h"
#include "AsyncSnapshotCallable.h"
#include "SnapshotStrategy.h"
#include "SnapshotResources.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "KeyedStateHandle.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include <string>
#include <future>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <utility>

template <typename T, typename SR>
class SnapshotStrategyRunner {
public:
    SnapshotStrategyRunner(
        std::string description,
        std::shared_ptr<SnapshotStrategy<T, SR>> snapshotStrategy,
        SnapshotExecutionType executionType)
        : description_(std::move(description)),
          snapshotStrategy_(std::move(snapshotStrategy)),
          executionType_(executionType)
    {
        if (snapshotStrategy_ == nullptr) {
            INFO_RELEASE("Error:SnapshotStrategyRunner[" << description_ << "]: snapshot strategy is null");
            throw std::invalid_argument("Snapshot strategy must not be null");
        }
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<T>>()>> snapshot(
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<omnistream::OmniTaskBridge> bridge,
        std::string keySerializer)
    {
        try {
            auto snapshotResources = snapshotStrategy_->syncPrepareResources(checkpointId);
            auto asyncSnapshot = snapshotStrategy_->asyncSnapshot(
                snapshotResources, checkpointId, timestamp, streamFactory, checkpointOptions, keySerializer);
            auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<T>>()>>(
                [asyncSnapshot, snapshotResources, bridge]() {
                    try {
                        auto res = asyncSnapshot->get(bridge);
                        snapshotResources->cleanup();
                        return res;
                    } catch (...) {
                        snapshotResources->cleanup();
                        throw;
                    }
                });

            if (executionType_ == SnapshotExecutionType::SYNCHRONOUS) {
                try {
                    auto res = asyncSnapshot->get(bridge);
                    snapshotResources->cleanup();
                    if (res) {
                        LOG("native rocksdb checkpoint has been finished.");
                    }
                } catch (...) {
                    snapshotResources->cleanup();
                    throw;
                }
            }
            return task;
        } catch (const std::exception& e) {
            INFO_RELEASE(
                "Error:SnapshotStrategyRunner["
                << description_ << "]: snapshot pipeline failed during preparation, checkpointId=" << checkpointId
                << ", exception=" << e.what());
            throw;
        } catch (...) {
            INFO_RELEASE(
                "Error:SnapshotStrategyRunner["
                << description_ << "]: snapshot pipeline failed during preparation, checkpointId=" << checkpointId
                << ", exception=unknown");
            throw;
        }
    }

private:
    std::string description_;
    std::shared_ptr<SnapshotStrategy<T, SR>> snapshotStrategy_;
    SnapshotExecutionType executionType_;
};

#endif // OMNISTREAM_SNAPSHOTSTRATEGYRUNNER
