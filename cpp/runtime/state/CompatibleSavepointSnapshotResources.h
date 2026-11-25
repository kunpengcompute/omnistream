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

#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/OperatorSavepointAdaptor.h"
#include "state/FullSnapshotResources.h"
#include "common.h"

// Compatible savepoint 同步准备阶段的资源包装。该类只组合底层 FullSnapshotResources、已准备好的 adaptor
// 和 adaptorInfo，不写 savepoint、不做 payload 转换；调用方通过 adaptor() 临时访问 adaptor，通过 takeAdaptor()
// 将所有权一次性转给 async writer，cleanup() 只负责把资源释放传递给底层 resources。
class CompatibleSavepointSnapshotResources {
public:
    CompatibleSavepointSnapshotResources(
        std::shared_ptr<FullSnapshotResources> sourceResources,
        std::unique_ptr<omnistream::OperatorSavepointAdaptor> adaptor,
        FlinkSavepointAdaptorInfo adaptorInfo)
        : sourceResources_(std::move(sourceResources)),
          adaptor_(std::move(adaptor)),
          adaptorInfo_(std::move(adaptorInfo))
    {
        if (sourceResources_ == nullptr) {
            INFO_RELEASE("Error:CompatibleSavepointSnapshotResources source resources are null");
            throw std::invalid_argument("Compatible savepoint source resources must not be null");
        }
        if (adaptor_ == nullptr) {
            INFO_RELEASE("Error:CompatibleSavepointSnapshotResources adaptor is null");
            throw std::invalid_argument("Compatible savepoint adaptor must not be null");
        }
    }

    const std::shared_ptr<FullSnapshotResources>& sourceResources() const
    {
        return sourceResources_;
    }

    omnistream::OperatorSavepointAdaptor& adaptor() const
    {
        if (adaptor_ == nullptr) {
            INFO_RELEASE("Error:CompatibleSavepointSnapshotResources adaptor has been transferred");
            throw std::logic_error("Compatible savepoint adaptor has been transferred");
        }
        return *adaptor_;
    }

    std::unique_ptr<omnistream::OperatorSavepointAdaptor> takeAdaptor()
    {
        if (adaptor_ == nullptr) {
            INFO_RELEASE("Error:CompatibleSavepointSnapshotResources adaptor has already been transferred");
            throw std::logic_error("Compatible savepoint adaptor has already been transferred");
        }
        return std::move(adaptor_);
    }

    const FlinkSavepointAdaptorInfo& adaptorInfo() const
    {
        return adaptorInfo_;
    }

    void cleanup()
    {
        if (cleanedUp_) {
            return;
        }
        cleanedUp_ = true;
        sourceResources_->cleanup();
    }

private:
    std::shared_ptr<FullSnapshotResources> sourceResources_;
    std::unique_ptr<omnistream::OperatorSavepointAdaptor> adaptor_;
    FlinkSavepointAdaptorInfo adaptorInfo_;
    bool cleanedUp_ = false;
};
