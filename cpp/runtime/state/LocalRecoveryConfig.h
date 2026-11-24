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

#ifndef OMNISTREAM_LOCALRECOVERYCONFIG_H
#define OMNISTREAM_LOCALRECOVERYCONFIG_H

#include "LocalRecoveryDirectoryProvider.h"
#include "LocalRecoveryDirectoryProviderImpl.h"
#include <optional>
#include <stdexcept>
#include <memory>

class LocalRecoveryConfig {
public:
    explicit LocalRecoveryConfig(std::shared_ptr<LocalRecoveryDirectoryProvider> directoryProvider)
        : localStateDirectories_(std::move(directoryProvider))
    {
    }

    bool IsLocalRecoveryEnabled() const
    {
        return localStateDirectories_ != nullptr;
    }

    std::shared_ptr<LocalRecoveryDirectoryProvider> GetLocalStateDirectoryProvider() const
    {
        return localStateDirectories_;
    }

    std::string ToString() const
    {
        return "LocalRecoveryConfig{localStateDirectories=" +
               (localStateDirectories_ ? localStateDirectories_->ToString() : std::string("null")) + "}";
    }

private:
    std::shared_ptr<LocalRecoveryDirectoryProvider> localStateDirectories_;
};

#endif // OMNISTREAM_LOCALRECOVERYCONFIG_H
