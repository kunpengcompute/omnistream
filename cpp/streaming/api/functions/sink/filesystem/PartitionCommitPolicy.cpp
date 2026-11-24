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

#include "PartitionCommitPolicy.h"

namespace {

void addPolicy(std::vector<std::unique_ptr<PartitionCommitPolicy>>& policies, const std::string& kind)
{
    std::string trimmed = kind;
    trimmed.erase(0, trimmed.find_first_not_of(" \t"));
    trimmed.erase(trimmed.find_last_not_of(" \t") + 1);

    if (trimmed == "success-file") {
        policies.push_back(std::make_unique<SuccessFileCommitPolicy>());
    } else if (trimmed == "metastore") {
        policies.push_back(std::make_unique<MetastoreCommitPolicy>());
    }
}

} // anonymous namespace

std::vector<std::unique_ptr<PartitionCommitPolicy>> createPolicyChain(const std::string& policyKind)
{
    std::vector<std::unique_ptr<PartitionCommitPolicy>> policies;
    if (policyKind.empty()) {
        policies.push_back(std::make_unique<SuccessFileCommitPolicy>());
        return policies;
    }

    std::string kinds = policyKind;
    size_t pos = 0;
    while ((pos = kinds.find(',')) != std::string::npos) {
        std::string kind = kinds.substr(0, pos);
        addPolicy(policies, kind);
        kinds.erase(0, pos + 1);
    }
    addPolicy(policies, kinds);

    if (policies.empty()) {
        policies.push_back(std::make_unique<SuccessFileCommitPolicy>());
    }
    return policies;
}
