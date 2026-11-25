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

#ifndef PARTITION_COMMIT_POLICY_H
#define PARTITION_COMMIT_POLICY_H

#include <string>
#include <vector>
#include <map>
#include <memory>
#include "FileSystemCommitter.h"

/**
 * Context passed to partition commit policies.
 */
class PolicyContext {
public:
    PolicyContext(
        const std::string& catalogName,
        const std::string& databaseName,
        const std::string& tableName,
        const std::vector<std::string>& partitionKeys,
        const std::vector<std::string>& partitionValues,
        const std::string& partitionPath)
        : catalogName_(catalogName),
          databaseName_(databaseName),
          tableName_(tableName),
          partitionKeys_(partitionKeys),
          partitionValues_(partitionValues),
          partitionPath_(partitionPath)
    {
    }

    const std::string& catalogName() const
    {
        return catalogName_;
    }
    const std::string& databaseName() const
    {
        return databaseName_;
    }
    const std::string& tableName() const
    {
        return tableName_;
    }
    const std::vector<std::string>& partitionKeys() const
    {
        return partitionKeys_;
    }
    const std::vector<std::string>& partitionValues() const
    {
        return partitionValues_;
    }
    const std::string& partitionPath() const
    {
        return partitionPath_;
    }

    std::map<std::string, std::string> partitionSpec() const
    {
        std::map<std::string, std::string> spec;
        for (size_t i = 0; i < partitionKeys_.size() && i < partitionValues_.size(); ++i) {
            spec[partitionKeys_[i]] = partitionValues_[i];
        }
        return spec;
    }

private:
    std::string catalogName_;
    std::string databaseName_;
    std::string tableName_;
    std::vector<std::string> partitionKeys_;
    std::vector<std::string> partitionValues_;
    std::string partitionPath_;
};

/**
 * Interface for a partition commit policy.
 * Implementations must be idempotent - the same partition may be committed multiple times.
 */
class PartitionCommitPolicy {
public:
    virtual ~PartitionCommitPolicy() = default;

    virtual void commit(const PolicyContext& context) = 0;

    static constexpr const char* METASTORE = "metastore";
    static constexpr const char* SUCCESS_FILE = "success-file";
    static constexpr const char* CUSTOM = "custom";
};

/**
 * Policy that creates a _SUCCESS file in the partition directory to mark it as committed.
 */
class SuccessFileCommitPolicy : public PartitionCommitPolicy {
public:
    void commit(const PolicyContext& context) override
    {
        FileSystemCommitter::createSuccessFile(context.partitionPath());
    }
};

/**
 * Policy that notifies a metastore about the committed partition.
 * In a filesystem-only setup, this is a no-op.
 */
class MetastoreCommitPolicy : public PartitionCommitPolicy {
public:
    void commit(const PolicyContext& context) override
    {
    }
};

/**
 * Creates a chain of commit policies from the policy kind string.
 * Supported kinds (comma-separated): "success-file", "metastore", "custom"
 */
std::vector<std::unique_ptr<PartitionCommitPolicy>> createPolicyChain(const std::string& policyKind);

#endif // PARTITION_COMMIT_POLICY_H
