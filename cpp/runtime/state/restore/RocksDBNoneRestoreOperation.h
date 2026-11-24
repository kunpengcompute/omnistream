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

#ifndef OMNISTREAM_ROCKSDBNONERESTOREOPERATION_H
#define OMNISTREAM_ROCKSDBNONERESTOREOPERATION_H

#include "runtime/state/restore/RocksDBRestoreOperation.h"
#include "runtime/state/restore/RocksDBRestoreResult.h"
#include "runtime/state/rocksdb/RocksDbHandle.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "streaming/runtime/metrics/MetricGroup.h"
#include "runtime/state/UUID.h"

class RocksDBNoneRestoreOperation : public RocksDBRestoreOperation {
public:
    RocksDBNoneRestoreOperation(
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        std::filesystem::path& instanceRocksDBPath,
        std::shared_ptr<rocksdb::DBOptions> dbOptions,
        std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory)
    {
        this->rocksHandle = std::make_unique<RocksDbHandle>(
            kvStateInformation, instanceRocksDBPath, dbOptions, columnFamilyOptionsFactory);
    }

    ~RocksDBNoneRestoreOperation() = default;

    std::shared_ptr<RocksDBRestoreResult> restore() override
    {
        rocksHandle->openDB();
        return std::make_unique<RocksDBRestoreResult>(
            rocksHandle->getDb(),
            rocksHandle->getDefaultColumnFamilyHandle(),
            -1,
            UUID(),
            std::map<long, std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>>());
    }

private:
    std::unique_ptr<RocksDbHandle> rocksHandle;
};

#endif // OMNISTREAM_ROCKSDBNONERESTOREOPERATION_H
