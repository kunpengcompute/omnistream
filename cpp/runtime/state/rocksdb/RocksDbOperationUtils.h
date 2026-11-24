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

#include <string>

#include "executiongraph/TaskInformationPOD.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/RegisteredStateMetaInfoBase.h"
#include "runtime/state/RocksIteratorWrapper.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace fs = std::filesystem;

class RocksDbOperationUtils {
public:
    static rocksdb::DB* openDB(
        const std::string& path,
        const std::vector<rocksdb::ColumnFamilyDescriptor>& stateColumnFamilyDescriptors,
        std::vector<rocksdb::ColumnFamilyHandle*>& stateColumnFamilyHandles,
        const rocksdb::ColumnFamilyOptions& columnFamilyOptions,
        const rocksdb::DBOptions& dbOptions)
    {
        std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyDescriptors;
        columnFamilyDescriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, columnFamilyOptions);
        columnFamilyDescriptors.insert(
            columnFamilyDescriptors.end(), stateColumnFamilyDescriptors.begin(), stateColumnFamilyDescriptors.end());
        rocksdb::DB* db;
        rocksdb::Status status =
            rocksdb::DB::Open(dbOptions, path, columnFamilyDescriptors, &stateColumnFamilyHandles, &db);
        if (!status.ok()) {
            for (auto* handle : stateColumnFamilyHandles) {
                delete handle;
            }
            stateColumnFamilyHandles.clear();
            throw std::runtime_error("rocksdb open error");
        }

        if (1 + stateColumnFamilyDescriptors.size() != stateColumnFamilyHandles.size()) {
            delete db;
            for (auto* handle : stateColumnFamilyHandles) {
                delete handle;
            }
            stateColumnFamilyHandles.clear();

            throw std::runtime_error("Not all requested column family handles have been created");
        }

        return db;
    }

    static std::unique_ptr<RocksIteratorWrapper> getRocksIterator(
        rocksdb::DB* db, rocksdb::ColumnFamilyHandle* columnFamilyHandle, const rocksdb::ReadOptions& readOptions)
    {
        auto itPtr = db->NewIterator(readOptions, columnFamilyHandle);
        std::unique_ptr<rocksdb::Iterator> itUniqueptr(itPtr);
        return std::make_unique<RocksIteratorWrapper>(std::move(itUniqueptr));
    }

    static void registerKvStateInformation(
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        const std::string& columnFamilyName,
        std::shared_ptr<RocksDbKvStateInfo> registeredColumn)
    {
        kvStateInformation->emplace(columnFamilyName, registeredColumn);
    }

    static std::shared_ptr<RocksDbKvStateInfo> createStateInfo(
        std::shared_ptr<RegisteredStateMetaInfoBase> metaInfoBase,
        rocksdb::DB* db,
        std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory)
    {
        rocksdb::ColumnFamilyDescriptor columnFamilyDescriptor =
            createColumnFamilyDescriptor(metaInfoBase, columnFamilyOptionsFactory);
        rocksdb::ColumnFamilyHandle* handle = createColumnFamily(columnFamilyDescriptor, db);
        return std::make_shared<RocksDbKvStateInfo>(handle, metaInfoBase);
    }

    static rocksdb::ColumnFamilyDescriptor createColumnFamilyDescriptor(
        std::shared_ptr<RegisteredStateMetaInfoBase> metaInfoBase,
        std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory)
    {
        auto options = createColumnFamilyOptions(columnFamilyOptionsFactory, metaInfoBase->getName());
        const std::string& nameStr = metaInfoBase->getName();
        return rocksdb::ColumnFamilyDescriptor(nameStr, options);
    }

    static rocksdb::ColumnFamilyOptions createColumnFamilyOptions(
        std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory,
        const std::string& stateName)
    {
        auto columnFamilyOptions = columnFamilyOptionsFactory(stateName);
        return columnFamilyOptions;
    }

    static rocksdb::ColumnFamilyHandle* createColumnFamily(
        rocksdb::ColumnFamilyDescriptor& columnDescriptor, rocksdb::DB* db)
    {
        rocksdb::ColumnFamilyHandle* handle = nullptr;
        rocksdb::Status status = db->CreateColumnFamily(columnDescriptor.options, columnDescriptor.name, &handle);
        if (!status.ok()) {
            throw std::runtime_error("Error creating ColumnFamilyHandle.");
        }
        return handle;
    }
};
