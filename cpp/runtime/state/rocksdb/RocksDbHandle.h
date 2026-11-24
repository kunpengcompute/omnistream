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

#ifndef OMNISTREAM_ROCKSDBHANDLE_H
#define OMNISTREAM_ROCKSDBHANDLE_H

#pragma once

#include <stdexcept>
#include <memory>
#include <string>
#include <vector>
#include <filesystem>
#include <iostream>
#include <utility>

#include "common.h"
#include "runtime/state/RegisteredStateMetaInfoBase.h"
#include "runtime/state/rocksdb/RocksDbOperationUtils.h"
#include "runtime/state/rocksdb/RocksDbStringAppendOperator.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/RocksDbKvStateInfo.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace fs = std::filesystem;

class RocksDbHandle {
public:
    RocksDbHandle(
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        const fs::path& instanceRocksDBPath,
        std::shared_ptr<rocksdb::DBOptions> dbOptions,
        std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory)
        : kvStateInformation(kvStateInformation),
          dbPath(instanceRocksDBPath.string()),
          dbOptions(std::move(dbOptions)),
          columnFamilyOptionsFactory(std::move(columnFamilyOptionsFactory))
    {
        columnFamilyHandles.reserve(1);
    }

    void openDB()
    {
        loadDb();
    }

    void openDB(
        const std::vector<rocksdb::ColumnFamilyDescriptor>& columnFamilyDescriptors,
        const std::vector<StateMetaInfoSnapshot>& stateMetaInfoSnapshots,
        const fs::path& restoreSourcePath)
    {
        if (columnFamilyDescriptors.size() != stateMetaInfoSnapshots.size()) {
            throw std::invalid_argument(
                "RocksDbHandle::openDB descriptor count " + std::to_string(columnFamilyDescriptors.size()) +
                " does not match snapshot count " + std::to_string(stateMetaInfoSnapshots.size()));
        }
        this->columnFamilyDescriptors = columnFamilyDescriptors;
        restoreInstanceDirectoryFromPath(restoreSourcePath);
        loadDb();

        for (size_t i = 0; i < stateMetaInfoSnapshots.size(); i++) {
            getOrRegisterStateColumnFamilyHandle(columnFamilyHandles[i], stateMetaInfoSnapshots[i]);
        }
    }

    rocksdb::DB* getDb()
    {
        return db;
    }

    rocksdb::ColumnFamilyHandle* getDefaultColumnFamilyHandle()
    {
        return defaultColumnFamilyHandle;
    };
    std::vector<rocksdb::ColumnFamilyHandle*> getColumnFamilyHandles()
    {
        return columnFamilyHandles;
    }

    std::function<rocksdb::ColumnFamilyOptions(const std::string&)> getColumnFamilyOptionsFactory()
    {
        return columnFamilyOptionsFactory;
    }

    std::shared_ptr<rocksdb::DBOptions> getDbOptions()
    {
        return dbOptions;
    }

    std::shared_ptr<RocksDbKvStateInfo> getOrRegisterStateColumnFamilyHandle(
        rocksdb::ColumnFamilyHandle* columnFamilyHandle, const StateMetaInfoSnapshot& stateMetaInfoSnapshot)
    {
        std::shared_ptr<RocksDbKvStateInfo> registeredStateMetaInfoEntry = nullptr;
        // Check if the state already exists in the map
        auto it = kvStateInformation->find(stateMetaInfoSnapshot.getName());
        if (it != kvStateInformation->end()) {
            registeredStateMetaInfoEntry = it->second;
        }

        if (nullptr == registeredStateMetaInfoEntry) {
            std::shared_ptr<RegisteredStateMetaInfoBase> stateMetaInfo =
                RegisteredStateMetaInfoBase::fromMetaInfoSnapshot(stateMetaInfoSnapshot);
            if (columnFamilyHandle == nullptr) {
                auto columnFamilyDescriptor =
                    RocksDbOperationUtils::createColumnFamilyDescriptor(stateMetaInfo, columnFamilyOptionsFactory);

                // LIST state uses Merge operations; ensure merge_operator is set when opening an
                // existing DB, otherwise RocksDB will reject the column family with "Invalid argument:
                // merge_operator_ must be set."
                auto stateTypeString =
                    stateMetaInfoSnapshot.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
                if (stateTypeString == "LIST") {
                    columnFamilyDescriptor.options.merge_operator.reset(new RocksDbStringAppendOperator(','));
                }

                columnFamilyHandle = RocksDbOperationUtils::createColumnFamily(columnFamilyDescriptor, db);
                PendingColumnFamilyHandleGuard pendingColumnFamilyHandle(db, columnFamilyHandle, dbPath);
                columnFamilyHandles.push_back(columnFamilyHandle);
                pendingColumnFamilyHandle.release();
                registeredStateMetaInfoEntry =
                    std::make_shared<RocksDbKvStateInfo>(columnFamilyHandle, std::move(stateMetaInfo));
            } else {
                registeredStateMetaInfoEntry =
                    std::make_shared<RocksDbKvStateInfo>(columnFamilyHandle, std::move(stateMetaInfo));
            }
            RocksDbOperationUtils::registerKvStateInformation(
                kvStateInformation, stateMetaInfoSnapshot.getName(), registeredStateMetaInfoEntry);
        }
        return registeredStateMetaInfoEntry;
    }

    void closeOpenDbNoThrow() noexcept
    {
        if (db == nullptr) {
            defaultColumnFamilyHandle = nullptr;
            columnFamilyHandles.clear();
            return;
        }

        size_t stateHandleIndex = 0;
        for (auto* stateColumnFamilyHandle : columnFamilyHandles) {
            if (stateColumnFamilyHandle != nullptr) {
                const auto status = db->DestroyColumnFamilyHandle(stateColumnFamilyHandle);
                if (!status.ok()) {
                    INFO_RELEASE(
                        "Error:RocksDbHandle::closeOpenDbNoThrow failed to destroy state column family handle, dbPath="
                        << dbPath << ", stateHandleIndex=" << stateHandleIndex << ", status=" << status.ToString());
                }
            }
            ++stateHandleIndex;
        }
        columnFamilyHandles.clear();

        if (defaultColumnFamilyHandle != nullptr) {
            const auto status = db->DestroyColumnFamilyHandle(defaultColumnFamilyHandle);
            if (!status.ok()) {
                INFO_RELEASE(
                    "Error:RocksDbHandle::closeOpenDbNoThrow failed to destroy default column family handle, dbPath="
                    << dbPath << ", status=" << status.ToString());
            }
            defaultColumnFamilyHandle = nullptr;
        }

        const auto status = db->Close();
        if (!status.ok()) {
            INFO_RELEASE(
                "Error:RocksDbHandle::closeOpenDbNoThrow failed to close DB, dbPath=" << dbPath << ", status="
                                                                                      << status.ToString());
        }
        delete db;
        db = nullptr;
    }

    void restoreInstanceDirectoryFromPath(const fs::path& source)
    {
        fs::path instanceRocksDBDirectory(dbPath);
        std::error_code ec;
        bool created = fs::create_directories(instanceRocksDBDirectory, ec);
        if (ec || !created) {
            std::string errMsg = "Could not create RocksDB data directory: " + instanceRocksDBDirectory.string();
            std::cerr << "ERROR: " << errMsg << std::endl;
            throw std::runtime_error(errMsg);
        }

        std::vector<fs::directory_entry> entries;
        try {
            for (const auto& entry : fs::directory_iterator(source)) {
                entries.push_back(entry);
            }
        } catch (const fs::filesystem_error& ex) {
            std::string errMsg = "Could not list directory: " + source.string() + ", error: " + ex.what();
            std::cerr << "ERROR: " + errMsg << std::endl;
            throw;
        }
        for (const auto& entry : entries) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const fs::path& file = entry.path();
            const std::string fileName = file.filename().string();
            const fs::path targetFile = instanceRocksDBDirectory / fileName;

            bool hardLinkSuccess = false;

            if (endsWithSst(fileName)) {
                try {
                    fs::create_hard_link(file, targetFile);
                    hardLinkSuccess = true;
                    continue;
                } catch (const fs::filesystem_error& ex) {
                    std::string logMessage = "Could not hard link sst file " + fileName;
                    std::cout << "INFO: " << logMessage << std::endl;
                }
            }
            std::cout << "hardLinkSuccess state: " << hardLinkSuccess << std::endl;
            try {
                fs::copy_file(file, targetFile, fs::copy_options::overwrite_existing);
            } catch (const fs::filesystem_error& ex) {
                std::string errMsg = "Failed to copy file from " + file.string() + " to " + targetFile.string() +
                                     ", error: " + ex.what();
                std::cerr << "ERROR: " << errMsg << std::endl;
                throw;
            }
        }
    }

private:
    class PendingColumnFamilyHandleGuard {
    public:
        PendingColumnFamilyHandleGuard(
            rocksdb::DB* db, rocksdb::ColumnFamilyHandle* columnFamilyHandle, const std::string& dbPath)
            : db(db),
              columnFamilyHandle(columnFamilyHandle),
              dbPath(dbPath)
        {
        }

        ~PendingColumnFamilyHandleGuard() noexcept
        {
            if (db == nullptr || columnFamilyHandle == nullptr) {
                return;
            }
            const auto status = db->DestroyColumnFamilyHandle(columnFamilyHandle);
            if (!status.ok()) {
                INFO_RELEASE(
                    "Error:RocksDbHandle pending state column family cleanup failed, dbPath=" << dbPath << ", status="
                                                                                              << status.ToString());
            }
        }

        PendingColumnFamilyHandleGuard(const PendingColumnFamilyHandleGuard&) = delete;
        PendingColumnFamilyHandleGuard& operator=(const PendingColumnFamilyHandleGuard&) = delete;
        PendingColumnFamilyHandleGuard(PendingColumnFamilyHandleGuard&& other) noexcept
            : db(std::exchange(other.db, nullptr)),
              columnFamilyHandle(std::exchange(other.columnFamilyHandle, nullptr)),
              dbPath(other.dbPath)
        {
        }
        PendingColumnFamilyHandleGuard& operator=(PendingColumnFamilyHandleGuard&&) = delete;

        void release() noexcept
        {
            columnFamilyHandle = nullptr;
        }

    private:
        rocksdb::DB* db;
        rocksdb::ColumnFamilyHandle* columnFamilyHandle;
        const std::string& dbPath;
    };

    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation;
    const std::string dbPath;
    std::shared_ptr<rocksdb::DBOptions> dbOptions;
    const std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory;
    std::vector<rocksdb::ColumnFamilyHandle*> columnFamilyHandles;
    std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyDescriptors;
    rocksdb::DB* db = nullptr;
    rocksdb::ColumnFamilyHandle* defaultColumnFamilyHandle = nullptr;
    const std::string SST_FILE_SUFFIX = ".sst";
    std::string::size_type SST_SUFFIX_LENGTH = 4;
    void loadDb()
    {
        rocksdb::ColumnFamilyOptions columnFamilyOptions =
            RocksDbOperationUtils::createColumnFamilyOptions(columnFamilyOptionsFactory, "default");
        std::vector<rocksdb::ColumnFamilyHandle*> openedColumnFamilyHandles;
        auto* openedDb = RocksDbOperationUtils::openDB(
            dbPath, columnFamilyDescriptors, openedColumnFamilyHandles, columnFamilyOptions, *dbOptions);
        auto* openedDefaultColumnFamilyHandle = openedColumnFamilyHandles.front();
        openedColumnFamilyHandles.erase(openedColumnFamilyHandles.begin());

        db = openedDb;
        defaultColumnFamilyHandle = openedDefaultColumnFamilyHandle;
        columnFamilyHandles.swap(openedColumnFamilyHandles);
    }

    bool endsWithSst(const std::string& str)
    {
        if (str.length() < SST_SUFFIX_LENGTH) {
            return false;
        }
        return str.substr(str.length() - SST_SUFFIX_LENGTH) == SST_FILE_SUFFIX;
    }
};
#endif // OMNISTREAM_ROCKSDBHANDLE_H
