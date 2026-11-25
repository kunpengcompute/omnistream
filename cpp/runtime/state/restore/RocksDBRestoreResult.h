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

#ifndef OMNISTREAM_ROCKSDBRESTORERESULT_H
#define OMNISTREAM_ROCKSDBRESTORERESULT_H

#include "runtime/state/IncrementalKeyedStateHandle.h"
#include "runtime/state/UUID.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

class RocksDBRestoreResult {
public:
    RocksDBRestoreResult(
        rocksdb::DB* db,
        rocksdb::ColumnFamilyHandle* defaultColumnFamilyHandle,
        long lastCompletedCheckpointId,
        const UUID& backendUID,
        std::map<long, std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>> restoredSstFiles)
        : db(db),
          defaultColumnFamilyHandle(defaultColumnFamilyHandle),
          lastCompletedCheckpointId(lastCompletedCheckpointId),
          backendUID(backendUID),
          restoredSstFiles(std::move(restoredSstFiles)) {};

    rocksdb::DB* getDb()
    {
        return db;
    }

    long getLastCompletedCheckpointId()
    {
        return lastCompletedCheckpointId;
    }

    UUID getBackendUID()
    {
        return backendUID;
    }

    std::map<long, std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>> getRestoredSstFiles()
    {
        return restoredSstFiles;
    }

    rocksdb::ColumnFamilyHandle* getDefaultColumnFamilyHandle()
    {
        return defaultColumnFamilyHandle;
    };

private:
    rocksdb::DB* db;
    rocksdb::ColumnFamilyHandle* defaultColumnFamilyHandle;

    // fields only for incremental restore
    const int64_t lastCompletedCheckpointId;
    const UUID backendUID;
    std::map<long, std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>> restoredSstFiles;
};

#endif // OMNISTREAM_ROCKSDBRESTORERESULT_H
