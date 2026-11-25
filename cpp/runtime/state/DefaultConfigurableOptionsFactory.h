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
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "include/functions/Configuration.h"
#include "core/configuration/MemorySize.h"
#include "RocksDBConfigurableOptions.h"

class DefaultConfigurableOptionsFactory {
public:
    static void createColumnOptions(
        ROCKSDB_NAMESPACE::ColumnFamilyOptions& currentOptions,
        ROCKSDB_NAMESPACE::BlockBasedTableOptions& blockBasedTableOptions);
    static void createDBOptions(ROCKSDB_NAMESPACE::DBOptions& currentOptions);
};
