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

#include <filesystem>
#include <memory>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/advanced_cache.h"
#include "core/api/common/state/MapState.h"
#include "typeutils/LongSerializer.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/rocksdb/RocksdbMapState.h"
#include "state/VoidNamespaceSerializer.h"

namespace {

std::string MakeDbPath(const std::string& name)
{
    auto now = std::chrono::system_clock::now();
    auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    return "/tmp/rocksdb_map_batch_ut/" + std::to_string(nanos) + "_" + name;
}

class RocksdbMapStateBatchTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        std::filesystem::create_directories("/tmp/rocksdb_map_batch_ut");
        dbPath_ = MakeDbPath(::testing::UnitTest::GetInstance()->current_test_info()->name());

        context_ = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 0), 1);
        namespaceSerializer_ = new VoidNamespaceSerializer();
        longSerializer_ = new LongSerializer();
        auto metaInfo =
            std::make_unique<RegisteredKeyValueStateBackendMetaInfo>("metaInfo", namespaceSerializer_, longSerializer_);
        table_ = std::make_unique<RocksdbMapStateTable<int64_t, VoidNamespace, int64_t, int64_t>>(
            context_, std::move(metaInfo), longSerializer_, longSerializer_);
        state_ = std::make_unique<RocksdbMapState<int64_t, VoidNamespace, int64_t, int64_t>>(
            table_.get(), longSerializer_, longSerializer_, namespaceSerializer_);

        ROCKSDB_NAMESPACE::Options options;
        options.create_if_missing = true;
        ROCKSDB_NAMESPACE::Status status = ROCKSDB_NAMESPACE::DB::Open(options, dbPath_, &rocksDb_);
        ASSERT_TRUE(status.ok()) << status.ToString();
        sharedCache_ = ROCKSDB_NAMESPACE::NewLRUCache(1024 * 1024);
        ROCKSDB_NAMESPACE::BlockBasedTableOptions blockOptions;
        blockOptions.block_cache = sharedCache_;
        blockOptions.cache_index_and_filter_blocks = true;
        blockOptions.cache_index_and_filter_blocks_with_high_priority = true;
        blockOptions.pin_l0_filter_and_index_blocks_in_cache = true;
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> kvStateInformation;
        kvStateInformation["RocksdbMapStateBatchTest"] = std::make_shared<RocksDbKvStateInfo>();
        kvStateInformation["RocksdbMapStateBatchTestvb"] = std::make_shared<RocksDbKvStateInfo>();
        state_->createTable(
            rocksDb_, "RocksdbMapStateBatchTest", &kvStateInformation, [blockOptions](const std::string&) {
                ROCKSDB_NAMESPACE::ColumnFamilyOptions options;
                options.table_factory.reset(ROCKSDB_NAMESPACE::NewBlockBasedTableFactory(blockOptions));
                return options;
            });
        for (const auto& entry : kvStateInformation) {
            auto options = rocksDb_->GetOptions(entry.second->columnFamilyHandle_);
            const auto* tableOptions = options.table_factory->GetOptions<ROCKSDB_NAMESPACE::BlockBasedTableOptions>();
            ASSERT_NE(tableOptions, nullptr);
            EXPECT_EQ(tableOptions->block_cache, sharedCache_);
            EXPECT_TRUE(tableOptions->cache_index_and_filter_blocks);
            EXPECT_TRUE(tableOptions->cache_index_and_filter_blocks_with_high_priority);
            EXPECT_TRUE(tableOptions->pin_l0_filter_and_index_blocks_in_cache);
        }
        context_->setCurrentKey(0);
    }

    void TearDown() override
    {
        state_.reset();
        table_.reset();
        if (rocksDb_ != nullptr) {
            rocksDb_->Close();
            rocksDb_ = nullptr;
        }
        std::filesystem::remove_all(dbPath_);
    }

    std::optional<int64_t> Read(int64_t key, int64_t userKey)
    {
        context_->setCurrentKey(key);
        return state_->get(userKey);
    }

    InternalKeyContextImpl<int64_t>* context_ = nullptr;
    VoidNamespaceSerializer* namespaceSerializer_ = nullptr;
    LongSerializer* longSerializer_ = nullptr;
    std::unique_ptr<RocksdbMapStateTable<int64_t, VoidNamespace, int64_t, int64_t>> table_;
    std::unique_ptr<RocksdbMapState<int64_t, VoidNamespace, int64_t, int64_t>> state_;
    ROCKSDB_NAMESPACE::DB* rocksDb_ = nullptr;
    std::string dbPath_;
    std::shared_ptr<ROCKSDB_NAMESPACE::Cache> sharedCache_;
};

TEST_F(RocksdbMapStateBatchTest, PutEntriesMatchesSinglePut)
{
    context_->setCurrentKey(1);
    state_->put(10, 100);

    std::vector<std::pair<int64_t, int64_t>> entries{{11, 110}, {10, 101}};
    state_->putEntries(entries);

    EXPECT_EQ(Read(1, 10), 101);
    EXPECT_EQ(Read(1, 11), 110);
    EXPECT_FALSE(Read(1, 12).has_value());
    EXPECT_FALSE(Read(2, 10).has_value());
}

TEST_F(RocksdbMapStateBatchTest, PutEntriesEmptyLeavesExistingValue)
{
    context_->setCurrentKey(1);
    state_->put(10, 100);
    state_->putEntries({});
    EXPECT_EQ(Read(1, 10), 100);
}

TEST_F(RocksdbMapStateBatchTest, PutEntriesLastPairWinsForSameUserKey)
{
    context_->setCurrentKey(1);
    state_->putEntries({{10, 1}, {10, 2}});
    EXPECT_EQ(Read(1, 10), 2);
}

TEST_F(RocksdbMapStateBatchTest, PutByBatchSingleKeyMatchesPut)
{
    context_->setCurrentKey(1);
    state_->put(10, 1);

    std::unordered_map<int64_t, int64_t> data{{10, 100}, {11, 110}};
    state_->putByBatch(7, data);

    EXPECT_EQ(Read(7, 10), 100);
    EXPECT_EQ(Read(7, 11), 110);
    EXPECT_EQ(Read(1, 10), 1);
}

TEST_F(RocksdbMapStateBatchTest, PutByBatchGroupedByKeyMatchesPut)
{
    context_->setCurrentKey(1);
    state_->put(10, 1);

    std::unordered_map<int64_t, std::unordered_map<int64_t, int64_t>> data;
    data[1] = {{10, 100}, {11, 110}};
    data[2] = {{20, 200}};
    state_->putByBatch(data);

    EXPECT_EQ(Read(1, 10), 100);
    EXPECT_EQ(Read(1, 11), 110);
    EXPECT_EQ(Read(2, 20), 200);
    EXPECT_FALSE(Read(2, 10).has_value());
}

TEST_F(RocksdbMapStateBatchTest, PutByBatchSharedPtrTuplesMatchesPut)
{
    std::vector<std::shared_ptr<std::tuple<int64_t, int64_t, int64_t>>> data;
    data.push_back(std::make_shared<std::tuple<int64_t, int64_t, int64_t>>(3, 30, 300));
    data.push_back(std::make_shared<std::tuple<int64_t, int64_t, int64_t>>(3, 31, 310));
    data.push_back(std::make_shared<std::tuple<int64_t, int64_t, int64_t>>(4, 30, 400));
    state_->putByBatch(data);

    EXPECT_EQ(Read(3, 30), 300);
    EXPECT_EQ(Read(3, 31), 310);
    EXPECT_EQ(Read(4, 30), 400);
    EXPECT_FALSE(Read(3, 32).has_value());
}

TEST_F(RocksdbMapStateBatchTest, PutByBatchTuplesLastValueWins)
{
    std::vector<std::tuple<int64_t, int64_t, int64_t>> data;
    data.emplace_back(5, 50, 500);
    data.emplace_back(5, 50, 501);
    data.emplace_back(6, 60, 600);
    state_->putByBatch(data);

    EXPECT_EQ(Read(5, 50), 501);
    EXPECT_EQ(Read(6, 60), 600);
}

TEST_F(RocksdbMapStateBatchTest, RemoveEntriesRemovesOnlyListedUserKeys)
{
    context_->setCurrentKey(1);
    state_->putEntries({{10, 100}, {11, 110}, {12, 120}});
    state_->removeEntries({10, 12});

    EXPECT_FALSE(Read(1, 10).has_value());
    EXPECT_EQ(Read(1, 11), 110);
    EXPECT_FALSE(Read(1, 12).has_value());
}

TEST_F(RocksdbMapStateBatchTest, RemoveEntriesEmptyLeavesExistingValue)
{
    context_->setCurrentKey(1);
    state_->put(10, 100);
    state_->removeEntries({});
    EXPECT_EQ(Read(1, 10), 100);
}

TEST_F(RocksdbMapStateBatchTest, RemoveByBatchRemovesAcrossKeys)
{
    context_->setCurrentKey(1);
    state_->put(10, 100);
    state_->put(11, 110);
    context_->setCurrentKey(2);
    state_->put(20, 200);
    state_->put(21, 210);

    std::unordered_map<int64_t, std::unordered_set<int64_t>> toRemove;
    toRemove[1] = {10};
    toRemove[2] = {20};
    state_->removeByBatch(toRemove);

    EXPECT_FALSE(Read(1, 10).has_value());
    EXPECT_EQ(Read(1, 11), 110);
    EXPECT_FALSE(Read(2, 20).has_value());
    EXPECT_EQ(Read(2, 21), 210);
}

} // namespace
