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

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/rocksdb/RocksDbHandle.h"

namespace {

StateMetaInfoSnapshot makeMetaInfo(const std::string& stateName)
{
    return StateMetaInfoSnapshot(
        stateName,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
}

class RocksDbHandleTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        const auto* testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
        dbPath_ = std::filesystem::temp_directory_path() /
                  ("rocks-handle-" + std::string(testInfo->test_suite_name()) + "-" + std::string(testInfo->name()) +
                   "-" + std::to_string(nextDbPathId_++));
        std::filesystem::remove_all(dbPath_);

        dbOptions_ = std::make_shared<rocksdb::DBOptions>();
        dbOptions_->create_if_missing = true;
    }

    void TearDown() override
    {
        std::filesystem::remove_all(dbPath_);
    }

    std::unique_ptr<RocksDbHandle> makeHandle()
    {
        return std::make_unique<RocksDbHandle>(&kvStateInformation_, dbPath_, dbOptions_, [](const std::string&) {
            return rocksdb::ColumnFamilyOptions();
        });
    }

    void expectPathCanBeOpenedAgain(const std::vector<std::string>& stateColumnFamilies)
    {
        std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
        descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
        for (const auto& stateName : stateColumnFamilies) {
            descriptors.emplace_back(stateName, rocksdb::ColumnFamilyOptions());
        }

        rocksdb::DBOptions reopenOptions;
        reopenOptions.create_if_missing = true;
        rocksdb::DB* reopenedDb = nullptr;
        std::vector<rocksdb::ColumnFamilyHandle*> reopenedHandles;
        auto status = rocksdb::DB::Open(reopenOptions, dbPath_.string(), descriptors, &reopenedHandles, &reopenedDb);

        ASSERT_TRUE(status.ok()) << "Failed to reopen RocksDB path: " << status.ToString();
        ASSERT_NE(reopenedDb, nullptr);
        ASSERT_EQ(reopenedHandles.size(), descriptors.size());

        for (auto it = reopenedHandles.rbegin(); it != reopenedHandles.rend(); ++it) {
            status = reopenedDb->DestroyColumnFamilyHandle(*it);
            EXPECT_TRUE(status.ok()) << "Failed to destroy reopened column family handle: " << status.ToString();
        }
        status = reopenedDb->Close();
        EXPECT_TRUE(status.ok()) << "Failed to close reopened RocksDB: " << status.ToString();
        delete reopenedDb;
    }

    std::filesystem::path dbPath_;
    std::shared_ptr<rocksdb::DBOptions> dbOptions_;
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> kvStateInformation_;
    inline static std::atomic<unsigned long> nextDbPathId_{0};
};

} // namespace

TEST_F(RocksDbHandleTest, CloseOpenDbNoThrowReleasesStateDefaultCfAndDbLock)
{
    auto handle = makeHandle();
    handle->openDB();
    auto stateInfo = handle->getOrRegisterStateColumnFamilyHandle(nullptr, makeMetaInfo("created"));

    ASSERT_NE(stateInfo, nullptr);
    ASSERT_NE(stateInfo->columnFamilyHandle_, nullptr);
    ASSERT_EQ(kvStateInformation_.size(), 1U);
    ASSERT_EQ(handle->getColumnFamilyHandles().size(), 1U);

    kvStateInformation_.clear();
    handle->closeOpenDbNoThrow();

    EXPECT_EQ(handle->getDb(), nullptr);
    EXPECT_EQ(handle->getDefaultColumnFamilyHandle(), nullptr);
    EXPECT_TRUE(handle->getColumnFamilyHandles().empty());
    expectPathCanBeOpenedAgain({"created"});
}

TEST_F(RocksDbHandleTest, CloseOpenDbNoThrowIsIdempotent)
{
    auto handle = makeHandle();
    handle->openDB();

    handle->closeOpenDbNoThrow();
    EXPECT_NO_THROW(handle->closeOpenDbNoThrow());

    EXPECT_EQ(handle->getDb(), nullptr);
    EXPECT_EQ(handle->getDefaultColumnFamilyHandle(), nullptr);
    EXPECT_TRUE(handle->getColumnFamilyHandles().empty());
    expectPathCanBeOpenedAgain({});
}

TEST_F(RocksDbHandleTest, CloseOpenDbNoThrowBeforeOpenIsNoOp)
{
    auto handle = makeHandle();

    EXPECT_NO_THROW(handle->closeOpenDbNoThrow());

    EXPECT_EQ(handle->getDb(), nullptr);
    EXPECT_EQ(handle->getDefaultColumnFamilyHandle(), nullptr);
    EXPECT_TRUE(handle->getColumnFamilyHandles().empty());
    EXPECT_TRUE(kvStateInformation_.empty());
}

TEST_F(RocksDbHandleTest, CloseOpenDbNoThrowReleasesMultipleStateColumnFamilies)
{
    auto handle = makeHandle();
    handle->openDB();
    handle->getOrRegisterStateColumnFamilyHandle(nullptr, makeMetaInfo("state-a"));
    handle->getOrRegisterStateColumnFamilyHandle(nullptr, makeMetaInfo("state-b"));

    ASSERT_EQ(kvStateInformation_.size(), 2U);
    ASSERT_EQ(handle->getColumnFamilyHandles().size(), 2U);

    kvStateInformation_.clear();
    handle->closeOpenDbNoThrow();

    EXPECT_EQ(handle->getDb(), nullptr);
    EXPECT_EQ(handle->getDefaultColumnFamilyHandle(), nullptr);
    EXPECT_TRUE(handle->getColumnFamilyHandles().empty());
    expectPathCanBeOpenedAgain({"state-a", "state-b"});
}

TEST_F(RocksDbHandleTest, OpenDbRejectsDescriptorSnapshotCountMismatchBeforeRestoringFiles)
{
    auto handle = makeHandle();
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    descriptors.emplace_back("state", rocksdb::ColumnFamilyOptions());
    std::vector<StateMetaInfoSnapshot> snapshots;
    const auto missingRestoreSource = dbPath_ / "missing-restore-source";

    ASSERT_FALSE(std::filesystem::exists(missingRestoreSource));
    bool sawExpectedException = false;
    try {
        handle->openDB(descriptors, snapshots, missingRestoreSource);
    } catch (const std::invalid_argument& exception) {
        sawExpectedException = true;
        const std::string message = exception.what();
        EXPECT_NE(message.find("descriptor count 1"), std::string::npos);
        EXPECT_NE(message.find("snapshot count 0"), std::string::npos);
    } catch (const std::exception& exception) {
        ADD_FAILURE() << "Unexpected exception type with message: " << exception.what();
    } catch (...) {
        ADD_FAILURE() << "Unexpected non-standard exception";
    }

    EXPECT_TRUE(sawExpectedException);
    EXPECT_FALSE(std::filesystem::exists(dbPath_));
    EXPECT_EQ(handle->getDb(), nullptr);
    EXPECT_EQ(handle->getDefaultColumnFamilyHandle(), nullptr);
    EXPECT_TRUE(handle->getColumnFamilyHandles().empty());
    EXPECT_TRUE(kvStateInformation_.empty());
}
