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
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/utils/ByteView.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RocksDBRestoreKVState.h"
#include "runtime/state/restore/RocksDBRestorePQState.h"
#include "runtime/state/rocksdb/RocksDbHandle.h"

namespace {

StateMetaInfoSnapshot makeMetaInfo(const std::string& name, StateMetaInfoSnapshot::BackendStateType type)
{
    return StateMetaInfoSnapshot(
        name,
        type,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
}

class RocksDBRestoreStateWriterTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        const auto* testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
        dbPath_ =
            std::filesystem::temp_directory_path() /
            ("rocks-restore-state-writer-" + std::string(testInfo->name()) + "-" + std::to_string(nextDbPathId_++));
        std::filesystem::remove_all(dbPath_);

        auto dbOptions = std::make_shared<rocksdb::DBOptions>();
        dbOptions->create_if_missing = true;
        rocksDbHandle_ = std::make_unique<RocksDbHandle>(
            &kvStateInformation_, dbPath_, std::move(dbOptions), [](const std::string&) {
                return rocksdb::ColumnFamilyOptions();
            });
        rocksDbHandle_->openDB();

        auto mainState = rocksDbHandle_->getOrRegisterStateColumnFamilyHandle(
            nullptr, makeMetaInfo("main", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE));
        auto pqState = rocksDbHandle_->getOrRegisterStateColumnFamilyHandle(
            nullptr, makeMetaInfo("pq", StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE));
        mainCf_ = mainState->columnFamilyHandle_;
        pqCf_ = pqState->columnFamilyHandle_;
    }

    void TearDown() override
    {
        kvStateInformation_.clear();
        if (rocksDbHandle_ != nullptr) {
            rocksDbHandle_->closeOpenDbNoThrow();
        }
        rocksDbHandle_.reset();
        std::filesystem::remove_all(dbPath_);
    }

    rocksdb::Status get(
        rocksdb::ColumnFamilyHandle* columnFamily, const std::vector<int8_t>& key, std::string* value) const
    {
        return rocksDbHandle_->getDb()->Get(
            rocksdb::ReadOptions(),
            columnFamily,
            rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size()),
            value);
    }

    omnistream::RocksDBWriterContext makeContext(int64_t* mainEntryCount)
    {
        return {rocksDbHandle_->getDb(), 2 * 1024 * 1024, 1, 0, -1, mainEntryCount};
    }

    std::filesystem::path dbPath_;
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> kvStateInformation_;
    std::unique_ptr<RocksDbHandle> rocksDbHandle_;
    rocksdb::ColumnFamilyHandle* mainCf_ = nullptr;
    rocksdb::ColumnFamilyHandle* pqCf_ = nullptr;
    inline static std::atomic<unsigned long> nextDbPathId_{0};
};

} // namespace

TEST_F(RocksDBRestoreStateWriterTest, ByteViewEntryPreservesEmbeddedZeroBytes)
{
    int64_t mainEntryCount = 0;
    auto context = makeContext(&mainEntryCount);
    const std::vector<int8_t> key{3, 2, 1};
    const std::vector<int8_t> value{0, 9, 0, 8, 0};

    omnistream::RocksDBRestoreKVState<int> writer(context, mainCf_, 4);
    writer.writeEntry(key, ByteView::fromBuffer(value.data(), value.size()));
    writer.flush();

    std::string restored;
    ASSERT_TRUE(get(mainCf_, key, &restored).ok());
    EXPECT_EQ(std::vector<int8_t>(restored.begin(), restored.end()), value);
    EXPECT_EQ(mainEntryCount, 1);
}

TEST_F(RocksDBRestoreStateWriterTest, EmptyByteViewIsStoredAsEmptyValueWithoutStatisticsPointer)
{
    auto context = makeContext(nullptr);
    const std::vector<int8_t> key{7};

    omnistream::RocksDBRestoreKVState<int> writer(context, mainCf_, 5);
    writer.writeEntry(key, ByteView());
    writer.flush();

    std::string restored = "not-empty";
    ASSERT_TRUE(get(mainCf_, key, &restored).ok());
    EXPECT_TRUE(restored.empty());
}

TEST_F(RocksDBRestoreStateWriterTest, EmptyWriterFlushAndDiscardAreSafe)
{
    auto context = makeContext(nullptr);
    omnistream::RocksDBRestoreKVState<int> writer(context, mainCf_, 6);

    EXPECT_NO_THROW(writer.flush());
    EXPECT_NO_THROW(writer.discard());
}

TEST_F(RocksDBRestoreStateWriterTest, DiscardDropsPendingKeyValueEntry)
{
    auto context = makeContext(nullptr);
    const std::vector<int8_t> key{6, 6};
    const std::vector<int8_t> value{7, 7};
    omnistream::RocksDBRestoreKVState<int> writer(context, mainCf_, 6);

    writer.writeEntry(key, ByteView::fromBuffer(value.data(), value.size()));
    writer.discard();
    EXPECT_NO_THROW(writer.discard());

    std::string restored;
    EXPECT_TRUE(get(mainCf_, key, &restored).IsNotFound());
}

TEST_F(RocksDBRestoreStateWriterTest, SetKeyGroupIdUpdatesSharedWriterContext)
{
    auto context = makeContext(nullptr);
    omnistream::RocksDBRestoreKVState<int> writer(context, mainCf_, 7);

    writer.setKeyGroupId(19);

    EXPECT_EQ(context.keyGroupId, 19);
}

TEST_F(RocksDBRestoreStateWriterTest, PriorityQueueEntriesPreserveKeysAndValues)
{
    const std::vector<int8_t> firstKey{0, 1, 0};
    const std::vector<int8_t> firstValue{4, 0, 5};
    const std::vector<int8_t> secondKey{2};
    const std::vector<int8_t> secondValue{};

    omnistream::RocksDBRestorePQState writer(rocksDbHandle_.get(), pqCf_, 2 * 1024 * 1024);
    writer.writeEntry(firstKey, firstValue);
    writer.writeEntry(secondKey, secondValue);
    writer.flush();

    std::string restored;
    ASSERT_TRUE(get(pqCf_, firstKey, &restored).ok());
    EXPECT_EQ(std::vector<int8_t>(restored.begin(), restored.end()), firstValue);
    ASSERT_TRUE(get(pqCf_, secondKey, &restored).ok());
    EXPECT_TRUE(restored.empty());
}

TEST_F(RocksDBRestoreStateWriterTest, EmptyPriorityQueueWriterFlushAndDiscardAreSafe)
{
    omnistream::RocksDBRestorePQState writer(rocksDbHandle_.get(), pqCf_, 2 * 1024 * 1024);

    EXPECT_NO_THROW(writer.flush());
    EXPECT_NO_THROW(writer.discard());
}

TEST_F(RocksDBRestoreStateWriterTest, DiscardDropsPendingPriorityQueueEntry)
{
    const std::vector<int8_t> key{9, 9};
    omnistream::RocksDBRestorePQState writer(rocksDbHandle_.get(), pqCf_, 2 * 1024 * 1024);

    writer.writeEntry(key, {1, 2, 3});
    writer.discard();
    EXPECT_NO_THROW(writer.discard());

    std::string restored;
    EXPECT_TRUE(get(pqCf_, key, &restored).IsNotFound());
}
