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
#include "runtime/state/heap/HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "runtime/state/heap/RestoredHeapPriorityQueueSnapshotRestoreWrapper.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RocksDBHeapRestorePQState.h"
#include "runtime/state/restore/RocksDBRestoreBackendDelegate.h"
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

class RecordingHeapPQWrapper : public HeapPriorityQueueSnapshotRestoreWrapperBase {
public:
    std::shared_ptr<StateMetaInfoSnapshot> snapshotMetaInfo() override
    {
        return nullptr;
    }

    std::unique_ptr<SingleStateIterator> createSnapshotIterator(int, int) override
    {
        return nullptr;
    }

    void restoreSerializedElement(const std::vector<int8_t>& serializedKey, int keyGroupPrefixBytes) override
    {
        restoredKeys.push_back(serializedKey);
        restoredPrefixes.push_back(keyGroupPrefixBytes);
    }

    std::vector<std::vector<int8_t>> restoredKeys;
    std::vector<int> restoredPrefixes;
};

class RocksDBRestoreBackendDelegateTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        const auto* testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
        dbPath_ = std::filesystem::temp_directory_path() /
                  ("rocks-restore-delegate-" + std::string(testInfo->name()) + "-" + std::to_string(nextDbPathId_++));
        std::filesystem::remove_all(dbPath_);

        auto dbOptions = std::make_shared<rocksdb::DBOptions>();
        dbOptions->create_if_missing = true;
        rocksDbHandle_ = std::make_unique<RocksDbHandle>(
            &kvStateInformation_, dbPath_, std::move(dbOptions), [](const std::string&) {
                return rocksdb::ColumnFamilyOptions();
            });
        rocksDbHandle_->openDB();
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

    rocksdb::Status get(const std::string& stateName, const std::vector<int8_t>& key, std::string* value) const
    {
        auto stateIt = kvStateInformation_.find(stateName);
        if (stateIt == kvStateInformation_.end()) {
            return rocksdb::Status::NotFound("state is not registered");
        }
        return rocksDbHandle_->getDb()->Get(
            rocksdb::ReadOptions(),
            stateIt->second->columnFamilyHandle_,
            rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size()),
            value);
    }

    std::filesystem::path dbPath_;
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> kvStateInformation_;
    std::unique_ptr<RocksDbHandle> rocksDbHandle_;
    inline static std::atomic<unsigned long> nextDbPathId_{0};
};

} // namespace

TEST_F(RocksDBRestoreBackendDelegateTest, ReusesMainColumnFamilyForSameStateId)
{
    omnistream::RocksDBRestoreBackendDelegate<int> delegate(rocksDbHandle_.get(), 0, 1, 2 * 1024 * 1024);
    const auto metaInfo = makeMetaInfo("main-state", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
    const std::vector<int8_t> firstKey{1};
    const std::vector<int8_t> secondKey{2};
    const std::vector<int8_t> firstValue{3};
    const std::vector<int8_t> secondValue{4};

    auto firstWriter = delegate.createKVState(7, metaInfo);
    firstWriter->writeEntry(firstKey, ByteView::fromBuffer(firstValue.data(), firstValue.size()));
    firstWriter->flush();
    auto secondWriter = delegate.createKVState(7, metaInfo);
    secondWriter->writeEntry(secondKey, ByteView::fromBuffer(secondValue.data(), secondValue.size()));
    secondWriter->flush();

    ASSERT_EQ(kvStateInformation_.size(), 1U);
    std::string restored;
    ASSERT_TRUE(get("main-state", firstKey, &restored).ok());
    EXPECT_EQ(std::vector<int8_t>(restored.begin(), restored.end()), firstValue);
    ASSERT_TRUE(get("main-state", secondKey, &restored).ok());
    EXPECT_EQ(std::vector<int8_t>(restored.begin(), restored.end()), secondValue);
}

TEST_F(RocksDBRestoreBackendDelegateTest, PriorityQueueFallsBackToRocksDBWithoutHeapRegistry)
{
    omnistream::RocksDBRestoreBackendDelegate<int> delegate(rocksDbHandle_.get(), 0, 1, 2 * 1024 * 1024);
    const auto metaInfo = makeMetaInfo("rocks-pq", StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE);
    const std::vector<int8_t> key{8, 0};
    const std::vector<int8_t> value{9, 0};

    auto writer = delegate.createPQState(11, metaInfo);
    ASSERT_NE(dynamic_cast<omnistream::RocksDBRestorePQState*>(writer.get()), nullptr);
    writer->writeEntry(key, value);
    writer->flush();

    std::string restored;
    ASSERT_TRUE(get("rocks-pq", key, &restored).ok());
    EXPECT_EQ(std::vector<int8_t>(restored.begin(), restored.end()), value);
}

TEST_F(RocksDBRestoreBackendDelegateTest, ExistingHeapPriorityQueueReceivesSerializedEntry)
{
    auto registry = std::make_shared<omnistream::RocksDBRestoreBackendDelegate<int>::RegisteredPQStatesMap>();
    auto wrapper = std::make_shared<RecordingHeapPQWrapper>();
    registry->emplace("heap-pq", wrapper);
    omnistream::RocksDBRestoreBackendDelegate<int> delegate(rocksDbHandle_.get(), 0, 2, 2 * 1024 * 1024, registry);
    const auto metaInfo = makeMetaInfo("heap-pq", StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE);
    const std::vector<int8_t> key{0, 4, 5};

    auto writer = delegate.createPQState(12, metaInfo);
    ASSERT_NE(dynamic_cast<omnistream::RocksDBHeapRestorePQState*>(writer.get()), nullptr);
    writer->writeEntry(key, {7});

    EXPECT_EQ(wrapper->restoredKeys, (std::vector<std::vector<int8_t>>{key}));
    EXPECT_EQ(wrapper->restoredPrefixes, (std::vector<int>{2}));
    EXPECT_TRUE(kvStateInformation_.empty());
}

TEST_F(RocksDBRestoreBackendDelegateTest, MissingHeapPriorityQueueCreatesPendingWrapper)
{
    auto registry = std::make_shared<omnistream::RocksDBRestoreBackendDelegate<int>::RegisteredPQStatesMap>();
    omnistream::RocksDBRestoreBackendDelegate<int> delegate(rocksDbHandle_.get(), 0, 1, 2 * 1024 * 1024, registry);
    const auto metaInfo = makeMetaInfo("pending-pq", StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE);
    const std::vector<int8_t> key{3, 2, 1};

    auto writer = delegate.createPQState(13, metaInfo);
    ASSERT_NE(dynamic_cast<omnistream::RocksDBHeapRestorePQState*>(writer.get()), nullptr);
    writer->writeEntry(key, {});

    ASSERT_EQ(registry->size(), 1U);
    auto pending =
        std::dynamic_pointer_cast<RestoredHeapPriorityQueueSnapshotRestoreWrapper>(registry->at("pending-pq"));
    ASSERT_NE(pending, nullptr);
    EXPECT_EQ(pending->getStateName(), "pending-pq");
    EXPECT_EQ(pending->size(), 1U);
    EXPECT_TRUE(kvStateInformation_.empty());
}
