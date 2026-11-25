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
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <filesystem>
#include <memory>
#include <vector>
#include <chrono>

#include "runtime/state/heap/KeyGroupPartitionedPriorityQueue.h"
#include "runtime/state/rocksdb/RocksDBCachingPriorityQueueSet.h"
#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "core/typeutils/LongSerializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "runtime/state/heap/HeapPriorityQueueElement.h"
#include "runtime/state/KeyGroupRangeAssignment.h"

namespace fs = std::filesystem;

static const std::string kDBPath = "/tmp/rocksdb_kgpq_ut/";

static std::string getRocksDbPath()
{
    auto now = std::chrono::system_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    return kDBPath + std::to_string(ns) + "_";
}

namespace {
class TestElement : public HeapPriorityQueueElement {
public:
    TestElement(int64_t val) : val(val)
    {
    }
    int64_t getKey() const
    {
        return val;
    }

    struct SharedPtrComparator {
        bool operator()(const std::shared_ptr<TestElement>& lhs, const std::shared_ptr<TestElement>& rhs) const
        {
            return lhs->getKey() < rhs->getKey();
        }
    };

    struct SharedPtrHash {
        size_t operator()(const std::shared_ptr<TestElement>& obj) const
        {
            if (obj) {
                return std::hash<int64_t>()(obj->getKey());
            }
            return 0;
        }
    };

    struct SharedPtrEqual {
        bool operator()(const std::shared_ptr<TestElement>& lhs, const std::shared_ptr<TestElement>& rhs) const
        {
            if (lhs && rhs) {
                return lhs->getKey() == rhs->getKey();
            }
            return lhs == rhs;
        }
    };

private:
    int64_t val;
};

class TestElementSerializer : public TypeSerializer {
public:
    void serialize(void* record, DataOutputSerializer& target) override
    {
        auto* elem = static_cast<TestElement*>(record);
        int64_t val = elem->getKey();
        LongSerializer::INSTANCE->serialize(&val, target);
    }

    void* deserialize(DataInputView& source) override
    {
        void* raw = LongSerializer::INSTANCE->deserialize(source);
        auto* val = static_cast<int64_t*>(raw);
        auto* elem = new TestElement(*val);
        delete val;
        return elem;
    }
};
} // namespace

class KeyGroupPartitionedPriorityQueueTestFixture : public ::testing::Test {
protected:
    void SetUp() override
    {
        fs::remove_all(fs::path(kDBPath));
        fs::create_directories(fs::path(kDBPath));

        const ::testing::TestInfo* testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
        dbPath_ = getRocksDbPath() + testInfo->name();

        rocksdb::Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;

        rocksdb::ColumnFamilyOptions cfOptions;
        std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilies;
        columnFamilies.emplace_back("default", rocksdb::ColumnFamilyOptions());
        columnFamilies.emplace_back("pq_test_cf", cfOptions);

        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        rocksdb::Status s = rocksdb::DB::Open(options, dbPath_, columnFamilies, &handles, &db_);
        ASSERT_TRUE(s.ok()) << "Failed to open RocksDB: " << s.ToString();

        defaultCfHandle_ = handles[0];
        cfHandle_ = handles[1];

        readOptions_ = std::make_shared<rocksdb::ReadOptions>();
        writeOptions_ = std::make_shared<rocksdb::WriteOptions>();

        batchWrapper_ = std::make_shared<RocksDBWriteBatchWrapper>(db_, writeOptions_);

        outputStream_ = std::make_shared<DataOutputSerializer>();
        inputStream_ = std::make_shared<DataInputDeserializer>();
        serializer_ = new TestElementSerializer();

        keyGroupRange_ = std::make_unique<KeyGroupRange>(0, 3);
        totalKeyGroups_ = 4;
    }

    void TearDown() override
    {
        delete serializer_;
        batchWrapper_.reset();
        db_->DestroyColumnFamilyHandle(cfHandle_);
        db_->DestroyColumnFamilyHandle(defaultCfHandle_);
        db_->Close();
        delete db_;
        fs::remove_all(fs::path(dbPath_));
    }

    using PQ = KeyGroupPartitionedPriorityQueue<int, std::shared_ptr<TestElement>, TestElement::SharedPtrComparator>;

    std::unique_ptr<PQ> createPQ(int32_t cacheSize = 5)
    {
        auto factory = [this, cacheSize](
                           int32_t keyGroupId, int32_t totalKeyGroups) -> std::shared_ptr<PQ::RocksDBCachingPQ> {
            return std::make_shared<PQ::RocksDBCachingPQ>(
                keyGroupId,
                1,
                db_,
                readOptions_,
                cfHandle_,
                serializer_,
                outputStream_,
                inputStream_,
                batchWrapper_,
                cacheSize);
        };
        return std::make_unique<PQ>(factory, keyGroupRange_.get(), totalKeyGroups_);
    }

    rocksdb::DB* db_ = nullptr;
    rocksdb::ColumnFamilyHandle* defaultCfHandle_ = nullptr;
    rocksdb::ColumnFamilyHandle* cfHandle_ = nullptr;
    std::shared_ptr<rocksdb::ReadOptions> readOptions_;
    std::shared_ptr<rocksdb::WriteOptions> writeOptions_;
    std::shared_ptr<RocksDBWriteBatchWrapper> batchWrapper_;
    std::shared_ptr<DataOutputSerializer> outputStream_;
    std::shared_ptr<DataInputDeserializer> inputStream_;
    TestElementSerializer* serializer_ = nullptr;
    std::string dbPath_;
    std::unique_ptr<KeyGroupRange> keyGroupRange_;
    int32_t totalKeyGroups_ = 4;
};

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, Initialization)
{
    auto pq = createPQ();
    EXPECT_EQ(pq->size(), 0);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, AddAndPoll)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, Inserts)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(3));
    pq->add(std::make_shared<TestElement>(2));
    pq->add(std::make_shared<TestElement>(4));

    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->poll()->getKey(), 2);
    EXPECT_EQ(pq->poll()->getKey(), 3);
    EXPECT_EQ(pq->poll()->getKey(), 4);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, Peek)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(5));
    pq->add(std::make_shared<TestElement>(3));
    pq->add(std::make_shared<TestElement>(1));

    EXPECT_EQ(pq->peek()->getKey(), 1);
    EXPECT_EQ(pq->peek()->getKey(), 1);
    EXPECT_EQ(pq->size(), 3);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, SizeTest)
{
    auto pq = createPQ();
    EXPECT_EQ(pq->size(), 0);

    pq->add(std::make_shared<TestElement>(1));
    EXPECT_EQ(pq->size(), 1);

    pq->add(std::make_shared<TestElement>(2));
    EXPECT_EQ(pq->size(), 2);

    pq->poll();
    EXPECT_EQ(pq->size(), 1);

    pq->remove(std::make_shared<TestElement>(2));
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, RemoveTest)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(2));
    pq->add(std::make_shared<TestElement>(3));
    pq->remove(std::make_shared<TestElement>(2));
    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->poll()->getKey(), 3);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, RemoveNonExistentTest)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(2));

    EXPECT_EQ(pq->remove(std::make_shared<TestElement>(999)), false);
    EXPECT_EQ(pq->size(), 2);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, RemoveHeadTest)
{
    auto pq = createPQ();
    auto elem1 = std::make_shared<TestElement>(1);
    auto elem2 = std::make_shared<TestElement>(2);
    auto elem3 = std::make_shared<TestElement>(3);

    pq->add(elem1);
    pq->add(elem2);
    pq->add(elem3);

    pq->remove(elem1);
    EXPECT_EQ(pq->size(), 2);
    EXPECT_EQ(pq->poll()->getKey(), 2);
    EXPECT_EQ(pq->poll()->getKey(), 3);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, RemoveLastElementTest)
{
    auto pq = createPQ();
    auto elem1 = std::make_shared<TestElement>(1);
    auto elem2 = std::make_shared<TestElement>(2);
    auto elem3 = std::make_shared<TestElement>(3);

    pq->add(elem1);
    pq->add(elem2);
    pq->add(elem3);

    pq->remove(elem3);
    EXPECT_EQ(pq->size(), 2);
    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->poll()->getKey(), 2);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, AddAllTest)
{
    auto pq = createPQ();
    std::vector<std::shared_ptr<TestElement>> elements = {
        std::make_shared<TestElement>(5),
        std::make_shared<TestElement>(2),
        std::make_shared<TestElement>(8),
        std::make_shared<TestElement>(1)};

    pq->addAll(elements);
    EXPECT_EQ(pq->size(), 4);
    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->poll()->getKey(), 2);
    EXPECT_EQ(pq->poll()->getKey(), 5);
    EXPECT_EQ(pq->poll()->getKey(), 8);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, AddAllEmptyTest)
{
    auto pq = createPQ();
    std::vector<std::shared_ptr<TestElement>> emptyElements;

    pq->addAll(emptyElements);
    EXPECT_EQ(pq->size(), 0);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, DuplicateTest)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(1));

    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->poll(), nullptr);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, MixedOperationsTest)
{
    auto pq = createPQ();

    pq->add(std::make_shared<TestElement>(5));
    pq->add(std::make_shared<TestElement>(3));
    EXPECT_EQ(pq->poll()->getKey(), 3);

    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(4));
    EXPECT_EQ(pq->peek()->getKey(), 1);

    pq->remove(std::make_shared<TestElement>(4));
    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->poll()->getKey(), 5);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, SingleElementTest)
{
    auto pq = createPQ();
    auto elem = std::make_shared<TestElement>(42);

    pq->add(elem);
    EXPECT_EQ(pq->size(), 1);
    EXPECT_EQ(pq->isEmpty(), false);
    EXPECT_EQ(pq->peek()->getKey(), 42);

    pq->remove(elem);
    EXPECT_EQ(pq->size(), 0);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, LargeScaleTest)
{
    auto pq = createPQ(100);
    for (int i = 500; i >= 1; --i) {
        pq->add(std::make_shared<TestElement>(i));
    }
    EXPECT_EQ(pq->size(), 500);

    for (int i = 1; i <= 500; ++i) {
        EXPECT_EQ(pq->poll()->getKey(), i);
    }
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, SmallCacheWithLargeDataTest)
{
    auto pq = createPQ(2);

    for (int i = 10; i >= 1; --i) {
        pq->add(std::make_shared<TestElement>(i));
    }
    EXPECT_EQ(pq->size(), 10);

    for (int i = 1; i <= 10; ++i) {
        EXPECT_EQ(pq->poll()->getKey(), i);
    }
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, MultiKeyGroupDistributionTest)
{
    auto pq = createPQ();

    for (int i = 0; i < 100; ++i) {
        pq->add(std::make_shared<TestElement>(i));
    }

    EXPECT_EQ(pq->size(), 100);

    int64_t prev = -1;
    while (!pq->isEmpty()) {
        auto elem = pq->poll();
        EXPECT_GT(elem->getKey(), prev);
        prev = elem->getKey();
    }
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, PollAllThenAddTest)
{
    auto pq = createPQ();

    pq->add(std::make_shared<TestElement>(3));
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(2));

    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->poll()->getKey(), 2);
    EXPECT_EQ(pq->poll()->getKey(), 3);
    EXPECT_EQ(pq->isEmpty(), true);

    pq->add(std::make_shared<TestElement>(10));
    pq->add(std::make_shared<TestElement>(5));
    EXPECT_EQ(pq->poll()->getKey(), 5);
    EXPECT_EQ(pq->poll()->getKey(), 10);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, IteratorTest)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(2));
    pq->add(std::make_shared<TestElement>(3));

    auto iter = pq->iterator();
    int count = 0;
    while (iter->hasNext()) {
        iter->next();
        count++;
    }
    EXPECT_EQ(count, 3);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, CacheOverflowAcrossKeyGroupsTest)
{
    auto pq = createPQ(3);

    for (int i = 1; i <= 20; ++i) {
        pq->add(std::make_shared<TestElement>(i));
    }

    EXPECT_EQ(pq->size(), 20);

    for (int i = 1; i <= 20; ++i) {
        EXPECT_EQ(pq->poll()->getKey(), i);
    }
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, MultiKeyGroupPollOrderingTest)
{
    auto pq = createPQ();

    std::map<int, std::vector<int64_t>> elementsByKeyGroup;
    for (int64_t i = 0; i < 50; ++i) {
        pq->add(std::make_shared<TestElement>(i));
        int kg = KeyGroupRangeAssignment<int>::assignToKeyGroup(static_cast<int>(i), totalKeyGroups_);
        elementsByKeyGroup[kg].push_back(i);
    }

    int numKeyGroupsUsed = 0;
    for (auto& [kg, elems] : elementsByKeyGroup) {
        if (!elems.empty()) numKeyGroupsUsed++;
    }
    EXPECT_GT(numKeyGroupsUsed, 1) << "Test requires elements in multiple key groups";

    int64_t prev = -1;
    while (!pq->isEmpty()) {
        auto elem = pq->poll();
        ASSERT_NE(elem, nullptr);
        EXPECT_GT(elem->getKey(), prev);
        prev = elem->getKey();
    }
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, GetSubsetForKeyGroupBasicTest)
{
    auto pq = createPQ();

    std::map<int, std::vector<int64_t>> elementsByKeyGroup;
    for (int64_t i = 0; i < 20; ++i) {
        pq->add(std::make_shared<TestElement>(i));
        int kg = KeyGroupRangeAssignment<int>::assignToKeyGroup(static_cast<int>(i), totalKeyGroups_);
        elementsByKeyGroup[kg].push_back(i);
    }

    for (auto& [kg, expectedKeys] : elementsByKeyGroup) {
        auto subset = pq->getSubsetForKeyGroup(kg);
        EXPECT_EQ(subset->size(), expectedKeys.size());

        std::vector<int64_t> actualKeys;
        for (const auto& elem : *subset) {
            actualKeys.push_back(elem->getKey());
        }
        std::sort(actualKeys.begin(), actualKeys.end());
        std::sort(expectedKeys.begin(), expectedKeys.end());
        EXPECT_EQ(actualKeys, expectedKeys);
    }
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, GetSubsetForKeyGroupEmptyTest)
{
    auto pq = createPQ();

    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(2));

    int usedKg = KeyGroupRangeAssignment<int>::assignToKeyGroup(1, totalKeyGroups_);
    int emptyKg = -1;
    for (int kg = keyGroupRange_->getStartKeyGroup(); kg <= keyGroupRange_->getEndKeyGroup(); ++kg) {
        if (kg != usedKg) {
            emptyKg = kg;
            break;
        }
    }

    if (emptyKg >= 0) {
        auto subset = pq->getSubsetForKeyGroup(emptyKg);
        EXPECT_EQ(subset->size(), 0);
    }
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, GetSubsetForKeyGroupAllKeyGroupsTest)
{
    auto pq = createPQ();

    for (int64_t i = 0; i < 40; ++i) {
        pq->add(std::make_shared<TestElement>(i));
    }

    size_t totalFromSubsets = 0;
    for (int kg = keyGroupRange_->getStartKeyGroup(); kg <= keyGroupRange_->getEndKeyGroup(); ++kg) {
        auto subset = pq->getSubsetForKeyGroup(kg);
        totalFromSubsets += subset->size();
    }
    EXPECT_EQ(totalFromSubsets, 40u);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, GetSubsetForKeyGroupInvalidKeyGroupTest)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));

    EXPECT_THROW(pq->getSubsetForKeyGroup(-1), std::logic_error);
    EXPECT_THROW(pq->getSubsetForKeyGroup(totalKeyGroups_), std::logic_error);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, GetSubsetForKeyGroupAfterPollTest)
{
    auto pq = createPQ();

    for (int64_t i = 0; i < 10; ++i) {
        pq->add(std::make_shared<TestElement>(i));
    }

    pq->poll();
    pq->poll();

    size_t totalFromSubsets = 0;
    for (int kg = keyGroupRange_->getStartKeyGroup(); kg <= keyGroupRange_->getEndKeyGroup(); ++kg) {
        auto subset = pq->getSubsetForKeyGroup(kg);
        totalFromSubsets += subset->size();
    }
    EXPECT_EQ(totalFromSubsets, 8u);
}

TEST_F(KeyGroupPartitionedPriorityQueueTestFixture, GetSubsetForKeyGroupAfterRemoveTest)
{
    auto pq = createPQ();

    for (int64_t i = 0; i < 10; ++i) {
        pq->add(std::make_shared<TestElement>(i));
    }

    pq->remove(std::make_shared<TestElement>(5));
    pq->remove(std::make_shared<TestElement>(7));

    size_t totalFromSubsets = 0;
    for (int kg = keyGroupRange_->getStartKeyGroup(); kg <= keyGroupRange_->getEndKeyGroup(); ++kg) {
        auto subset = pq->getSubsetForKeyGroup(kg);
        totalFromSubsets += subset->size();
    }
    EXPECT_EQ(totalFromSubsets, 8u);
}
