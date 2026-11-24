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

#include "runtime/state/rocksdb/RocksDBCachingPriorityQueueSet.h"
#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "core/typeutils/LongSerializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "runtime/state/heap/HeapPriorityQueueElement.h"

namespace fs = std::filesystem;

static const std::string kDBPath = "/tmp/rocksdb_pq_ut/";

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

class RocksDBCachingPriorityQueueSetTestFixture : public ::testing::Test {
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

    std::unique_ptr<RocksDBCachingPriorityQueueSet<int, std::shared_ptr<TestElement>, TestElement::SharedPtrComparator>>
    createPQ(int keyGroupId = 0, int keyGroupPrefixBytes = 1, int32_t cacheSize = 5)
    {
        return std::make_unique<
            RocksDBCachingPriorityQueueSet<int, std::shared_ptr<TestElement>, TestElement::SharedPtrComparator>>(
            keyGroupId,
            keyGroupPrefixBytes,
            db_,
            readOptions_,
            cfHandle_,
            serializer_,
            outputStream_,
            inputStream_,
            batchWrapper_,
            cacheSize);
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
};

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, Initialization)
{
    auto pq = createPQ();
    EXPECT_EQ(pq->size(), 0);
    EXPECT_EQ(pq->isEmpty(), true);
    EXPECT_EQ(pq->peek(), nullptr);
    EXPECT_EQ(pq->poll(), nullptr);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, AddAndPoll)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, Inserts)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(3));
    pq->add(std::make_shared<TestElement>(2));
    pq->add(std::make_shared<TestElement>(4));
    pq->add(std::make_shared<TestElement>(6));
    pq->add(std::make_shared<TestElement>(7));
    pq->add(std::make_shared<TestElement>(9));
    pq->add(std::make_shared<TestElement>(5));
    pq->add(std::make_shared<TestElement>(8));
    pq->add(std::make_shared<TestElement>(10));

    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->poll()->getKey(), 2);
    EXPECT_EQ(pq->poll()->getKey(), 3);
    EXPECT_EQ(pq->poll()->getKey(), 4);
    EXPECT_EQ(pq->poll()->getKey(), 5);
    EXPECT_EQ(pq->poll()->getKey(), 6);
    EXPECT_EQ(pq->poll()->getKey(), 7);
    EXPECT_EQ(pq->poll()->getKey(), 8);
    EXPECT_EQ(pq->poll()->getKey(), 9);
    EXPECT_EQ(pq->poll()->getKey(), 10);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, Peek)
{
    auto pq = createPQ();
    EXPECT_EQ(pq->peek(), nullptr);

    pq->add(std::make_shared<TestElement>(5));
    pq->add(std::make_shared<TestElement>(3));
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(4));
    pq->add(std::make_shared<TestElement>(6));
    pq->add(std::make_shared<TestElement>(7));

    EXPECT_EQ(pq->peek()->getKey(), 1);
    EXPECT_EQ(pq->peek()->getKey(), 1);
    EXPECT_EQ(pq->size(), 6);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, ClearTest)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(5));
    pq->add(std::make_shared<TestElement>(3));
    EXPECT_EQ(pq->isEmpty(), false);
    EXPECT_EQ(pq->poll()->getKey(), 3);
    EXPECT_EQ(pq->poll()->getKey(), 5);
    EXPECT_EQ(pq->poll(), nullptr);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, DuplicateTest)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(3));
    pq->add(std::make_shared<TestElement>(2));
    pq->add(std::make_shared<TestElement>(4));
    pq->add(std::make_shared<TestElement>(6));
    pq->add(std::make_shared<TestElement>(7));
    pq->add(std::make_shared<TestElement>(9));
    pq->add(std::make_shared<TestElement>(5));
    pq->add(std::make_shared<TestElement>(8));
    pq->add(std::make_shared<TestElement>(10));
    pq->add(std::make_shared<TestElement>(1));

    EXPECT_EQ(pq->size(), 10);
    EXPECT_EQ(pq->poll()->getKey(), 1);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, RemoveTest)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(3));
    pq->add(std::make_shared<TestElement>(2));
    pq->add(std::make_shared<TestElement>(4));
    pq->add(std::make_shared<TestElement>(6));
    pq->add(std::make_shared<TestElement>(7));
    pq->add(std::make_shared<TestElement>(9));
    pq->add(std::make_shared<TestElement>(5));
    pq->add(std::make_shared<TestElement>(8));
    pq->add(std::make_shared<TestElement>(10));

    pq->remove(std::make_shared<TestElement>(2));
    EXPECT_EQ(pq->size(), 9);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, SizeTest)
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

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, IteratorTest)
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

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, AddAllTest)
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

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, RemoveNonExistentTest)
{
    auto pq = createPQ();
    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(2));

    EXPECT_EQ(pq->remove(std::make_shared<TestElement>(999)), false);
    EXPECT_EQ(pq->size(), 2);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, RemoveHeadTest)
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

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, EmptyQueueOperationsTest)
{
    auto pq = createPQ();
    EXPECT_EQ(pq->isEmpty(), true);
    EXPECT_EQ(pq->size(), 0);
    EXPECT_EQ(pq->peek(), nullptr);
    EXPECT_EQ(pq->poll(), nullptr);
    EXPECT_EQ(pq->remove(std::make_shared<TestElement>(1)), false);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, LargeScaleTest)
{
    auto pq = createPQ(0, 1, 100);
    for (int i = 500; i >= 1; --i) {
        pq->add(std::make_shared<TestElement>(i));
    }
    EXPECT_EQ(pq->size(), 500);

    for (int i = 1; i <= 500; ++i) {
        EXPECT_EQ(pq->poll()->getKey(), i);
    }
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, MixedOperationsTest)
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

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, CacheOverflowTest)
{
    auto pq = createPQ(0, 1, 3);

    pq->add(std::make_shared<TestElement>(1));
    pq->add(std::make_shared<TestElement>(2));
    pq->add(std::make_shared<TestElement>(3));
    pq->add(std::make_shared<TestElement>(4));
    pq->add(std::make_shared<TestElement>(5));

    EXPECT_EQ(pq->size(), 5);
    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->poll()->getKey(), 2);
    EXPECT_EQ(pq->poll()->getKey(), 3);
    EXPECT_EQ(pq->poll()->getKey(), 4);
    EXPECT_EQ(pq->poll()->getKey(), 5);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, SingleElementTest)
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
    EXPECT_EQ(pq->peek(), nullptr);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, AddAllEmptyTest)
{
    auto pq = createPQ();
    std::vector<std::shared_ptr<TestElement>> emptyElements;

    pq->addAll(emptyElements);
    EXPECT_EQ(pq->size(), 0);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, RemoveLastElementTest)
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

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, RemoveMiddleElementTest)
{
    auto pq = createPQ();
    auto elem1 = std::make_shared<TestElement>(1);
    auto elem2 = std::make_shared<TestElement>(5);
    auto elem3 = std::make_shared<TestElement>(3);
    auto elem4 = std::make_shared<TestElement>(2);
    auto elem5 = std::make_shared<TestElement>(4);

    pq->add(elem1);
    pq->add(elem2);
    pq->add(elem3);
    pq->add(elem4);
    pq->add(elem5);

    pq->remove(elem3);
    EXPECT_EQ(pq->size(), 4);
    EXPECT_EQ(pq->poll()->getKey(), 1);
    EXPECT_EQ(pq->poll()->getKey(), 2);
    EXPECT_EQ(pq->poll()->getKey(), 4);
    EXPECT_EQ(pq->poll()->getKey(), 5);
    EXPECT_EQ(pq->isEmpty(), true);
}

TEST_F(RocksDBCachingPriorityQueueSetTestFixture, SmallCacheWithLargeDataTest)
{
    auto pq = createPQ(0, 1, 2);

    for (int i = 100; i >= 1; --i) {
        pq->add(std::make_shared<TestElement>(i));
    }
    EXPECT_EQ(pq->size(), 100);

    for (int i = 1; i <= 100; ++i) {
        EXPECT_EQ(pq->poll()->getKey(), i);
    }
    EXPECT_EQ(pq->isEmpty(), true);
}
