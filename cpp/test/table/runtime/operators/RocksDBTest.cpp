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
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "typeutils/TypeSerializer.h"
#include "typeutils/BinaryRowDataSerializer.h"
#include "typeutils/LongSerializer.h"
#include "memory/DataInputDeserializer.h"
#include "emhash7.hpp"
#include <filesystem>
#include "table/runtime/operators/window/TimeWindow.h"
#include "state/rocksdb/RocksdbListState.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/rocksdb/RocksdbStateTable.h"
#include "state/VoidNamespaceSerializer.h"
#include "core/api/common/state/MapState.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "runtime/state/rocksdb/RocksdbMapState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "state/rocksdb/RocksDbStringAppendOperator.h"
#include <cstdlib>
#include <ctime>
#include "globals.h"
#include "config.h"

namespace fs = std::filesystem;
using namespace ROCKSDB_NAMESPACE;

extern long vecBatchRows;
extern bool isFlush;

std::string kDBPath = "/tmp/rocksdb_ut/";

std::string getRocksDbPath()
{
    auto now = std::chrono::system_clock::now();
    auto nano_seconds = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    return kDBPath + std::to_string(nano_seconds) + "_";
}

ColumnFamilyOptions createTestColumnFamilyOptions(const std::string&)
{
    return ColumnFamilyOptions();
}

TEST(RocksDBTest, BasicTest)
{
    // restore rocksdb tmp Directory
    fs::remove_all(fs::path(kDBPath));
    fs::create_directories(fs::path(kDBPath));

    DB* db;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &db);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    // Put key-value
    s = db->Put(WriteOptions(), "key1", "value222");
    assert(s.ok());
    std::string value;
    // get value
    s = db->Get(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value222");
    db->Close();
}

TEST(RocksDBTest, BinaryRowDataTest)
{
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer* vSerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

    BinaryRowData* tmpS = BinaryRowData::createBinaryRowDataWithMem(3);
    tmpS->setLong(0, 100);
    tmpS->setLong(1, 200);
    tmpS->setLong(2, 300);

    //    if constexpr (std::is_pointer_v<BinaryRowData>) {
    vSerializer->serialize(tmpS, valueOutputSerializer);
    //    } else {
    //        vSerializer->serialize(&tmpS, valueOutputSerializer);
    //    }

    ROCKSDB_NAMESPACE::Slice sliceKey("key1");
    ROCKSDB_NAMESPACE::Slice sliceValue(
        reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());
    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);

    std::string valueInTable;
    rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), sliceKey, &valueInTable);

    DataInputDeserializer serializedData(
        reinterpret_cast<const uint8_t*>(valueInTable.data()), valueInTable.length(), 0);
    BinaryRowData* resPtr = reinterpret_cast<BinaryRowData*>(vSerializer->deserialize(serializedData));
    //    ASSERT_EQ(100, *resPtr->getLong(0));
    for (int i = 0; i < 3; i++) {
        std::cout << *resPtr->getLong(i) << "|";
    }
    rocksDb->Close();
}

TEST(RocksDBTest, BinaryRowDataStringTest)
{
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer* vSerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

    int arity = 4;
    std::string_view sv0 = "hello";
    std::string_view sv1 = "hellohello_worldworld_abcdefg";
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(arity);
    row->setLong(0, 42);
    row->setStringView(1, sv0);
    row->setInt(2, 42);
    row->setStringView(3, sv1);

    EXPECT_EQ(*row->getLong(0), 42);
    EXPECT_EQ(row->getStringView(1), sv0);
    EXPECT_EQ(*row->getInt(2), 42);
    EXPECT_EQ(row->getStringView(3), sv1);

    vSerializer->serialize(row, valueOutputSerializer);

    ROCKSDB_NAMESPACE::Slice sliceKey("key1");
    ROCKSDB_NAMESPACE::Slice sliceValue(
        reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());
    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);

    std::string valueInTable;
    rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), sliceKey, &valueInTable);

    DataInputDeserializer serializedData(
        reinterpret_cast<const uint8_t*>(valueInTable.data()), valueInTable.length(), 0);
    BinaryRowData* resPtr = reinterpret_cast<BinaryRowData*>(vSerializer->deserialize(serializedData));
    EXPECT_EQ(*resPtr->getLong(0), 42);
    EXPECT_EQ(resPtr->getStringView(1), sv0);
    EXPECT_EQ(*resPtr->getInt(2), 42);
    EXPECT_EQ(resPtr->getStringView(3), sv1);
    rocksDb->Close();
}

TEST(RocksDBTest, BinaryRowDataAndLongTest)
{
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer* keySerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus keyOutputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&keyOutputBufferStatus);

    BinaryRowData* key = BinaryRowData::createBinaryRowDataWithMem(2);
    key->setLong(0, 100);
    key->setLong(1, 101);

    keySerializer->serialize(key, keyOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceKey(
        reinterpret_cast<const char*>(keyOutputSerializer.getData()), keyOutputSerializer.length());

    long value = 100;
    TypeSerializer* vSerializer = new LongSerializer();
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
    vSerializer->serialize(&value, valueOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceValue(
        reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());
    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);

    BinaryRowData* key_copy = BinaryRowData::createBinaryRowDataWithMem(2);
    key_copy->setLong(0, 100);
    key_copy->setLong(1, 101);
    TypeSerializer* keySerializer_copy = new BinaryRowDataSerializer(2);
    DataOutputSerializer keyOutputSerializer_copy;
    OutputBufferStatus keyOutputBufferStatus_copy;
    keyOutputSerializer_copy.setBackendBuffer(&keyOutputBufferStatus_copy);
    keySerializer_copy->serialize(key_copy, keyOutputSerializer_copy);

    ROCKSDB_NAMESPACE::Slice slicekey_copy(
        reinterpret_cast<const char*>(keyOutputSerializer_copy.getData()), keyOutputSerializer_copy.length());
    std::cout << "keyOutputSerializer_copy.length() is " << keyOutputSerializer_copy.length() << std::endl;

    std::string valueInTable;
    rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), slicekey_copy, &valueInTable);
    std::cout << "valueInTable length is " << valueInTable.length() << std::endl;

    DataInputDeserializer serializedData(
        reinterpret_cast<const uint8_t*>(valueInTable.data()), valueInTable.length(), 0);
    int res = serializedData.readLong();
    ASSERT_EQ(100, res);
    std::cout << "success" << std::endl;

    BinaryRowData* key_copy2 = BinaryRowData::createBinaryRowDataWithMem(2);
    key_copy2->setLong(0, 100);
    key_copy2->setLong(1, 102);
    TypeSerializer* keySerializer_copy2 = new BinaryRowDataSerializer(1);
    DataOutputSerializer keyOutputSerializer_copy2;
    OutputBufferStatus keyOutputBufferStatus_copy2;
    keyOutputSerializer_copy2.setBackendBuffer(&keyOutputBufferStatus_copy2);
    keySerializer_copy2->serialize(key_copy2, keyOutputSerializer_copy2);

    ROCKSDB_NAMESPACE::Slice slicekey_copy2(
        reinterpret_cast<const char*>(keyOutputSerializer_copy2.getData()), keyOutputSerializer_copy2.length());
    std::cout << "keyOutputSerializer_copy2.length() is " << keyOutputSerializer_copy2.length() << std::endl;

    std::string valueInTable2;
    auto status = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), slicekey_copy2, &valueInTable2);

    std::cout << "valueInTable2 length is " << valueInTable2.length() << std::endl;

    ASSERT_EQ(true, s.ok());
    std::cout << "success2" << std::endl;
    rocksDb->Close();
}

TEST(RocksDBTest, GetKeyTest)
{
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer* keySerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus keyOutputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&keyOutputBufferStatus);

    BinaryRowData* key = BinaryRowData::createBinaryRowDataWithMem(2);
    key->setLong(0, 100);
    key->setLong(1, 101);

    keySerializer->serialize(key, keyOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceKey(
        reinterpret_cast<const char*>(keyOutputSerializer.getData()), keyOutputSerializer.length());

    std::string valueInTable;
    s = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), sliceKey, &valueInTable);
    std::cout << (int)s.code() << std::endl;
    //    ASSERT_EQ(true, s.IsNotFound());
    //    ASSERT_EQ(0, valueInTable.length());
    //    std::cout << "valueInTable length is: " << valueInTable.length() << ", valueInTable is : " << valueInTable <<
    //    std::endl;
    rocksDb->Close();
}

TEST(RocksDBTest, EntriesTest)
{
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    // put BinaryRowData(100, 101), 100
    TypeSerializer* keySerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus keyOutputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&keyOutputBufferStatus);

    BinaryRowData* key = BinaryRowData::createBinaryRowDataWithMem(2);
    key->setLong(0, 100);
    key->setLong(1, 101);

    keySerializer->serialize(key, keyOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceKey(
        reinterpret_cast<const char*>(keyOutputSerializer.getData()), keyOutputSerializer.length());

    long value = 100;
    TypeSerializer* vSerializer = new LongSerializer();
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
    vSerializer->serialize(&value, valueOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceValue(
        reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());
    //    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);

    BinaryRowDataSerializer* userKeySerializer = new BinaryRowDataSerializer(1);
    LongSerializer* valueSerializer = new LongSerializer();

    // get entries data
    emhash7::HashMap<BinaryRowData*, long> resultMap;

    ROCKSDB_NAMESPACE::Iterator* iterator = rocksDb->NewIterator(ROCKSDB_NAMESPACE::ReadOptions());
    iterator->Seek(sliceKey);
    // 遍历以指定前缀开头的键值对
    for (; iterator->Valid() && iterator->key().starts_with(sliceKey); iterator->Next()) {
        ROCKSDB_NAMESPACE::Slice key = iterator->key();
        ROCKSDB_NAMESPACE::Slice value = iterator->value();
        BinaryRowData* entryKey;
        long entryValue;

        DataInputDeserializer serializedData(reinterpret_cast<const uint8_t*>(key.data()), key.size(), 0);
        void* resPtr = userKeySerializer->deserialize(serializedData);
        entryKey = (BinaryRowData*)resPtr;

        serializedData = DataInputDeserializer(reinterpret_cast<const uint8_t*>(value.data()), value.size(), 0);
        entryValue = serializedData.readLong();
        resultMap.emplace(entryKey, entryValue);
    }
    for (const auto& pair : resultMap) {
        std::cout << "Key: " << std::endl;
        pair.first->printRow();
        std::cout << ", Value: " << pair.second << std::endl;
    }
    std::cout << "success!" << std::endl;
    rocksDb->Close();
}

TEST(RocksDBTest, TimeWindowTypeTest)
{
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer* vSerializer = new TimeWindow::Serializer();
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

    TimeWindow timeWindow(1000, 2000);

    vSerializer->serialize(&timeWindow, valueOutputSerializer);

    ROCKSDB_NAMESPACE::Slice sliceKey("key1");
    ROCKSDB_NAMESPACE::Slice sliceValue(
        reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());
    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);

    std::string valueInTable;
    rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), sliceKey, &valueInTable);

    DataInputDeserializer serializedData(
        reinterpret_cast<const uint8_t*>(valueInTable.data()), valueInTable.length(), 0);
    TimeWindow* resPtr = reinterpret_cast<TimeWindow*>(vSerializer->deserialize(serializedData));
    ASSERT_EQ(1000, resPtr->getStart());
    ASSERT_EQ(2000, resPtr->getEnd());
    rocksDb->Close();
}

TEST(RocksDBTest, MergeTest)
{
    DB* rocksDb;
    Options options;
    options.merge_operator.reset(new RocksDbStringAppendOperator(','));
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());
    std::string key = "key";
    std::string valueInTable;

    Status status = rocksDb->Merge(ROCKSDB_NAMESPACE::WriteOptions(), key, "1");
    status = rocksDb->Merge(ROCKSDB_NAMESPACE::WriteOptions(), key, "2");
    status = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), key, &valueInTable);
    ASSERT_EQ("1,2", valueInTable);

    LongSerializer* serializer = LongSerializer::INSTANCE;
    int64_t currentKey = 100;
    // serializer long data
    DataOutputSerializer outputSerializer;
    OutputBufferStatus outputBufferStatus;
    outputSerializer.setBackendBuffer(&outputBufferStatus);
    serializer->serialize(&currentKey, outputSerializer);
    ROCKSDB_NAMESPACE::Slice keySlice(
        reinterpret_cast<const char*>(outputSerializer.getData()), outputSerializer.length());
    ROCKSDB_NAMESPACE::Slice valueSlice(
        reinterpret_cast<const char*>(outputSerializer.getData()), outputSerializer.length());
    status = rocksDb->Merge(ROCKSDB_NAMESPACE::WriteOptions(), keySlice, valueSlice);
    status = rocksDb->Merge(ROCKSDB_NAMESPACE::WriteOptions(), keySlice, valueSlice);
    status = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), keySlice, &valueInTable);

    DataInputDeserializer serializedData(
        reinterpret_cast<const uint8_t*>(valueInTable.data()), valueInTable.length(), 0);
    auto* result = new std::vector<int64_t>();
    while (serializedData.Available() > 0) {
        void* resPtr = serializer->deserialize(serializedData);
        result->push_back(*(int64_t*)resPtr);
        if (serializedData.Available() > 0) {
            serializedData.readByte();
        }
    }
    for (const auto& item : *result) {
        std::cout << "Rocks merge result: " << item << std::endl;
    }
    rocksDb->Close();
}

TEST(RocksDBTest, ListStateVectorBatchTest)
{
    // Initialize the InternalKeyContext
    auto* context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 2);
    BinaryRowData* binaryRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    binaryRowData->setLong(0, 100);
    context->setCurrentKey(binaryRowData);
    context->setCurrentKeyGroupIndex(1);
    // Initialize RegisteredKeyValueStateBackendMetaInfo
    auto metaInfo = std::make_unique<RegisteredKeyValueStateBackendMetaInfo>(
        "metaInfo", new LongSerializer(), new LongSerializer());
    RocksdbStateTable<RowData*, int64_t, int64_t> rocksdbStateTable(
        context, std::move(metaInfo), new BinaryRowDataSerializer(1));
    // Create HeapListState
    auto* rocksdbListState = new RocksdbListState(&rocksdbStateTable, new LongSerializer(), new LongSerializer());
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;
    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());
    auto kvStateInformation = new std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>();
    rocksdbListState->createTable(
        rocksDb, "ListStateVectorBatchTest", kvStateInformation, createTestColumnFamilyOptions);
    rocksdbListState->setCurrentNamespace(1000);

    // add VectorBatch
    auto vbatchLeft = new omnistream::VectorBatch(2);
    auto vKeyLeft = new omniruntime::vec::Vector<int64_t>(2);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 1);
    vbatchLeft->Append(vKeyLeft);
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(2);
    vWindowEndTimeLeft->SetValue(0, 1000);
    vWindowEndTimeLeft->SetValue(1, 2000);
    vbatchLeft->Append(vWindowEndTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(2);
    vValLeft->SetValue(0, 12);
    vValLeft->SetValue(1, 24);
    vbatchLeft->Append(vValLeft);
    auto keyGroup = context->getCurrentKeyGroupIndex();
    auto sequenceNumber = rocksdbListState->getNextSequenceNumber(keyGroup);
    ASSERT_EQ(0, sequenceNumber);
    rocksdbListState->addVectorBatch(keyGroup, vbatchLeft);
    ASSERT_EQ(1, rocksdbListState->getNextSequenceNumber(keyGroup));

    // verify getVectorBatch
    auto vectorBatch = rocksdbListState->getVectorBatch(keyGroup, sequenceNumber);
    ASSERT_EQ(0, vectorBatch->GetValueAt<int64_t>(0, 0));
    ASSERT_EQ(1000, vectorBatch->GetValueAt<int64_t>(1, 0));
    ASSERT_EQ(12, vectorBatch->GetValueAt<int64_t>(2, 0));
    ASSERT_EQ(1, vectorBatch->GetValueAt<int64_t>(0, 1));
    ASSERT_EQ(2000, vectorBatch->GetValueAt<int64_t>(1, 1));
    ASSERT_EQ(24, vectorBatch->GetValueAt<int64_t>(2, 1));

    vValLeft->SetValue(0, 120);
    auto nextSequenceNumber = rocksdbListState->getNextSequenceNumber(keyGroup);
    ASSERT_EQ(1, nextSequenceNumber);
    rocksdbListState->addVectorBatch(keyGroup, vbatchLeft);
    ASSERT_EQ(2, rocksdbListState->getNextSequenceNumber(keyGroup));
    ASSERT_EQ(120, rocksdbListState->getVectorBatch(keyGroup, nextSequenceNumber)->GetValueAt<int64_t>(2, 0));

    int32_t otherKeyGroup = keyGroup - 1;
    auto otherSequenceNumber = rocksdbListState->getNextSequenceNumber(otherKeyGroup);
    ASSERT_EQ(0, otherSequenceNumber);
    vValLeft->SetValue(0, 999);
    rocksdbListState->addVectorBatch(otherKeyGroup, vbatchLeft);
    ASSERT_EQ(1, rocksdbListState->getNextSequenceNumber(otherKeyGroup));
    ASSERT_EQ(2, rocksdbListState->getNextSequenceNumber(keyGroup));
    ASSERT_EQ(999, rocksdbListState->getVectorBatch(otherKeyGroup, otherSequenceNumber)->GetValueAt<int64_t>(2, 0));
    ASSERT_EQ(12, rocksdbListState->getVectorBatch(keyGroup, sequenceNumber)->GetValueAt<int64_t>(2, 0));
    rocksDb->Close();
}

TEST(RocksDBTest, MapStateVectorBatchTest)
{
    auto* context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 2);
    auto* binaryRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    binaryRowData->setLong(0, 100);
    context->setCurrentKey(binaryRowData);
    context->setCurrentKeyGroupIndex(1);

    auto* voidNamespaceSerializer = new VoidNamespaceSerializer();
    auto* longSerializer = new LongSerializer();
    auto* keySerializer = new BinaryRowDataSerializer(1);
    auto metaInfo =
        std::make_unique<RegisteredKeyValueStateBackendMetaInfo>("metaInfo", voidNamespaceSerializer, longSerializer);
    auto rocksdbMapStateTable = std::make_unique<RocksdbMapStateTable<RowData*, VoidNamespace, int64_t, int64_t>>(
        context, std::move(metaInfo), keySerializer, longSerializer);
    RocksdbMapState<RowData*, VoidNamespace, int64_t, int64_t> rocksdbMapState(
        rocksdbMapStateTable.get(), keySerializer, longSerializer, voidNamespaceSerializer);

    DB* rocksDb;
    Options options;
    options.create_if_missing = true;
    const ::testing::TestInfo* const testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
    Status status = DB::Open(options, getRocksDbPath() + testInfo->name(), &rocksDb);
    ASSERT_TRUE(status.ok());
    auto* kvStateInformation = new std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>();
    rocksdbMapState.createTable(rocksDb, "MapStateVectorBatchTest", kvStateInformation, createTestColumnFamilyOptions);

    auto* vectorBatch = new omnistream::VectorBatch(1);
    auto* values = new omniruntime::vec::Vector<int64_t>(1);
    vectorBatch->Append(values);

    int32_t keyGroup = context->getCurrentKeyGroupIndex();
    ASSERT_EQ(0, rocksdbMapState.getNextSequenceNumber(keyGroup));
    values->SetValue(0, 100);
    rocksdbMapState.addVectorBatch(keyGroup, vectorBatch);
    ASSERT_EQ(1, rocksdbMapState.getNextSequenceNumber(keyGroup));

    values->SetValue(0, 200);
    rocksdbMapState.addVectorBatch(keyGroup, vectorBatch);
    ASSERT_EQ(2, rocksdbMapState.getNextSequenceNumber(keyGroup));

    int32_t otherKeyGroup = keyGroup - 1;
    ASSERT_EQ(0, rocksdbMapState.getNextSequenceNumber(otherKeyGroup));
    values->SetValue(0, 999);
    rocksdbMapState.addVectorBatch(otherKeyGroup, vectorBatch);
    ASSERT_EQ(1, rocksdbMapState.getNextSequenceNumber(otherKeyGroup));
    ASSERT_EQ(2, rocksdbMapState.getNextSequenceNumber(keyGroup));

    ASSERT_EQ(100, rocksdbMapState.getVectorBatch(keyGroup, 0)->GetValueAt<int64_t>(0, 0));
    ASSERT_EQ(200, rocksdbMapState.getVectorBatch(keyGroup, 1)->GetValueAt<int64_t>(0, 0));
    ASSERT_EQ(999, rocksdbMapState.getVectorBatch(otherKeyGroup, 0)->GetValueAt<int64_t>(0, 0));
    rocksdbMapStateTable.reset();
    rocksDb->Close();
}

TEST(RocksDBTest, ValueStateTest)
{
    // Initialize serializers
    VoidNamespaceSerializer* voidNamespaceSerializer = new VoidNamespaceSerializer();
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(1);
    // Initialize the InternalKeyContext
    InternalKeyContextImpl<RowData*>* context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 1);
    BinaryRowData* keyRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData->setLong(0, 100);
    context->setCurrentKey(keyRowData);
    // Initialize RegisteredKeyValueStateBackendMetaInfo
    auto metaInfo = std::make_unique<RegisteredKeyValueStateBackendMetaInfo>(
        "metaInfo", voidNamespaceSerializer, binaryRowDataSerializer);
    RocksdbStateTable<RowData*, VoidNamespace, RowData*> rocksdbStateTable(
        context, std::move(metaInfo), binaryRowDataSerializer);

    // Create HeapListState
    RocksdbValueState<RowData*, VoidNamespace, RowData*>* rocksdbValueState =
        RocksdbValueState<RowData*, VoidNamespace, RowData*>::create(
            new ValueStateDescriptor<RowData*>("ValueStateTest", binaryRowDataSerializer),
            &rocksdbStateTable,
            binaryRowDataSerializer);

    DB* rocksDb;
    Options options;
    options.create_if_missing = true;
    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());
    auto kvStateInformation = new std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>();
    rocksdbValueState->createTable(rocksDb, "ValueStateTest", kvStateInformation, createTestColumnFamilyOptions);
    // Test: Add a BinaryRowData to the ValueState
    BinaryRowData* value = BinaryRowData::createBinaryRowDataWithMem(3);
    value->setLong(0, 100);
    value->setLong(1, 200);
    value->setLong(2, 300);
    rocksdbValueState->update(value);
    BinaryRowData* result = dynamic_cast<BinaryRowData*>(rocksdbValueState->value());

    EXPECT_EQ(*result->getLong(0), 100);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);

    // Test: Update BinaryRowData to the ValueState
    value->setLong(0, 200);
    result = dynamic_cast<BinaryRowData*>(rocksdbValueState->value());
    EXPECT_EQ(*result->getLong(0), 100);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);
    rocksdbValueState->update(value);
    result = dynamic_cast<BinaryRowData*>(rocksdbValueState->value());

    EXPECT_EQ(*result->getLong(0), 200);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);

    // Test: new BinaryRowData, get empty value
    context->setCurrentKey(BinaryRowData::createBinaryRowDataWithMem(1));
    result = dynamic_cast<BinaryRowData*>(rocksdbValueState->value());
    ASSERT_EQ(nullptr, result);

    // Test: clear ValueState
    context->setCurrentKey(keyRowData);
    result = dynamic_cast<BinaryRowData*>(rocksdbValueState->value());

    EXPECT_EQ(*result->getLong(0), 200);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);
    rocksdbValueState->clear();
    result = dynamic_cast<BinaryRowData*>(rocksdbValueState->value());
    ASSERT_EQ(nullptr, result);
    rocksDb->Close();
}

TEST(RocksDBTest, ListStateTest)
{
    // Initialize serializers
    LongSerializer* serializer = LongSerializer::INSTANCE;
    // Initialize the InternalKeyContext
    auto* context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 1);
    BinaryRowData* binaryRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    binaryRowData->setLong(0, 100);
    context->setCurrentKey(binaryRowData);
    context->setCurrentKeyGroupIndex(1);
    // Initialize RegisteredKeyValueStateBackendMetaInfo
    auto metaInfo = std::make_unique<RegisteredKeyValueStateBackendMetaInfo>("metaInfo", serializer, serializer);
    RocksdbStateTable<RowData*, int64_t, int64_t> rocksdbStateTable(
        context, std::move(metaInfo), new BinaryRowDataSerializer(1));
    // Create HeapListState
    auto* rocksdbListState = new RocksdbListState(&rocksdbStateTable, serializer, new LongSerializer());
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;
    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());
    auto kvStateInformation = new std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>();
    rocksdbListState->createTable(rocksDb, "ListStateTest", kvStateInformation, createTestColumnFamilyOptions);
    rocksdbListState->setCurrentNamespace(1000);
    // Test: Add a value to the list
    rocksdbListState->add(10);
    auto list = rocksdbListState->get();

    // Verify: Check if the value is in the list
    ASSERT_TRUE(list != nullptr);
    EXPECT_EQ(list->size(), 1);
    EXPECT_EQ((*list)[0], 10);

    // Test: Add another value to the list
    rocksdbListState->add(20);
    list = rocksdbListState->get();

    // Verify: Check the updated list
    ASSERT_TRUE(list != nullptr);
    EXPECT_EQ(list->size(), 2);
    EXPECT_EQ((*list)[0], 10);
    EXPECT_EQ((*list)[1], 20);

    // Test: Clear the state
    rocksdbListState->clear();
    list = rocksdbListState->get();

    // Verify: List should now be empty
    ASSERT_TRUE(list == nullptr);
    rocksDb->Close();
}

// 待区分写RocksDB和序列化
long newDataTime = 0;
long writeTotalTime = 0;
long writeRocksDBTime = 0;
long serializeTime = 0;
long readTotalTime[] = {0, 0, 0, 0, 0};
long readRocksDBTime[] = {0, 0, 0, 0, 0};
long desTime[] = {0, 0, 0, 0, 0};

void addVectorBatch(
    DB* rocksDb, int32_t keyGroup, uint32_t sequenceNumber, omnistream::VectorBatch* vectorBatch, bool isSync)
{
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus outputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    CompositeKeySerializationUtils::writeKeyGroup(keyGroup, 1, keyOutputSerializer);
    LongSerializer longSerializer;
    int64_t sequenceNumberForSerializer = sequenceNumber;
    longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);

    ROCKSDB_NAMESPACE::Slice key(
        reinterpret_cast<const char*>(keyOutputSerializer.getData()), (int32_t)(keyOutputSerializer.getPosition()));
    int batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];
    auto serializeStart = std::chrono::high_resolution_clock::now();
    omnistream::SerializedBatchInfo serializedBatchInfo =
        omnistream::VectorBatchSerializationUtils::serializeVectorBatch(vectorBatch, batchSize, buffer);
    auto serializeEnd = std::chrono::high_resolution_clock::now();
    serializeTime += std::chrono::duration_cast<std::chrono::nanoseconds>(serializeEnd - serializeStart).count();
    ROCKSDB_NAMESPACE::Slice vbValue(
        reinterpret_cast<const char*>(serializedBatchInfo.dataAddress), serializedBatchInfo.dataSize);

    rocksdb::WriteOptions write_options;
    write_options.disableWAL = true;
    if (isSync) {
        //        std::cout << "flush is true!" << std::endl;
        write_options.sync = isSync;
    }
    auto writeRocksDBStart = std::chrono::high_resolution_clock::now();
    auto res = rocksDb->Put(write_options, key, vbValue);
    auto writeRocksDBEnd = std::chrono::high_resolution_clock::now();
    writeRocksDBTime +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(writeRocksDBEnd - writeRocksDBStart).count();
    if (!res.ok()) {
        ASSERT_EQ(1, 2);
    }
}

omnistream::VectorBatch* getVectorBatch(DB* rocksDb, int32_t keyGroup, uint32_t sequenceNumber, int index)
{
    auto totalTimeStart = std::chrono::high_resolution_clock::now();
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus outputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    CompositeKeySerializationUtils::writeKeyGroup(keyGroup, 1, keyOutputSerializer);
    LongSerializer longSerializer;
    int64_t sequenceNumberForSerializer = sequenceNumber;
    longSerializer.serialize(&sequenceNumberForSerializer, keyOutputSerializer);

    ROCKSDB_NAMESPACE::Slice key(
        reinterpret_cast<const char*>(keyOutputSerializer.getData()), (int32_t)(keyOutputSerializer.getPosition()));

    std::string valueInTable;
    auto rocksDbReadStart = std::chrono::high_resolution_clock::now();
    auto s = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), key, &valueInTable);
    auto rocksDbReadEnd = std::chrono::high_resolution_clock::now();
    readRocksDBTime[index] =
        readRocksDBTime[index] +
        std::chrono::duration_cast<std::chrono::nanoseconds>(rocksDbReadEnd - rocksDbReadStart).count();
    if (!s.ok()) {
        std::cout << "getVectorBatch failed, keyGroup: " << keyGroup << ", sequenceNumber: " << sequenceNumber
                  << std::endl;
        return nullptr;
    } else {
        uint8_t* address = reinterpret_cast<uint8_t*>(valueInTable.data()) + sizeof(int8_t);
        auto desTimeStart = std::chrono::high_resolution_clock::now();
        auto batch = omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(address);
        auto totalTimend = std::chrono::high_resolution_clock::now();
        desTime[index] =
            desTime[index] + std::chrono::duration_cast<std::chrono::nanoseconds>(totalTimend - desTimeStart).count();
        readTotalTime[index] =
            readTotalTime[index] +
            std::chrono::duration_cast<std::chrono::nanoseconds>(totalTimend - totalTimeStart).count();
        return batch;
    }
}

omnistream::VectorBatch* newVectorBatch(int64_t rowCount)
{
    auto vBatch = new omnistream::VectorBatch(rowCount);
    for (int i = 0; i < 5; ++i) {
        auto pVector = new omniruntime::vec::Vector<long>(rowCount);
        for (int j = 0; j < rowCount; ++j) {
            pVector->SetValue(j, 100);
        }
        vBatch->Append(pVector);
    }
    return vBatch;
}

void addBinaryRowData(DB* rocksDb, long id, bool isSync)
{
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus outputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    LongSerializer longSerializer;
    longSerializer.serialize(&id, keyOutputSerializer);

    ROCKSDB_NAMESPACE::Slice sliceKey(
        reinterpret_cast<const char*>(keyOutputSerializer.getData()), (int32_t)(keyOutputSerializer.getPosition()));

    TypeSerializer* vSerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

    auto newDataStart = std::chrono::high_resolution_clock::now();
    BinaryRowData* tmpS = BinaryRowData::createBinaryRowDataWithMem(5);
    for (int j = 0; j < 5; ++j) {
        tmpS->setLong(j, 100);
    }
    auto newDataEnd = std::chrono::high_resolution_clock::now();
    newDataTime += std::chrono::duration_cast<std::chrono::nanoseconds>(newDataEnd - newDataStart).count();
    auto serializeStart = std::chrono::high_resolution_clock::now();
    vSerializer->serialize(tmpS, valueOutputSerializer);
    auto serializeEnd = std::chrono::high_resolution_clock::now();
    serializeTime += std::chrono::duration_cast<std::chrono::nanoseconds>(serializeEnd - serializeStart).count();

    ROCKSDB_NAMESPACE::Slice sliceValue(
        reinterpret_cast<const char*>(valueOutputSerializer.getData()), valueOutputSerializer.length());
    auto writeRocksDBStart = std::chrono::high_resolution_clock::now();
    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);
    auto writeRocksDBEnd = std::chrono::high_resolution_clock::now();
    writeRocksDBTime +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(writeRocksDBEnd - writeRocksDBStart).count();
}

BinaryRowData* getBinaryRowData(DB* rocksDb, long rowId, int index)
{
    auto totalTimeStart = std::chrono::high_resolution_clock::now();
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus outputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    LongSerializer longSerializer;
    longSerializer.serialize(&rowId, keyOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceKey(
        reinterpret_cast<const char*>(keyOutputSerializer.getData()), (int32_t)(keyOutputSerializer.getPosition()));

    std::string valueInTable;
    auto rocksDbReadStart = std::chrono::high_resolution_clock::now();
    auto s = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), sliceKey, &valueInTable);
    auto rocksDbReadEnd = std::chrono::high_resolution_clock::now();
    readRocksDBTime[index] =
        readRocksDBTime[index] +
        std::chrono::duration_cast<std::chrono::nanoseconds>(rocksDbReadEnd - rocksDbReadStart).count();
    if (!s.ok()) {
        return nullptr;
    } else {
        TypeSerializer* vSerializer = new BinaryRowDataSerializer(1);
        DataInputDeserializer serializedData(
            reinterpret_cast<const uint8_t*>(valueInTable.data()), valueInTable.length(), 0);
        auto desTimeStart = std::chrono::high_resolution_clock::now();
        BinaryRowData* resPtr = reinterpret_cast<BinaryRowData*>(vSerializer->deserialize(serializedData));
        auto totalTimend = std::chrono::high_resolution_clock::now();
        desTime[index] =
            desTime[index] + std::chrono::duration_cast<std::chrono::nanoseconds>(totalTimend - desTimeStart).count();
        readTotalTime[index] =
            readTotalTime[index] +
            std::chrono::duration_cast<std::chrono::nanoseconds>(totalTimend - totalTimeStart).count();
        return resPtr;
    }
}

TEST(RocksDBTest, CustomBinaryRowDataPerformanceTest)
{
    std::srand(static_cast<unsigned int>(std::time(nullptr)));
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    long binaryRowDataCount = 500;
    auto writeStart = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < binaryRowDataCount; ++i) {
        addBinaryRowData(rocksDb, i, isFlush);
    }
    if (isFlush) {
        //        std::cout << "flush is true!" << std::endl;
        rocksdb::FlushOptions flush_opts;
        rocksDb->Flush(flush_opts);
    }
    auto writeEnd = std::chrono::high_resolution_clock::now();
    writeTotalTime += std::chrono::duration_cast<std::chrono::nanoseconds>(writeEnd - writeStart).count();
    for (int i = 0; i < 10000; ++i) {
        int rowId = std::rand() % binaryRowDataCount;
        //        std::cout << "batchId: " << batchId << std::endl;
        getBinaryRowData(rocksDb, rowId, 0);
    }
    std::cout << "BinaryRowData" << vecBatchRows << "新建BinaryRowData耗时: " << newDataTime / 1000000 << " 毫秒"
              << std::endl;
    std::cout << "BinaryRowData" << vecBatchRows << "写入RocksDB耗时: " << writeRocksDBTime / 1000000 << " 毫秒"
              << std::endl;
    std::cout << "BinaryRowData" << vecBatchRows << "序列化耗时: " << serializeTime / 1000000 << " 毫秒" << std::endl;
    std::cout << "BinaryRowData" << vecBatchRows << "写入BinaryRowData总耗时: " << writeTotalTime / 1000000 << " 毫秒"
              << std::endl;
    std::cout << "BinaryRowData" << vecBatchRows << "读取RocksDB耗时: " << readRocksDBTime[0] / 1000000 << " 毫秒"
              << std::endl;
    std::cout << "BinaryRowData" << vecBatchRows << "反序列化耗时: " << desTime[0] / 1000000 << " 毫秒" << std::endl;
    std::cout << "BinaryRowData" << vecBatchRows << "读取BinaryRowData总耗时: " << readTotalTime[0] / 1000000 << " 毫秒"
              << std::endl;
    rocksDb->Close();
    delete rocksDb;
}

TEST(RocksDBTest, CustomVectorBatchPerformanceTest)
{
    std::srand(static_cast<unsigned int>(std::time(nullptr)));
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;
    //    options.compression = kNoCompression;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    int vecBatchCount = 50000000 / vecBatchRows;
    std::cout << "vecBatchCount: " << vecBatchCount << std::endl;
    auto writeStart = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < vecBatchCount; ++i) {
        auto newDataStart = std::chrono::high_resolution_clock::now();
        omnistream::VectorBatch* tmpBatch = newVectorBatch(vecBatchRows);
        auto newDataEnd = std::chrono::high_resolution_clock::now();
        newDataTime += std::chrono::duration_cast<std::chrono::nanoseconds>(newDataEnd - newDataStart).count();
        int32_t keyGroup = i % 2;
        uint32_t sequenceNumber = static_cast<uint32_t>(i / 2);
        addVectorBatch(rocksDb, keyGroup, sequenceNumber, tmpBatch, isFlush);
    }
    if (isFlush) {
        //        std::cout << "flush is true!" << std::endl;
        rocksdb::FlushOptions flush_opts;
        rocksDb->Flush(flush_opts);
    }
    auto writeEnd = std::chrono::high_resolution_clock::now();
    writeTotalTime += std::chrono::duration_cast<std::chrono::nanoseconds>(writeEnd - writeStart).count();
    for (int i = 0; i < 10000; ++i) {
        int vectorBatchIndex = std::rand() % vecBatchCount;
        int32_t keyGroup = vectorBatchIndex % 2;
        uint32_t sequenceNumber = static_cast<uint32_t>(vectorBatchIndex / 2);
        getVectorBatch(rocksDb, keyGroup, sequenceNumber, 0);
    }
    std::cout << "VectorBatch" << vecBatchRows << "新建VectorBatch耗时: " << newDataTime / 1000000 << " 毫秒"
              << std::endl;
    std::cout << "VectorBatch" << vecBatchRows << "写入RocksDB耗时: " << writeRocksDBTime / 1000000 << " 毫秒"
              << std::endl;
    std::cout << "VectorBatch" << vecBatchRows << "序列化耗时: " << serializeTime / 1000000 << " 毫秒" << std::endl;
    std::cout << "VectorBatch" << vecBatchRows << "写入VectorBatch总耗时: " << writeTotalTime / 1000000 << " 毫秒"
              << std::endl;
    std::cout << "VectorBatch" << vecBatchRows << "读取RocksDB耗时: " << readRocksDBTime[0] / 1000000 << " 毫秒"
              << std::endl;
    std::cout << "VectorBatch" << vecBatchRows << "反序列化耗时: " << desTime[0] / 1000000 << " 毫秒" << std::endl;
    std::cout << "VectorBatch" << vecBatchRows << "读取VectorBatch总耗时: " << readTotalTime[0] / 1000000 << " 毫秒"
              << std::endl;
    rocksDb->Close();
    delete rocksDb;
}

TEST(RocksDBTest, VectorBatchPerformanceTest)
{
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    int rowCounts[] = {1, 10, 100, 1000, 10000};
    uint32_t sequenceNumber = 0;
    for (const auto& rowCount : rowCounts) {
        omnistream::VectorBatch* tmpBatch = newVectorBatch(rowCount);
        addVectorBatch(rocksDb, 0, sequenceNumber++, tmpBatch, false);
    }
    for (int i = 0; i < 10000; ++i) {
        for (int j = 0; j < 5; ++j) {
            getVectorBatch(rocksDb, 0, static_cast<uint32_t>(j), j);
        }
    }
    for (int i = 0; i < 5; ++i) {
        std::cout << "VectorBatch" << rowCounts[i] << "读取RocksDB耗时: " << readRocksDBTime[i] / 1000000 << " 毫秒"
                  << std::endl;
        std::cout << "VectorBatch" << rowCounts[i] << "反序列化耗时: " << desTime[i] / 1000000 << " 毫秒" << std::endl;
        std::cout << "VectorBatch" << rowCounts[i] << "读取VectorBatch总耗时: " << readTotalTime[i] / 1000000 << " 毫秒"
                  << std::endl;
    }
    rocksDb->Close();
    delete rocksDb;
}
