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

#ifdef WITH_OMNISTATESTORE
#include <gtest/gtest.h>
#include "boost_state_db.h"
#include "bss_types.h"
#include "typeutils/BinaryRowDataSerializer.h"
#include "state/VoidNamespaceSerializer.h"
#include "state/InternalKeyContextImpl.h"
#include "state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "state/bss/BssStateTable.h"
#include "state/bss/BssValueState.h"
#include "api/common/state/ValueStateDescriptor.h"
#include "globals.h"
#include "state/bss/BssListState.h"
#include "state/bss/BssMapState.h"
#include "api/common/state/ListStateDescriptor.h"
#include <random>
#include <cstdint>
#include <stdexcept>

using namespace ock::bss;

long newDataTime1 = 0;
long writeTotalTime1 = 0;
long writeRocksDBTime1 = 0;
long serializeTime1 = 0;
long readTotalTime1[] = {0, 0, 0, 0, 0};
long readRocksDBTime1[] = {0, 0, 0, 0, 0};
long desTime1[] = {0, 0, 0, 0, 0};

class UUIDGenerator {
public:
    static uint32_t generateUUID()
    {
        // 每个线程有独立的随机数生成器
        thread_local std::random_device rd;                                      // 随机种子
        thread_local std::mt19937 gen(rd());                                     // 梅森旋转算法生成器
        thread_local std::uniform_int_distribution<uint32_t> dis(1, UINT32_MAX); // 1 到 uint32_t 最大值
        return dis(gen);
    }
};

void initialValueState(BssValueState<RowData*, VoidNamespace, RowData*>*& valueState)
{
    BoostStateDBPtr nDB = BoostStateDBFactory::Create();
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_1K);
    config->SetTaskSlotFlag(UUIDGenerator::generateUUID());
    nDB->Open(config);
    valueState->CreateTable(nDB);
}

void initialListState(BssListState<RowData*, int64_t, int64_t>*& listState)
{
    BoostStateDBPtr nDB = BoostStateDBFactory::Create();
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_1K);
    config->SetTaskSlotFlag(UUIDGenerator::generateUUID());
    nDB->Open(config);
    listState->CreateTable(nDB);
}

void initialMapState(BssMapState<RowData*, int64_t, int64_t, int64_t>*& mapState)
{
    BoostStateDBPtr nDB = BoostStateDBFactory::Create();
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_1K);
    config->SetTaskSlotFlag(UUIDGenerator::generateUUID());
    nDB->Open(config);
    mapState->CreateTable(nDB);
}

TableRef CreateFromDB(StateType keyedStateType, std::string tableName, BoostStateDB* db)
{
    auto tblDesc = std::make_shared<ock::bss::TableDescription>(
        keyedStateType, "dbTable", -1, ock::bss::TableSerializer{}, db->GetConfig());
    return db->GetTableOrCreate(tblDesc);
}

TEST(BssStateTest, kvtable_long_test)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_1K);
    config->SetTaskSlotFlag(UUIDGenerator::generateUUID());
    BoostStateDBPtr nDB = BoostStateDBFactory::Create();
    nDB->Open(config);
    KVTableRef kVTable = nullptr;
    kVTable = std::dynamic_pointer_cast<KVTable>(CreateFromDB(StateType::VALUE, "kVTable", nDB));

    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus outputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    LongSerializer longSerializer;
    long vectorBatchId = 1;
    longSerializer.serialize(&vectorBatchId, keyOutputSerializer);

    BinaryData priKey(keyOutputSerializer.getData(), (int32_t)(keyOutputSerializer.getPosition()));
    BinaryData val(keyOutputSerializer.getData(), (int32_t)(keyOutputSerializer.getPosition()));
    uint32_t keyHashCode = HashCode::Hash(keyOutputSerializer.getData(), (int32_t)(keyOutputSerializer.getPosition()));
    kVTable->Put(keyHashCode, priKey, val);

    BinaryData readValue;
    kVTable->Get(keyHashCode, priKey, readValue);
    DataInputDeserializer serializedData(reinterpret_cast<const uint8_t*>(readValue.Data()), readValue.Length(), 0);
    void* readLong = longSerializer.deserialize(serializedData);
    ASSERT_EQ(*reinterpret_cast<long*>(readLong), 1);

    nDB->Close();
    delete nDB;
}

omnistream::VectorBatch* GetVectorBatch(int64_t rowCount)
{
    auto vBatch = new omnistream::VectorBatch(rowCount);
    for (int i = 0; i < 5; ++i) {
        auto pVector = new omniruntime::vec::Vector<long>(rowCount);
        for (int j = 0; j < rowCount; ++j) {
            pVector->SetValue(j, 100 + j + i);
        }
        vBatch->Append(pVector);
    }
    return vBatch;
}

TEST(BssStateTest, BinaryRowDataSerialzerTest)
{
    BinaryRowData* value = BinaryRowData::createBinaryRowDataWithMem(3);
    value->setLong(0, 100);
    value->setLong(1, 200);
    value->setLong(2, 300);
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus outputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(1);
    binaryRowDataSerializer->serialize(value, keyOutputSerializer);
    BinaryData priKey(keyOutputSerializer.getData(), (int32_t)(keyOutputSerializer.getPosition()));
    DataInputDeserializer serializedData(priKey.Data(), priKey.Length(), 0);
    void* readLong = binaryRowDataSerializer->deserialize(serializedData);
    auto res = *reinterpret_cast<BinaryRowData*>(readLong);
    EXPECT_EQ(*res.getLong(0), 100);
    EXPECT_EQ(*res.getLong(1), 200);
}

TEST(BssStateTest, KVTable_putNGet_binaryRowData)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_1K);
    config->SetTaskSlotFlag(UUIDGenerator::generateUUID());
    BoostStateDBPtr nDB = BoostStateDBFactory::Create();
    nDB->Open(config);
    KVTableRef kVTable = nullptr;
    kVTable = std::dynamic_pointer_cast<KVTable>(CreateFromDB(StateType::VALUE, "kVTable", nDB));

    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus outputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(3);
    BinaryRowData* value = BinaryRowData::createBinaryRowDataWithMem(3);
    value->setLong(0, 100);
    value->setLong(1, 200);
    value->setLong(2, 300);
    binaryRowDataSerializer->serialize(value, keyOutputSerializer);

    BinaryData priKey(keyOutputSerializer.getData(), (int32_t)(keyOutputSerializer.getPosition()));
    BinaryData val(keyOutputSerializer.getData(), (int32_t)(keyOutputSerializer.getPosition()));
    uint32_t keyHashCode = HashCode::Hash(keyOutputSerializer.getData(), (int32_t)(keyOutputSerializer.getPosition()));
    kVTable->Put(keyHashCode, priKey, val);

    BinaryData readValue;
    kVTable->Get(keyHashCode, priKey, readValue);
    DataInputDeserializer serializedData(reinterpret_cast<const uint8_t*>(readValue.Data()), readValue.Length(), 0);
    void* readLong = binaryRowDataSerializer->deserialize(serializedData);
    auto res = *reinterpret_cast<BinaryRowData*>(readLong);
    EXPECT_EQ(*res.getLong(0), 100);
    EXPECT_EQ(*res.getLong(1), 200);

    kVTable->Get(keyHashCode, priKey, readValue);
    DataInputDeserializer serializedData1(reinterpret_cast<const uint8_t*>(readValue.Data()), readValue.Length(), 0);
    readLong = binaryRowDataSerializer->deserialize(serializedData1);
    res = *reinterpret_cast<BinaryRowData*>(readLong);
    EXPECT_EQ(*res.getLong(0), 100);
    EXPECT_EQ(*res.getLong(1), 200);

    nDB->Close();
    delete nDB;
}

TEST(BssStateTest, BssValueStateTest)
{
    // serializer
    VoidNamespaceSerializer* voidNamespaceSerializer = new VoidNamespaceSerializer();
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(3);
    // Initialize the InternalKeyContext
    InternalKeyContextImpl<RowData*>* context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 1);
    BinaryRowData* keyRowData = BinaryRowData::createBinaryRowDataWithMem(3);
    keyRowData->setLong(0, 100);
    keyRowData->setLong(1, 100);
    keyRowData->setLong(2, 100);
    context->setCurrentKey(keyRowData);
    context->setCurrentKeyGroupIndex(1);
    RegisteredKeyValueStateBackendMetaInfo* metaInfo =
        new RegisteredKeyValueStateBackendMetaInfo("metaInfo", voidNamespaceSerializer, binaryRowDataSerializer);
    BssStateTable<RowData*, VoidNamespace, RowData*> bssStateTable(context, metaInfo, binaryRowDataSerializer);
    BssValueState<RowData*, VoidNamespace, RowData*>* valueState =
        BssValueState<RowData*, VoidNamespace, RowData*>::create(
            new ValueStateDescriptor<RowData*>("ValueStateTest", binaryRowDataSerializer),
            &bssStateTable,
            binaryRowDataSerializer);
    initialValueState(valueState);
    BinaryRowData* value = BinaryRowData::createBinaryRowDataWithMem(3);
    value->setLong(0, 100);
    value->setLong(1, 200);
    value->setLong(2, 300);
    valueState->update(value);
    BinaryRowData* result = dynamic_cast<BinaryRowData*>(valueState->value());
    EXPECT_EQ(*result->getLong(0), 100);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);
    LOG("*******************************************");
    value->setLong(0, 200);
    result = dynamic_cast<BinaryRowData*>(valueState->value());
    EXPECT_EQ(*result->getLong(0), 100);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);
    LOG("*******************************************");
    valueState->update(value);
    result = dynamic_cast<BinaryRowData*>(valueState->value());
    EXPECT_EQ(*result->getLong(0), 200);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);
    LOG("*******************************************");
    auto newKey = BinaryRowData::createBinaryRowDataWithMem(1);
    context->setCurrentKey(BinaryRowData::createBinaryRowDataWithMem(1));
    result = dynamic_cast<BinaryRowData*>(valueState->value());
    ASSERT_EQ(nullptr, result);
    LOG("*******************************************");
    context->setCurrentKey(keyRowData);
    result = dynamic_cast<BinaryRowData*>(valueState->value());
    LOG("*******************************************");
    EXPECT_EQ(*result->getLong(0), 200);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);
    LOG("*******************************************");
    valueState->clear();
    result = dynamic_cast<BinaryRowData*>(valueState->value());
    ASSERT_EQ(nullptr, result);
    LOG("*******************************************");
    delete newKey;
    delete keyRowData;
    delete value;
}

BinaryData GetBssUnifyingData(int64_t longValue, uint32_t& hashcode, DataOutputSerializer& keyOutputSerializer)
{
    LongSerializer longSerializer;
    longSerializer.serialize(&longValue, keyOutputSerializer);
    BinaryData bssData(keyOutputSerializer.getData(), keyOutputSerializer.getPosition());
    hashcode = HashCode::Hash(keyOutputSerializer.getData(), keyOutputSerializer.getPosition());
    return bssData;
}

TEST(BssStateTest, Bss_KListState_put_get)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_1K);
    config->SetTaskSlotFlag(UUIDGenerator::generateUUID());
    BoostStateDBPtr nDB = BoostStateDBFactory::Create();
    nDB->Open(config);
    KListTableRef kListTable;
    kListTable = std::dynamic_pointer_cast<KListTable>(CreateFromDB(StateType::LIST, "kListTable", nDB));

    int64_t key = 10000;
    uint32_t keyHashCode;
    OutputBufferStatus outputBufferStatus;
    DataOutputSerializer keyOutputSerializer;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    BinaryData keyData = GetBssUnifyingData(key, keyHashCode, keyOutputSerializer);

    int64_t value = 9999;
    uint32_t valueHashCode;
    OutputBufferStatus outputBufferStatus1;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus1);
    BinaryData valueData = GetBssUnifyingData(value, valueHashCode, keyOutputSerializer);

    kListTable->Put(keyHashCode, keyData, valueData);

    auto serializer = new LongSerializer();
    auto getResult = kListTable->Get(keyHashCode, keyData);
    for (std::size_t i = 0; i < getResult.addresses.size(); ++i) {
        DataInputDeserializer serializedData(
            reinterpret_cast<const uint8_t*>(getResult.addresses.at(i)), getResult.lengths.at(i), 0);
        void* resPtr = serializer->deserialize(serializedData);
        LOG("result is " << *(long*)resPtr);
        EXPECT_EQ(9999, *(long*)resPtr);
    }
}

TEST(BssStateTest, Bss_KListState_add_get)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_1K);
    config->SetTaskSlotFlag(UUIDGenerator::generateUUID());
    BoostStateDBPtr nDB = BoostStateDBFactory::Create();
    nDB->Open(config);
    KListTableRef kListTable;
    kListTable = std::dynamic_pointer_cast<KListTable>(CreateFromDB(StateType::LIST, "kListTable", nDB));

    int64_t key = 10000;
    uint32_t keyHashCode;
    OutputBufferStatus outputBufferStatus;
    DataOutputSerializer keyOutputSerializer;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    const BinaryData keyData = GetBssUnifyingData(key, keyHashCode, keyOutputSerializer);

    int64_t value = 9999;
    uint32_t valueHashCode;
    OutputBufferStatus outputBufferStatus1;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus1);
    BinaryData valueData = GetBssUnifyingData(value, valueHashCode, keyOutputSerializer);

    LongSerializer* serializer = new LongSerializer();

    int64_t value1 = 9998;
    uint32_t valueHashCode1;
    OutputBufferStatus outputBufferStatus2;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus2);
    BinaryData valueData1 = GetBssUnifyingData(value1, valueHashCode1, keyOutputSerializer);

    int64_t value2 = 9997;
    uint32_t valueHashCode2;
    OutputBufferStatus outputBufferStatus3;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus3);
    BinaryData valueData2 = GetBssUnifyingData(value2, valueHashCode2, keyOutputSerializer);

    kListTable->Add(keyHashCode, keyData, valueData);
    kListTable->Add(keyHashCode, keyData, valueData1);
    kListTable->Add(keyHashCode, keyData, valueData2);

    auto getResult = kListTable->Get(keyHashCode, keyData);
    kListTable->CleanResource(getResult.resId);
    std::vector<int64_t>* result = new std::vector<int64_t>();

    for (int i = 0; i < getResult.addresses.size(); ++i) {
        DataInputDeserializer serializedData(
            reinterpret_cast<const uint8_t*>(getResult.addresses.at(i)), getResult.lengths.at(i), 0);
        auto tempData = serializer->deserialize(serializedData);
        LOG("tempData: " << (long)tempData);
        LOG("tempData1: " << (long*)tempData);
        result->push_back(*(long*)tempData);
        LOG("==================================");
    }

    EXPECT_EQ(3, result->size());
    EXPECT_EQ(9999, result->at(0));
    EXPECT_EQ(9998, result->at(1));
    EXPECT_EQ(9997, result->at(2));
}

TEST(BssStateTest, Bss_KMapState_add_get1)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_1K);
    config->SetTaskSlotFlag(UUIDGenerator::generateUUID());
    BoostStateDBPtr nDB = BoostStateDBFactory::Create();
    nDB->Open(config);
    KMapTableRef kMapTable;
    kMapTable = std::dynamic_pointer_cast<KMapTable>(CreateFromDB(StateType::MAP, "kMapTable", nDB));

    int64_t key1 = 10000;
    uint32_t keyHashCode1;
    OutputBufferStatus outputBufferStatus;
    DataOutputSerializer keyOutputSerializer;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    const BinaryData keyData1 = GetBssUnifyingData(key1, keyHashCode1, keyOutputSerializer);

    int64_t key2 = 10001;
    uint32_t keyHashCode2;
    OutputBufferStatus outputBufferStatus1;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus1);
    const BinaryData keyData2 = GetBssUnifyingData(key2, keyHashCode2, keyOutputSerializer);

    int64_t key3 = 10002;
    uint32_t keyHashCode3;
    OutputBufferStatus outputBufferStatus2;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus2);
    const BinaryData keyData3 = GetBssUnifyingData(key3, keyHashCode3, keyOutputSerializer);

    int64_t value1 = 9998;
    uint32_t valueHashCode1;
    OutputBufferStatus outputBufferStatus3;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus3);
    BinaryData valueData1 = GetBssUnifyingData(value1, valueHashCode1, keyOutputSerializer);

    int64_t value2 = 9997;
    uint32_t valueHashCode2;
    OutputBufferStatus outputBufferStatus4;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus4);
    BinaryData valueData2 = GetBssUnifyingData(value2, valueHashCode2, keyOutputSerializer);

    int64_t value3 = 9996;
    uint32_t valueHashCode3;
    OutputBufferStatus outputBufferStatus5;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus5);
    BinaryData valueData3 = GetBssUnifyingData(value3, valueHashCode3, keyOutputSerializer);

    int64_t prikey = 100;
    uint32_t prikeyHashCode;
    OutputBufferStatus outputBufferStatus6;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus6);
    BinaryData prikeyData = GetBssUnifyingData(prikey, prikeyHashCode, keyOutputSerializer);
    LongSerializer* longSerializer = new LongSerializer();

    kMapTable->Put(keyHashCode1, prikeyData, keyData1, valueData1);
    kMapTable->Put(keyHashCode2, prikeyData, keyData2, valueData2);
    kMapTable->Put(keyHashCode3, prikeyData, keyData3, valueData3);
    auto iterator = kMapTable->EntryIteratorWrraper(keyHashCode1, prikeyData);
    while (iterator->HasNext()) {
        LOG("iterator1 test");
        auto temp = iterator->Next();
        auto keyData = temp[0];
        auto valueData = temp[1];
        DataInputDeserializer keySerializedData(reinterpret_cast<const uint8_t*>(keyData.Data()), keyData.Length(), 0);
        auto keyRes = *(long*)longSerializer->deserialize(keySerializedData);
        DataInputDeserializer valSerializedData(
            reinterpret_cast<const uint8_t*>(valueData.Data()), valueData.Length(), 0);
        auto valRes = *(long*)longSerializer->deserialize(valSerializedData);
        EXPECT_EQ(keyRes, 10000);
        EXPECT_EQ(valRes, 9998);
    }
    delete iterator;

    auto iterator2 = kMapTable->EntryIteratorWrraper(keyHashCode2, prikeyData);
    while (iterator2->HasNext()) {
        LOG("iterator2 test");
        auto temp = iterator2->Next();
        auto keyData = temp[0];
        auto valueData = temp[1];
        DataInputDeserializer keySerializedData(reinterpret_cast<const uint8_t*>(keyData.Data()), keyData.Length(), 0);
        auto keyRes = *(long*)longSerializer->deserialize(keySerializedData);
        DataInputDeserializer valSerializedData(
            reinterpret_cast<const uint8_t*>(valueData.Data()), valueData.Length(), 0);
        auto valRes = *(long*)longSerializer->deserialize(valSerializedData);
        EXPECT_EQ(keyRes, 10001);
        EXPECT_EQ(valRes, 9997);
    }
    delete iterator2;
    auto iterator3 = kMapTable->EntryIteratorWrraper(keyHashCode3, prikeyData);
    while (iterator3->HasNext()) {
        auto temp = iterator3->Next();
        auto keyData = temp[0];
        auto valueData = temp[1];
        DataInputDeserializer keySerializedData(reinterpret_cast<const uint8_t*>(keyData.Data()), keyData.Length(), 0);
        auto keyRes = *(long*)longSerializer->deserialize(keySerializedData);
        DataInputDeserializer valSerializedData(
            reinterpret_cast<const uint8_t*>(valueData.Data()), valueData.Length(), 0);
        auto valRes = *(long*)longSerializer->deserialize(valSerializedData);
        EXPECT_EQ(keyRes, 10002);
        EXPECT_EQ(valRes, 9996);
    }
    delete iterator3;
}

TEST(BssStateTest, Bss_KMapState_add_get)
{
    std::srand(static_cast<unsigned int>(std::time(nullptr)));
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_1K);
    config->SetTaskSlotFlag(UUIDGenerator::generateUUID());
    BoostStateDBPtr nDB = BoostStateDBFactory::Create();
    nDB->Open(config);
    KMapTableRef kMapTable;
    kMapTable = std::dynamic_pointer_cast<KMapTable>(CreateFromDB(StateType::MAP, "kMapTable", nDB));

    int64_t key = 10000;
    uint32_t keyHashCode;
    OutputBufferStatus outputBufferStatus;
    DataOutputSerializer keyOutputSerializer;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus);
    const BinaryData keyData = GetBssUnifyingData(key, keyHashCode, keyOutputSerializer);

    int64_t key1 = 10000;
    uint32_t keyHashCode1;
    OutputBufferStatus outputBufferStatus1;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus1);
    const BinaryData keyData1 = GetBssUnifyingData(key1, keyHashCode1, keyOutputSerializer);

    int64_t key2 = 10000;
    uint32_t keyHashCode2;
    OutputBufferStatus outputBufferStatus2;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus2);
    const BinaryData keyData2 = GetBssUnifyingData(key2, keyHashCode2, keyOutputSerializer);

    int64_t key3 = 10000;
    uint32_t keyHashCode3;
    OutputBufferStatus outputBufferStatus3;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus3);
    const BinaryData keyData3 = GetBssUnifyingData(key3, keyHashCode3, keyOutputSerializer);

    int64_t value = 9999;
    uint32_t valueHashCode;
    OutputBufferStatus outputBufferStatus4;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus4);
    BinaryData valueData = GetBssUnifyingData(value, valueHashCode, keyOutputSerializer);

    int64_t value1 = 9998;
    uint32_t valueHashCode1;
    OutputBufferStatus outputBufferStatus5;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus5);
    BinaryData valueData1 = GetBssUnifyingData(value1, valueHashCode1, keyOutputSerializer);

    int64_t value2 = 9997;
    uint32_t valueHashCode2;
    OutputBufferStatus outputBufferStatus6;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus6);
    BinaryData valueData2 = GetBssUnifyingData(value2, valueHashCode2, keyOutputSerializer);

    int64_t prikey = 100;
    uint32_t prikeyHashCode;
    OutputBufferStatus outputBufferStatus7;
    keyOutputSerializer.setBackendBuffer(&outputBufferStatus7);
    BinaryData prikeyData = GetBssUnifyingData(prikey, prikeyHashCode, keyOutputSerializer);
    kMapTable->Put(keyHashCode, prikeyData, keyData, valueData);
    kMapTable->Put(keyHashCode, prikeyData, keyData, valueData1);
    kMapTable->Put(keyHashCode, prikeyData, keyData, valueData2);
    BinaryData valueDataFromMapState;
    kMapTable->Get(keyHashCode, prikeyData, keyData, valueDataFromMapState);

    DataInputDeserializer serializedData(
        reinterpret_cast<const uint8_t*>(valueDataFromMapState.Data()), valueDataFromMapState.Length(), 0);
    LongSerializer* longSerializer = new LongSerializer();
    auto res = longSerializer->deserialize(serializedData);
    auto size = *(long*)res;
    EXPECT_EQ(size, 9997);

    kMapTable->Remove(keyHashCode, prikeyData);
    auto* wrapper = kMapTable->EntryIteratorWrraper(keyHashCode, prikeyData);
    ASSERT_EQ(false, wrapper->HasNext());
    delete wrapper;
}

TEST(BssStateTest, BssListState)
{
    LongSerializer* serializer = LongSerializer::INSTANCE;
    // Initialize the InternalKeyContext
    InternalKeyContextImpl<RowData*>* context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 1);
    BinaryRowData* keyRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData->setLong(0, 100);
    context->setCurrentKey(keyRowData);
    context->setCurrentKeyGroupIndex(1);
    auto* metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", serializer, serializer);
    BssListStateTable<RowData*, int64_t, int64_t> bssListStateTable(context, metaInfo, new BinaryRowDataSerializer(1));
    auto* listState = new BssListState<RowData*, int64_t, int64_t>(&bssListStateTable, serializer, serializer);
    initialListState(listState);
    listState->setCurrentNamespace(1000);
    listState->add(10);
    auto list = listState->get();
    EXPECT_EQ(list->size(), 1);
    EXPECT_EQ((*list)[0], 10);
    listState->add(10);
    listState->add(20);

    list = listState->get();
    LOG("list size " << list->size());
    EXPECT_EQ(list->size(), 3);
    EXPECT_EQ((*list)[1], 10);
    EXPECT_EQ((*list)[2], 20);
}

TEST(BssStateTest, BssMapState_putNget)
{
    LongSerializer* serializer = LongSerializer::INSTANCE;
    // Initialize the InternalKeyContext
    InternalKeyContextImpl<RowData*>* context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 1);
    BinaryRowData* keyRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData->setLong(0, 100);
    context->setCurrentKey(keyRowData);
    context->setCurrentKeyGroupIndex(1);
    auto* metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", serializer, serializer);
    BssMapStateTable<RowData*, int64_t, int64_t, int64_t> bssMapStateTable(
        context, new BinaryRowDataSerializer(1), serializer, metaInfo);
    auto* mapState = new BssMapState<RowData*, int64_t, int64_t, int64_t>(
        &bssMapStateTable, new BinaryRowDataSerializer(1), serializer, serializer);
    initialMapState(mapState);
    mapState->setCurrentNamespace(1000);
    mapState->put(10, 10);
    auto* entries = mapState->entries();
    LOG("map size is " << entries->size());
    auto val = mapState->get(10);
    EXPECT_EQ(val, 10);
    mapState->put(10, 20);
    entries = mapState->entries();
    LOG("map size is " << entries->size());
    val = mapState->get(10);
    EXPECT_EQ(val, 20);
    mapState->put(20, 10);
    entries = mapState->entries();
    LOG("map size is " << entries->size());
    val = mapState->get(20);
    EXPECT_EQ(val, 10);
    mapState->put(20, 20);
    entries = mapState->entries();
    LOG("map size is " << entries->size());
    val = mapState->get(20);
    EXPECT_EQ(val, 20);
}

TEST(BssStateTest, BssMapState_putNentries)
{
    LongSerializer* serializer = LongSerializer::INSTANCE;
    // Initialize the InternalKeyContext
    InternalKeyContextImpl<RowData*>* context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 1);
    BinaryRowData* keyRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData->setLong(0, 100);
    context->setCurrentKey(keyRowData);
    context->setCurrentKeyGroupIndex(1);
    auto* metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", serializer, serializer);
    BssMapStateTable<RowData*, int64_t, int64_t, int64_t> bssMapStateTable(
        context, new BinaryRowDataSerializer(1), serializer, metaInfo);
    auto* mapState = new BssMapState<RowData*, int64_t, int64_t, int64_t>(
        &bssMapStateTable, new BinaryRowDataSerializer(1), serializer, serializer);
    initialMapState(mapState);
    mapState->setCurrentNamespace(1000);
    mapState->put(10, 10);
    auto* entries = mapState->entries();
    LOG("map size is " << entries->size());
    LOG("================================================================================");
    mapState->put(20, 10);
    entries = mapState->entries();
    LOG("map size is " << entries->size());
    LOG("================================================================================");
    mapState->put(10, 20);

    mapState->put(30, 35);
    entries = mapState->entries();
    LOG("map size is " << entries->size());
    mapState->remove(30);
    entries = mapState->entries();
    LOG("map size is " << entries->size());
    LOG("================================================================================");

    EXPECT_EQ(entries->size(), 2);
    for (auto& pair : *entries) {
        LOG("key : " << pair.first << ", value : " << pair.second);
    }
    EXPECT_EQ(entries->at(20), 10);
    EXPECT_EQ(entries->at(10), 20);
}

TEST(BssStateTest, BssListState_vectorbatch)
{
    LongSerializer* serializer = LongSerializer::INSTANCE;
    // Initialize the InternalKeyContext
    InternalKeyContextImpl<RowData*>* context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 2);
    BinaryRowData* keyRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData->setLong(0, 100);
    context->setCurrentKey(keyRowData);
    context->setCurrentKeyGroupIndex(1);
    auto* metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", serializer, serializer);
    BssListStateTable<RowData*, int64_t, int64_t> bssListStateTable(context, metaInfo, new BinaryRowDataSerializer(1));
    auto* listState = new BssListState<RowData*, int64_t, int64_t>(&bssListStateTable, serializer, serializer);
    initialListState(listState);
    listState->setCurrentNamespace(1000);
    omnistream::VectorBatch* tmpBatch = GetVectorBatch(vecBatchRows);
    auto keyGroup = context->getCurrentKeyGroupIndex();
    auto sequenceNumber = listState->getNextSequenceNumber(keyGroup);
    listState->addVectorBatch(keyGroup, tmpBatch);
    LOG("vectorbatch sequenceNumber " << sequenceNumber);
    auto res = listState->getVectorBatch(keyGroup, sequenceNumber);
    LOG("vectorbatch row : " << tmpBatch->GetRowCount() << ", vector : " << tmpBatch->GetVectorCount());
    for (int i = 0; i < tmpBatch->GetVectorCount(); ++i) {
        for (int j = 0; j < tmpBatch->GetRowCount(); ++j) {
            EXPECT_EQ(tmpBatch->GetValueAt<long>(i, j), res->GetValueAt<long>(i, j));
        }
    }

    auto* tmpBatch2 = GetVectorBatch(vecBatchRows);
    auto* tmpBatch3 = GetVectorBatch(vecBatchRows);
    std::unordered_map<int32_t, omnistream::VectorBatch*> batchesByKeyGroup = {
        {keyGroup, tmpBatch2}, {keyGroup - 1, tmpBatch3}};
    listState->addVectorBatches(batchesByKeyGroup);
    EXPECT_EQ(listState->getNextSequenceNumber(keyGroup), 2);
    EXPECT_EQ(listState->getNextSequenceNumber(keyGroup - 1), 1);
    auto storedBatches = listState->getVectorBatches(keyGroup);
    ASSERT_EQ(storedBatches.size(), 2);
    EXPECT_EQ(storedBatches.at(1)->GetValueAt<long>(0, 0), tmpBatch2->GetValueAt<long>(0, 0));
    for (auto* storedBatch : storedBatches) {
        delete storedBatch;
    }
    auto storedBatchesInOtherKeyGroup = listState->getVectorBatches(keyGroup - 1);
    ASSERT_EQ(storedBatchesInOtherKeyGroup.size(), 1);
    EXPECT_EQ(storedBatchesInOtherKeyGroup.at(0)->GetValueAt<long>(0, 0), tmpBatch3->GetValueAt<long>(0, 0));
    delete storedBatchesInOtherKeyGroup.at(0);
    delete tmpBatch2;
    delete tmpBatch3;
}

TEST(BssStateTest, BssMapState_vectorbatch)
{
    LongSerializer* serializer = LongSerializer::INSTANCE;
    // Initialize the InternalKeyContext
    InternalKeyContextImpl<RowData*>* context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 2);
    BinaryRowData* keyRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData->setLong(0, 100);
    context->setCurrentKey(keyRowData);
    context->setCurrentKeyGroupIndex(1);
    auto* metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", serializer, serializer);
    BssMapStateTable<RowData*, int64_t, int64_t, int64_t> bssMapStateTable(
        context, new BinaryRowDataSerializer(1), serializer, metaInfo);
    auto* mapState =
        new BssMapState<RowData*, int64_t, int64_t, int64_t>(&bssMapStateTable, serializer, serializer, serializer);
    initialMapState(mapState);
    mapState->setCurrentNamespace(1000);
    omnistream::VectorBatch* tmpBatch = GetVectorBatch(vecBatchRows);
    auto keyGroup = context->getCurrentKeyGroupIndex();
    auto sequenceNumber = mapState->getNextSequenceNumber(keyGroup);
    EXPECT_EQ(sequenceNumber, 0);
    mapState->addVectorBatch(keyGroup, tmpBatch);
    EXPECT_EQ(mapState->getNextSequenceNumber(keyGroup), 1);
    LOG("vectorbatch sequenceNumber " << sequenceNumber);
    auto res = mapState->getVectorBatch(keyGroup, sequenceNumber);
    LOG("vectorbatch row : " << tmpBatch->GetRowCount() << ", vector : " << tmpBatch->GetVectorCount());
    for (int i = 0; i < tmpBatch->GetVectorCount(); ++i) {
        for (int j = 0; j < tmpBatch->GetRowCount(); ++j) {
            EXPECT_EQ(tmpBatch->GetValueAt<long>(i, j), res->GetValueAt<long>(i, j));
        }
    }

    omnistream::VectorBatch* tmpBatch2 = GetVectorBatch(vecBatchRows);
    auto sequenceNumber1 = mapState->getNextSequenceNumber(keyGroup);
    mapState->addVectorBatch(keyGroup, tmpBatch2);
    LOG("vectorbatch sequenceNumber " << sequenceNumber1);
    auto res1 = mapState->getVectorBatch(keyGroup, sequenceNumber1);
    LOG("vectorbatch row : " << tmpBatch2->GetRowCount() << ", vector : " << tmpBatch->GetVectorCount());
    for (int i = 0; i < tmpBatch2->GetVectorCount(); ++i) {
        for (int j = 0; j < tmpBatch2->GetRowCount(); ++j) {
            EXPECT_EQ(tmpBatch2->GetValueAt<long>(i, j), res1->GetValueAt<long>(i, j));
        }
    }

    // sequenceNumber is scoped to a keyGroup; the same sequenceNumber in another
    // keyGroup must address a different VectorBatchId.
    int32_t otherKeyGroup = keyGroup - 1;
    auto otherSequenceNumber = mapState->getNextSequenceNumber(otherKeyGroup);
    EXPECT_EQ(otherSequenceNumber, 0);
    auto* otherBatch = GetVectorBatch(vecBatchRows);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(otherBatch->Get(0))->SetValue(0, 999);
    mapState->addVectorBatch(otherKeyGroup, otherBatch);
    EXPECT_EQ(mapState->getNextSequenceNumber(otherKeyGroup), 1);
    EXPECT_EQ(mapState->getNextSequenceNumber(keyGroup), 2);
    auto* otherResult = mapState->getVectorBatch(otherKeyGroup, otherSequenceNumber);
    EXPECT_EQ(otherResult->GetValueAt<long>(0, 0), 999);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, sequenceNumber)->GetValueAt<long>(0, 0), 100);
}
#endif
