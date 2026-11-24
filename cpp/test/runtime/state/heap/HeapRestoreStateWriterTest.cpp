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

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "core/typeutils/VoidSerializer.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/OperatorRuntimeStateSchemaProvider.h"
#include "runtime/state/heap/HeapRestoreBackendDelegate.h"
#include "runtime/state/heap/HeapSingleStateIterator.h"
#include "runtime/state/heap/HeapRestoreKVState.h"
#include "runtime/state/heap/HeapRestorePQState.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/RestoreKVState.h"
#include "runtime/state/restore/RestorePQState.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"

using namespace omnistream;

namespace {

// ============================================================================
// Fixture — 创建一个最小 HeapKeyedStateBackend<int> 供 restore writer 测试使用
// ============================================================================

class HeapRestoreStateWriterTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        range_ = std::make_unique<KeyGroupRange>(0, 10);
        context_ = std::make_unique<InternalKeyContextImpl<int>>(range_.get(), 10);
        keySerializer_ = std::make_shared<IntSerializer>();
        backend_ = std::make_unique<HeapKeyedStateBackend<int>>(new IntSerializer(), context_.get());
        delegate_ = std::make_unique<HeapRestoreBackendDelegate<int>>(
            backend_.get(), keySerializer_, /*keyGroupPrefixBytes=*/2);
    }

    void TearDown() override
    {
        delegate_.reset();
        backend_.reset();
        keySerializer_.reset();
        context_.reset();
        range_.reset();
    }

    // 构造 VALUE 类型 StateMetaInfoSnapshot。
    // 默认使用 LongSerializer::INSTANCE 作为 VALUE_SERIALIZER，对应 BIGINT_BK backend。
    StateMetaInfoSnapshot makeKvMetaInfo(const std::string& name = "testState")
    {
        std::unordered_map<std::string, std::string> options;
        options["KEYED_STATE_TYPE"] = "VALUE";

        std::unordered_map<std::string, TypeSerializer*> serializers;
        serializers["VALUE_SERIALIZER"] = LongSerializer::INSTANCE;
        serializers["NAMESPACE_SERIALIZER"] = VoidNamespaceSerializer::INSTANCE;

        return StateMetaInfoSnapshot(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    // 构造 LIST 类型 StateMetaInfoSnapshot（BIGINT_BK 元素）。
    StateMetaInfoSnapshot makeListMetaInfo(const std::string& name = "testListState")
    {
        std::unordered_map<std::string, std::string> options;
        options["KEYED_STATE_TYPE"] = "LIST";

        auto* listSer = new ListSerializer(new LongSerializer());
        serializersToClean_.emplace_back(listSer);

        std::unordered_map<std::string, TypeSerializer*> serializers;
        serializers["NAMESPACE_SERIALIZER"] = VoidNamespaceSerializer::INSTANCE;
        serializers["VALUE_SERIALIZER"] = listSer;

        return StateMetaInfoSnapshot(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    std::unique_ptr<KeyGroupRange> range_;
    std::unique_ptr<InternalKeyContextImpl<int>> context_;
    std::shared_ptr<IntSerializer> keySerializer_;
    std::unique_ptr<HeapKeyedStateBackend<int>> backend_;
    std::unique_ptr<HeapRestoreBackendDelegate<int>> delegate_;
    // 跟踪所有 heap-allocated 的 serializer，在 fixture 析构时自动释放
    std::vector<std::unique_ptr<TypeSerializer>> serializersToClean_;
};

// ============================================================================
// HeapRestorePQState 测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, PqStateConstructsAndFlushesWithoutThrowing)
{
    HeapRestorePQState<int> pqState(backend_.get(), "pqState", 2);

    EXPECT_NO_THROW(pqState.flush());
    EXPECT_NO_THROW(pqState.discard());
}

// ============================================================================
// HeapRestoreBackendDelegate 工厂方法测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, DelegateCreatePqStateReturnsNonNull)
{
    auto metaInfo = StateMetaInfoSnapshot(
        "pqState",
        StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        std::unordered_map<std::string, TypeSerializer*>{});

    auto pq = delegate_->createPQState(0, metaInfo);
    ASSERT_NE(pq, nullptr);
}

TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateReturnsNonNull)
{
    auto metaInfo = makeKvMetaInfo("kvState");
    auto kv = delegate_->createKVState(0, metaInfo);
    ASSERT_NE(kv, nullptr);
}

TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateRegistersStateInfo)
{
    auto metaInfo = makeKvMetaInfo("kvState");

    // 首次创建注册 state info
    auto kv = delegate_->createKVState(0, metaInfo);
    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].stateName, "kvState");
}

// 看护 LIST 类型注册：mainStateDesc 正确创建
TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateRegistersListStateInfo)
{
    auto metaInfo = makeListMetaInfo("testList");
    auto kv = delegate_->createKVState(0, metaInfo);

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    auto& info = delegate_->getStateInfos()[0];
    EXPECT_EQ(info.stateType, StateDescriptor::Type::LIST);
    EXPECT_NE(info.mainStateDesc, nullptr);
}

// ============================================================================
// HeapRestoreKVState 写操作测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateFlushAndDiscardDoNotThrow)
{
    auto metaInfo = makeKvMetaInfo("flushState");
    auto kv = delegate_->createKVState(0, metaInfo);

    EXPECT_NO_THROW(kv->flush());
    EXPECT_NO_THROW(kv->discard());
}

TEST_F(HeapRestoreStateWriterTest, KvStateSetKeyGroupIdDoesNotThrow)
{
    auto metaInfo = makeKvMetaInfo("kgState");
    auto kv = delegate_->createKVState(0, metaInfo);

    EXPECT_NO_THROW(kv->setKeyGroupId(5));
}

// ============================================================================
// 辅助函数：构造测试用的 key bytes
// ============================================================================

static std::vector<int8_t> makeKeyBytes(int key, int keyGroupId, int keyGroupPrefixBytes)
{
    DataOutputSerializer out(16);
    for (int i = keyGroupPrefixBytes - 1; i >= 0; --i) {
        out.writeByte(static_cast<uint8_t>((keyGroupId >> (i * 8)) & 0xff));
    }
    out.writeInt(static_cast<uint32_t>(key));
    out.writeByte(0); // VoidNamespace
    std::vector<int8_t> result(out.length());
    std::memcpy(result.data(), out.getData(), out.length());
    return result;
}

static std::vector<int8_t> makeLongValueBytes(int64_t value)
{
    DataOutputSerializer out(8);
    out.writeLong(value);
    std::vector<int8_t> result(out.length());
    std::memcpy(result.data(), out.getData(), out.length());
    return result;
}

static std::vector<int8_t> makeIntValueBytes(int32_t value)
{
    DataOutputSerializer out(4);
    out.writeInt(static_cast<uint32_t>(value));
    std::vector<int8_t> result(out.length());
    std::memcpy(result.data(), out.getData(), out.length());
    return result;
}

// 构造带 map key 后缀的 composite key bytes
// 格式: keyGroupPrefix(2) + stateKey(4) + namespace(1) + mapKey(N)
static std::vector<int8_t> makeKeyBytesWithMapKey(
    int key, int keyGroupId, int keyGroupPrefixBytes, const std::vector<int8_t>& mapKeySuffix)
{
    auto base = makeKeyBytes(key, keyGroupId, keyGroupPrefixBytes);
    base.insert(base.end(), mapKeySuffix.begin(), mapKeySuffix.end());
    return base;
}

// 带指定 valueSerializer 的 VALUE 类型 MetaInfo
StateMetaInfoSnapshot makeKvMetaInfoWithSerializer(
    const std::string& name, TypeSerializer* valSerializer, const std::string& stateTypeStr = "VALUE")
{
    std::unordered_map<std::string, std::string> options;
    options["KEYED_STATE_TYPE"] = stateTypeStr;

    std::unordered_map<std::string, TypeSerializer*> serializers;
    serializers["NAMESPACE_SERIALIZER"] = VoidNamespaceSerializer::INSTANCE;
    if (valSerializer != nullptr) {
        serializers["VALUE_SERIALIZER"] = valSerializer;
    }

    return StateMetaInfoSnapshot(
        name,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        options,
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        serializers);
}

// ============================================================================
// HeapRestoreKVState writeValueEntry 测试 — BIGINT_BK backend
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateWriteValueEntryBigintBackend)
{
    auto metaInfo = makeKvMetaInfo("bigintValue");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(0);

    auto keyBytes = makeKeyBytes(42, 0, 2);
    auto valueBytes = makeLongValueBytes(100);

    // 通过 writeBytesEntry 写入: 路由到 writeValueEntry → BIGINT_BK
    EXPECT_NO_THROW(kv->writeEntry(keyBytes, ByteView(valueBytes.data(), valueBytes.size())));

    // Verify entry count
    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);
    EXPECT_NE(delegate_->getStateInfos()[0].mainTablePtr, 0);

    // Verify restored value: key=42, value=100L
    auto* restoredTable = reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, int64_t>*>(
        delegate_->getStateInfos()[0].mainTablePtr);
    ASSERT_NE(restoredTable, nullptr);
    EXPECT_EQ(restoredTable->get(42, 0, VoidNamespace()), 100L);
}

TEST_F(HeapRestoreStateWriterTest, SavepointSnapshotsExcludeRegisteredVbSideTables)
{
    auto metaInfo = makeKvMetaInfo("rowState");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(0);
    const auto keyBytes = makeKeyBytes(42, 0, 2);
    const auto valueBytes = makeLongValueBytes(100);
    kv->writeEntry(keyBytes, ByteView(valueBytes.data(), valueBytes.size()));
    ASSERT_NE(backend_->getStateTablePtr("rowStatevb"), 0U);

    for (const auto& savepointResources : {backend_->savepoint(), backend_->compatibleSavepoint()}) {
        const auto& resources = savepointResources->getSnapshotResources();
        const auto& metaInfos = resources->getMetaInfoSnapshots();
        ASSERT_EQ(metaInfos.size(), 1U);
        EXPECT_EQ(metaInfos[0]->getName(), "rowState");

        auto iterator = resources->createKVStateIterator();
        ASSERT_TRUE(iterator->isValid());
        EXPECT_EQ(iterator->current().kvStateId, 0);
        EXPECT_EQ(iterator->current().value.size(), valueBytes.size());
        EXPECT_EQ(std::vector<int8_t>(iterator->current().value.begin(), iterator->current().value.end()), valueBytes);
        iterator->next();
        EXPECT_FALSE(iterator->isValid());
        iterator->close();
        resources->cleanup();
    }
}

// ============================================================================
// HeapRestoreKVState writeValueEntry 测试 — INT_BK backend
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateWriteValueEntryIntBackend)
{
    auto* intSer = new IntSerializer();
    serializersToClean_.emplace_back(intSer);
    auto metaInfo = makeKvMetaInfoWithSerializer("intValue", intSer, "VALUE");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(0);

    auto keyBytes = makeKeyBytes(7, 0, 2);
    auto valueBytes = makeIntValueBytes(999);

    EXPECT_NO_THROW(kv->writeEntry(keyBytes, ByteView(valueBytes.data(), valueBytes.size())));

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);

    // Verify restored value: key=7, value=999
    auto* restoredTable =
        reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, int>*>(delegate_->getStateInfos()[0].mainTablePtr);
    ASSERT_NE(restoredTable, nullptr);
    EXPECT_EQ(restoredTable->get(7, 0, VoidNamespace()), 999);
}

// ============================================================================
// HeapRestoreKVState writeBytesEntry 路由测试 — LIST 类型路由到 writeListEntry
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateWriteBytesEntryRoutesToListEntry)
{
    auto metaInfo = makeListMetaInfo("testList");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(0);

    auto keyBytes = makeKeyBytes(2, 0, 2);

    // 构造 LIST value bytes: elem1(8bytes) + ',' + elem2(8bytes)（ListDelimitedSerializer 逗号分隔，无 size 前缀）
    DataOutputSerializer valOut(32);
    valOut.writeLong(100L);
    valOut.writeByte(static_cast<uint8_t>(','));
    valOut.writeLong(200L);
    std::vector<int8_t> listValueBytes(valOut.length());
    std::memcpy(listValueBytes.data(), valOut.getData(), valOut.length());

    // writeBytesEntry 检测 stateType=LIST 路由到 writeListEntry
    EXPECT_NO_THROW(kv->writeEntry(keyBytes, ByteView(listValueBytes.data(), listValueBytes.size())));

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);

    // Verify restored list data: element values must be {100L, 200L}
    auto* restoredTable = reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, std::vector<int64_t>*>*>(
        delegate_->getStateInfos()[0].mainTablePtr);
    ASSERT_NE(restoredTable, nullptr);
    auto* restoredList = restoredTable->get(2, 0, VoidNamespace());
    ASSERT_NE(restoredList, nullptr);
    ASSERT_EQ(restoredList->size(), 2u);
    EXPECT_EQ((*restoredList)[0], 100L);
    EXPECT_EQ((*restoredList)[1], 200L);
}

// ============================================================================
// HeapRestoreKVState writeMapEntry 测试 — INT+INT map
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateWriteMapEntryIntInt)
{
    // 构造 MAP 类型: IntSerializer + IntSerializer
    auto* mapSer = new MapSerializer(new IntSerializer(), new IntSerializer());
    serializersToClean_.emplace_back(mapSer);
    auto metaInfo = makeKvMetaInfoWithSerializer("mapIntInt", mapSer, "MAP");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(0);

    // 构造 composite key: keyGroupPrefix(2) + stateKey(4) + ns(1) + mapKey(INT=4)
    DataOutputSerializer mapKeyOut(4);
    mapKeyOut.writeInt(100); // INT_BK map key
    std::vector<int8_t> mapKeySuffix(mapKeyOut.length());
    std::memcpy(mapKeySuffix.data(), mapKeyOut.getData(), mapKeyOut.length());
    auto keyBytes = makeKeyBytesWithMapKey(3, 0, 2, mapKeySuffix);

    // value bytes: readBoolean(false) + mapValue(int)
    DataOutputSerializer valOut(8);
    valOut.writeBoolean(false);
    valOut.writeInt(200); // map value
    std::vector<int8_t> mapValueBytes(valOut.length());
    std::memcpy(mapValueBytes.data(), valOut.getData(), valOut.length());

    // 通过 writeBytesEntry 路由到 writeMapEntry
    EXPECT_NO_THROW(kv->writeEntry(keyBytes, ByteView(mapValueBytes.data(), mapValueBytes.size())));

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);
}

// ============================================================================
// HeapRestoreKVState writeMapEntry 测试 — BIGINT+BIGINT map
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateWriteMapEntryBigintBigint)
{
    auto* mapSer = new MapSerializer(new LongSerializer(), new LongSerializer());
    serializersToClean_.emplace_back(mapSer);
    auto metaInfo = makeKvMetaInfoWithSerializer("mapBigintBigint", mapSer, "MAP");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(0);

    // 构造 composite key: keyGroupPrefix(2) + stateKey(4) + ns(1) + mapKey(BIGINT=8)
    DataOutputSerializer mapKeyOut(8);
    mapKeyOut.writeLong(1000L); // BIGINT_BK map key
    std::vector<int8_t> mapKeySuffix(mapKeyOut.length());
    std::memcpy(mapKeySuffix.data(), mapKeyOut.getData(), mapKeyOut.length());
    auto keyBytes = makeKeyBytesWithMapKey(4, 0, 2, mapKeySuffix);

    // value bytes: readBoolean(false) + mapValue(BIGINT)
    DataOutputSerializer valOut(12);
    valOut.writeBoolean(false);
    valOut.writeLong(2000L); // map value
    std::vector<int8_t> mapValueBytes(valOut.length());
    std::memcpy(mapValueBytes.data(), valOut.getData(), valOut.length());

    EXPECT_NO_THROW(kv->writeEntry(keyBytes, ByteView(mapValueBytes.data(), mapValueBytes.size())));

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);
}

// Flink SP metadata keeps ROW_BK, while StreamingJoin owns shared_ptr<RowData>
// in memory. The restore writer must dispatch by the runtime table descriptor.
TEST_F(HeapRestoreStateWriterTest, KvStateWriteMapEntryUsesRuntimeSchemaForSharedRowKey)
{
    nlohmann::json operatorDescription{
        {"joinType", "InnerJoin"}, {"leftInputSpec", "NoUniqueKey"}, {"rightInputSpec", "NoUniqueKey"}};
    auto provider = OperatorRuntimeStateSchemaProviderFactory::create(operatorDescription);
    ASSERT_NE(provider, nullptr);
    HeapRestoreBackendDelegate<int> runtimeDelegate(backend_.get(), keySerializer_, 2, provider.get());

    omnistream::RowType rowType(true, std::vector<std::string>{"BIGINT"});
    auto* mapSer = new MapSerializer(new RowDataSerializer(&rowType), new IntSerializer());
    serializersToClean_.emplace_back(mapSer);
    auto metaInfo = makeKvMetaInfoWithSerializer("left-records", mapSer, "MAP");
    auto kv = runtimeDelegate.createKVState(0, metaInfo);
    kv->setKeyGroupId(0);

    const auto& stateInfo = runtimeDelegate.getStateInfos()[0];
    ASSERT_NE(stateInfo.mainStateDesc, nullptr);
    EXPECT_EQ(mapSer->getKeySerializer()->getBackendId(), BackendDataType::ROW_BK);
    EXPECT_FALSE(mapSer->getKeySerializer()->isReusable());
    EXPECT_EQ(stateInfo.mainStateDesc->getKeyDataId(), BackendDataType::SHARED_ROW_BK);

    for (int32_t value : {200, 201}) {
        std::unique_ptr<BinaryRowData> mapKey(BinaryRowData::createBinaryRowDataWithMem(1));
        mapKey->setLong(0, value - 100L);
        DataOutputSerializer mapKeyOut(32);
        mapSer->getKeySerializer()->serialize(mapKey.get(), mapKeyOut);
        std::vector<int8_t> mapKeySuffix(mapKeyOut.length());
        std::memcpy(mapKeySuffix.data(), mapKeyOut.getData(), mapKeyOut.length());
        auto keyBytes = makeKeyBytesWithMapKey(3, 0, 2, mapKeySuffix);

        DataOutputSerializer valOut(8);
        valOut.writeBoolean(false);
        valOut.writeInt(value);
        std::vector<int8_t> mapValueBytes(valOut.length());
        std::memcpy(mapValueBytes.data(), valOut.getData(), valOut.length());

        ASSERT_NO_THROW(kv->writeEntry(keyBytes, ByteView(mapValueBytes.data(), mapValueBytes.size())));
    }

    using SharedRowMap = emhash7::HashMap<std::shared_ptr<RowData>, int32_t>;
    auto* table = reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, SharedRowMap*>*>(stateInfo.mainTablePtr);
    ASSERT_NE(table, nullptr);
    VoidNamespace ns;
    auto* restoredMap = table->get(3, 0, ns);
    ASSERT_NE(restoredMap, nullptr);
    ASSERT_EQ(restoredMap->size(), 2u);
    for (const auto& entry : *restoredMap) {
        EXPECT_NE(entry.first, nullptr);
        EXPECT_TRUE(entry.second == 200 || entry.second == 201);
    }
}

// ============================================================================
// SHARED_ROW MAP 状态：写侧 per-entry 快照序列化 + 恢复回环测试
// 验证 HeapSingleStateIterator::serializeMapEntriesPerEntry 对
// shared_ptr<RowData> map key 的处理（序列化 pointee row，而非 shared_ptr 对象字节）：
//   key   = [keyGroupPrefix][sourceKey][namespace][mapKey(row bytes)]
//   value = [null_bool(false)][mapValue]
// 并将序列化结果回灌恢复侧，验证端到端数据一致。
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, SharedRowMapSnapshotSerializePerEntryAndRestoreRoundTrip)
{
    const int keyGroupId = 5;
    const int sourceKey = 3;
    const int keyGroupPrefixBytes = 2;
    using SharedRowMap = emhash7::HashMap<std::shared_ptr<RowData>, int32_t>;

    omnistream::RowType rowType(true, std::vector<std::string>{"BIGINT"});
    auto* mapSer = new MapSerializer(new RowDataSerializer(&rowType), new IntSerializer());
    serializersToClean_.emplace_back(mapSer);

    // 构造期望的序列化字节（mapKey 用 RowDataSerializer 序列化 pointee row）
    const std::vector<std::pair<int64_t, int32_t>> mapEntries{{100L, 1000}, {200L, 2000}};
    std::vector<std::pair<std::vector<int8_t>, std::vector<int8_t>>> expected;
    for (const auto& [rowValue, mapValue] : mapEntries) {
        std::shared_ptr<RowData> row(BinaryRowData::createBinaryRowDataWithMem(1));
        static_cast<BinaryRowData*>(row.get())->setLong(0, rowValue);

        DataOutputSerializer mapKeyOut(32);
        mapSer->getKeySerializer()->serialize(row.get(), mapKeyOut);
        std::vector<int8_t> mapKeySuffix(mapKeyOut.length());
        std::memcpy(mapKeySuffix.data(), mapKeyOut.getData(), mapKeyOut.length());
        auto keyBytes = makeKeyBytesWithMapKey(sourceKey, keyGroupId, keyGroupPrefixBytes, mapKeySuffix);

        DataOutputSerializer valOut(8);
        valOut.writeBoolean(false); // null_bool
        valOut.writeInt(static_cast<uint32_t>(mapValue));
        std::vector<int8_t> valBytes(valOut.length());
        std::memcpy(valBytes.data(), valOut.getData(), valOut.length());

        expected.emplace_back(std::move(keyBytes), std::move(valBytes));
    }

    // Phase 1: 构造含 shared_ptr<RowData> key 的 MAP 状态表
    auto* tableMetaInfo = new RegisteredKeyValueStateBackendMetaInfo(
        StateDescriptor::Type::MAP, "left-records", new VoidNamespaceSerializer(), mapSer);
    CopyOnWriteStateTable<int, VoidNamespace, SharedRowMap*> sourceTable(
        context_.get(), tableMetaInfo, IntSerializer::INSTANCE);
    {
        auto* kvMap = new SharedRowMap();
        for (const auto& [rowValue, mapValue] : mapEntries) {
            std::shared_ptr<RowData> row(BinaryRowData::createBinaryRowDataWithMem(1));
            static_cast<BinaryRowData*>(row.get())->setLong(0, rowValue);
            (*kvMap)[row] = mapValue;
        }
        sourceTable.put(sourceKey, keyGroupId, VoidNamespace(), kvMap);
    }

    // Phase 2: 写侧快照序列化，逐字节比对
    std::vector<std::pair<std::vector<int8_t>, std::vector<int8_t>>> actual;
    {
        HeapSingleStateIterator<int, VoidNamespace, SharedRowMap*> iterator(&sourceTable, 0, keyGroupPrefixBytes);
        // per-entry 布局：每个 map entry 输出一条状态 entry
        ASSERT_EQ(iterator.getEntryCount(), mapEntries.size());
        for (; iterator.isValid(); iterator.next()) {
            ASSERT_EQ(iterator.keyGroup(), keyGroupId);
            auto keyView = iterator.key();
            auto valView = iterator.value();
            actual.emplace_back(
                std::vector<int8_t>(keyView.data(), keyView.data() + keyView.size()),
                std::vector<int8_t>(valView.data(), valView.data() + valView.size()));
        }
    }
    auto entryLess = [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; };
    std::sort(expected.begin(), expected.end(), entryLess);
    std::sort(actual.begin(), actual.end(), entryLess);
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i].first, expected[i].first);
        EXPECT_EQ(actual[i].second, expected[i].second);
    }

    // Phase 3: 序列化字节回灌恢复侧（StreamingJoin schema: ROW_BK -> SHARED_ROW_BK），验证回环
    nlohmann::json operatorDescription{
        {"joinType", "InnerJoin"}, {"leftInputSpec", "NoUniqueKey"}, {"rightInputSpec", "NoUniqueKey"}};
    auto provider = OperatorRuntimeStateSchemaProviderFactory::create(operatorDescription);
    ASSERT_NE(provider, nullptr);
    HeapRestoreBackendDelegate<int> runtimeDelegate(
        backend_.get(), keySerializer_, keyGroupPrefixBytes, provider.get());

    auto metaInfoSnap = makeKvMetaInfoWithSerializer("left-records", mapSer, "MAP");
    auto kv = runtimeDelegate.createKVState(0, metaInfoSnap);
    kv->setKeyGroupId(keyGroupId);
    for (const auto& [keyBytes, valBytes] : actual) {
        EXPECT_NO_THROW(kv->writeEntry(keyBytes, ByteView(valBytes.data(), valBytes.size())));
    }

    const auto& stateInfo = runtimeDelegate.getStateInfos()[0];
    EXPECT_EQ(stateInfo.mainEntryCount, static_cast<int>(mapEntries.size()));
    auto* restoredTable =
        reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, SharedRowMap*>*>(stateInfo.mainTablePtr);
    ASSERT_NE(restoredTable, nullptr);
    auto* restoredMap = restoredTable->get(sourceKey, keyGroupId, VoidNamespace());
    ASSERT_NE(restoredMap, nullptr);
    ASSERT_EQ(restoredMap->size(), mapEntries.size());
    std::map<int64_t, int32_t> restored;
    for (const auto& entry : *restoredMap) {
        ASSERT_NE(entry.first, nullptr);
        restored[*(static_cast<BinaryRowData*>(entry.first.get())->getLong(0))] = entry.second;
    }
    for (const auto& [rowValue, mapValue] : mapEntries) {
        auto it = restored.find(rowValue);
        ASSERT_NE(it, restored.end());
        EXPECT_EQ(it->second, mapValue);
    }
}

// ============================================================================
// BIGINT LIST 状态：写侧快照序列化 + 恢复回环测试
// 验证 HeapSingleStateIterator::serializeValue 对 LIST 状态的处理：
// 序列化逗号分隔格式（ListDelimitedSerializer，无 size 前缀）：
//   value = [elem1_bytes][','][elem2_bytes][',']...
// 并将序列化结果回灌恢复侧，验证端到端数据一致。
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, BigintListSnapshotSerializeAndRestoreRoundTrip)
{
    const int keyGroupId = 5;
    const int sourceKey = 3;
    const int keyGroupPrefixBytes = 2;
    using BigintList = std::vector<int64_t>;

    auto* listSer = new ListSerializer(new LongSerializer());
    serializersToClean_.emplace_back(listSer);

    const std::vector<int64_t> testValues = {100L, 200L, 300L};

    // Phase 1: 构造 LIST 状态表
    auto* tableMetaInfo = new RegisteredKeyValueStateBackendMetaInfo(
        StateDescriptor::Type::LIST, "test-list", new VoidNamespaceSerializer(), listSer);
    CopyOnWriteStateTable<int, VoidNamespace, BigintList*> sourceTable(
        context_.get(), tableMetaInfo, IntSerializer::INSTANCE);
    {
        auto* list = new BigintList(testValues);
        sourceTable.put(sourceKey, keyGroupId, VoidNamespace(), list);
    }

    // Phase 2: 写侧快照序列化，逐字节比对
    // key 格式：[keyGroupPrefix(2)][sourceKey(IntSerializer=4)][VoidNamespace(1)]
    auto expectedKey = makeKeyBytes(sourceKey, keyGroupId, keyGroupPrefixBytes);

    // value 格式：[LongSerializer(100L)][','][LongSerializer(200L)][','][LongSerializer(300L)]
    DataOutputSerializer expectedValOut(32);
    LongSerializer* longSer = LongSerializer::INSTANCE;
    for (size_t i = 0; i < testValues.size(); i++) {
        if (i > 0) {
            expectedValOut.writeByte(static_cast<uint8_t>(','));
        }
        int64_t val = testValues[i];
        longSer->serialize(&val, expectedValOut);
    }
    std::vector<int8_t> expectedValue(expectedValOut.length());
    std::memcpy(expectedValue.data(), expectedValOut.getData(), expectedValOut.length());

    // 执行序列化
    HeapSingleStateIterator<int, VoidNamespace, BigintList*> iterator(&sourceTable, 0, keyGroupPrefixBytes);
    ASSERT_EQ(iterator.getEntryCount(), 1u);
    ASSERT_TRUE(iterator.isValid());
    EXPECT_EQ(iterator.keyGroup(), keyGroupId);
    auto keyView = iterator.key();
    auto valView = iterator.value();
    std::vector<int8_t> actualKey(keyView.data(), keyView.data() + keyView.size());
    std::vector<int8_t> actualValue(valView.data(), valView.data() + valView.size());
    EXPECT_EQ(actualKey, expectedKey);
    EXPECT_EQ(actualValue, expectedValue);

    // Phase 3: 序列化字节回灌恢复侧，验证回环数据一致
    auto metaInfoSnap = makeKvMetaInfoWithSerializer("test-list", listSer, "LIST");
    auto kv = delegate_->createKVState(0, metaInfoSnap);
    kv->setKeyGroupId(keyGroupId);
    EXPECT_NO_THROW(kv->writeEntry(actualKey, ByteView(actualValue.data(), actualValue.size())));

    const auto& stateInfo = delegate_->getStateInfos()[0];
    EXPECT_EQ(stateInfo.mainEntryCount, 1);
    auto* restoredTable =
        reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, BigintList*>*>(stateInfo.mainTablePtr);
    ASSERT_NE(restoredTable, nullptr);
    auto* restoredList = restoredTable->get(sourceKey, keyGroupId, VoidNamespace());
    ASSERT_NE(restoredList, nullptr);
    ASSERT_EQ(restoredList->size(), testValues.size());
    for (size_t i = 0; i < testValues.size(); i++) {
        EXPECT_EQ((*restoredList)[i], testValues[i]);
    }
}

// ============================================================================
// HeapRestoreKVState 多次写入测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateWriteMultipleEntriesIncrementsCount)
{
    auto metaInfo = makeKvMetaInfo("multiEntry");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(0);

    // 写入 3 条不同的 key
    auto valueBytes = makeLongValueBytes(42);
    for (int k = 0; k < 3; ++k) {
        auto keyBytes = makeKeyBytes(k, 0, 2);
        EXPECT_NO_THROW(kv->writeEntry(keyBytes, ByteView(valueBytes.data(), valueBytes.size())));
    }

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 3);
}

// ============================================================================
// HeapRestoreKVState 不同 keyGroup 写入测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateWriteWithDifferentKeyGroups)
{
    auto metaInfo = makeKvMetaInfo("kgMulti");
    auto kv = delegate_->createKVState(0, metaInfo);
    auto valueBytes = makeLongValueBytes(42);

    // 写入 keyGroup 0
    kv->setKeyGroupId(0);
    auto keyBytes0 = makeKeyBytes(1, 0, 2);
    EXPECT_NO_THROW(kv->writeEntry(keyBytes0, ByteView(valueBytes.data(), valueBytes.size())));

    // 写入 keyGroup 5
    kv->setKeyGroupId(5);
    auto keyBytes5 = makeKeyBytes(1, 5, 2);
    EXPECT_NO_THROW(kv->writeEntry(keyBytes5, ByteView(valueBytes.data(), valueBytes.size())));

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 2);
}

// ============================================================================
// HeapRestoreKVState flush 和 discard 多轮调用测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateFlushAndDiscardMultipleTimes)
{
    auto metaInfo = makeKvMetaInfo("flushMulti");
    auto kv = delegate_->createKVState(0, metaInfo);

    // 多轮 flush/discard 不应抛出异常
    for (int i = 0; i < 3; ++i) {
        EXPECT_NO_THROW(kv->flush());
        EXPECT_NO_THROW(kv->discard());
    }
}

// ============================================================================
// HeapRestoreKVState writeBytesEntry 路由测试 — VALUE 类型路由到 writeValueEntry
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateWriteBytesEntryRoutesToValueEntry)
{
    auto metaInfo = makeKvMetaInfo("valueRoute");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(0);

    auto keyBytes = makeKeyBytes(10, 0, 2);
    auto valueBytes = makeLongValueBytes(777);

    // writeBytesEntry 检测 stateType=VALUE 路由到 writeValueEntry
    EXPECT_NO_THROW(kv->writeEntry(keyBytes, ByteView(valueBytes.data(), valueBytes.size())));

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);

    // Verify restored value: key=10, value=777L
    auto* restoredTable = reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, int64_t>*>(
        delegate_->getStateInfos()[0].mainTablePtr);
    ASSERT_NE(restoredTable, nullptr);
    EXPECT_EQ(restoredTable->get(10, 0, VoidNamespace()), 777L);
}

// ============================================================================
// HeapRestoreKVState writeMapEntry 测试 — 空 map key/value serializer 应抛出异常
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateWriteMapEntryWithoutSerializersThrows)
{
    // 构造 MAP 类型但 valueSerializer 不是 MapSerializer
    std::unordered_map<std::string, std::string> options;
    options["KEYED_STATE_TYPE"] = "MAP";
    std::unordered_map<std::string, TypeSerializer*> serializers;
    serializers["NAMESPACE_SERIALIZER"] = VoidNamespaceSerializer::INSTANCE;
    auto* badSer = new LongSerializer(); // Not a MapSerializer!
    serializersToClean_.emplace_back(badSer);
    serializers["VALUE_SERIALIZER"] = badSer;

    auto metaInfo = StateMetaInfoSnapshot(
        "badMap",
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        options,
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        serializers);

    // createKVState 会调用 ensureStateRegistered，其中 dynamic_cast<MapSerializer*> 失败 → mapKeySerializer 为 nullptr
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(0);

    auto keyBytes = makeKeyBytes(1, 0, 2);
    DataOutputSerializer valOut(4);
    valOut.writeBoolean(false);
    valOut.writeInt(1);
    valOut.writeInt(2);
    std::vector<int8_t> mapValueBytes(valOut.length());
    std::memcpy(mapValueBytes.data(), valOut.getData(), valOut.length());

    // writeMapEntry 检测 mapKeySerializer/mapValueSerializer 为 nullptr 抛出异常
    EXPECT_THROW(kv->writeEntry(keyBytes, ByteView(mapValueBytes.data(), mapValueBytes.size())), std::runtime_error);
}

// ============================================================================
// HeapRestoreKVState writeMapEntry 测试 — null MAP value 抛出异常
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateWriteMapEntryNullValueThrows)
{
    // 构造正常 MAP 类型
    auto* mapSer = new MapSerializer(new IntSerializer(), new IntSerializer());
    serializersToClean_.emplace_back(mapSer);
    auto metaInfo = makeKvMetaInfoWithSerializer("mapNull", mapSer, "MAP");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(0);

    // composite key: keyGroupPrefix(2) + stateKey(4) + ns(1) + mapKey(INT=4)
    DataOutputSerializer mapKeyOut(4);
    mapKeyOut.writeInt(1);
    std::vector<int8_t> mapKeySuffix(mapKeyOut.length());
    std::memcpy(mapKeySuffix.data(), mapKeyOut.getData(), mapKeyOut.length());
    auto keyBytes = makeKeyBytesWithMapKey(1, 0, 2, mapKeySuffix);

    // value bytes: readBoolean(true) → null value → 抛出异常
    DataOutputSerializer valOut(2);
    valOut.writeBoolean(true); // null value!
    std::vector<int8_t> mapValueBytes(valOut.length());
    std::memcpy(mapValueBytes.data(), valOut.getData(), valOut.length());

    EXPECT_THROW(kv->writeEntry(keyBytes, ByteView(mapValueBytes.data(), mapValueBytes.size())), std::runtime_error);
}

// ============================================================================
// HeapRestorePQState 测试 — writeEntry
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, PqStateWriteEntry)
{
    // PQ state 不需要通过 delegate 创建，直接构造
    HeapRestorePQState<int> pqState(backend_.get(), "testPQ", 2);

    std::vector<int8_t> keyBytes = {0, 0, 0, 0, 0, 1};         // 简单 key
    std::vector<int8_t> valueBytes = {0, 0, 0, 0, 0, 0, 0, 1}; // 简单 value

    EXPECT_NO_THROW(pqState.writeEntry(keyBytes, valueBytes));
}

TEST_F(HeapRestoreStateWriterTest, PqStateWriteEntryToPendingQueue)
{
    // 验证 PQ 写入后 backend 的 pending 队列有记录
    HeapRestorePQState<int> pqState(backend_.get(), "pendingPQ", 2);

    std::vector<int8_t> keyBytes = {0, 0, 0, 0, 0, 5};
    std::vector<int8_t> valueBytes = {0, 0, 0, 0, 0, 0, 0, 5};

    EXPECT_NO_THROW(pqState.writeEntry(keyBytes, valueBytes));

    // 由于没有注册 PQ wrapper，数据应进入 pending 队列
    EXPECT_GT(backend_->getPendingPriorityQueueRestoreEntryCount("pendingPQ"), 0u);
}

// ============================================================================
// HeapRestoreBackendDelegate 测试 — 更多工厂方法场景
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateWithValueType)
{
    auto metaInfo = makeKvMetaInfo("valueState");
    auto kv = delegate_->createKVState(0, metaInfo);
    ASSERT_NE(kv, nullptr);

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].stateType, StateDescriptor::Type::VALUE);
    EXPECT_EQ(delegate_->getStateInfos()[0].stateName, "valueState");
}

TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateWithListType)
{
    auto metaInfo = makeListMetaInfo("listState");
    auto kv = delegate_->createKVState(0, metaInfo);
    ASSERT_NE(kv, nullptr);

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].stateType, StateDescriptor::Type::LIST);
    EXPECT_EQ(delegate_->getStateInfos()[0].stateName, "listState");
}

TEST_F(HeapRestoreStateWriterTest, DelegateCreatePqStateWithDelegate)
{
    auto metaInfo = StateMetaInfoSnapshot(
        "pqFromDelegate",
        StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        std::unordered_map<std::string, TypeSerializer*>{});

    auto pq = delegate_->createPQState(1, metaInfo);
    ASSERT_NE(pq, nullptr);

    // 验证 PQ state 写入
    std::vector<int8_t> keyBytes = {0, 0, 0, 0, 0, 10};
    std::vector<int8_t> valueBytes = {0, 0, 0, 0, 0, 0, 0, 10};
    EXPECT_NO_THROW(pq->writeEntry(keyBytes, valueBytes));
    EXPECT_NO_THROW(pq->flush());
    EXPECT_NO_THROW(pq->discard());
}

// ============================================================================
// HeapRestoreBackendDelegate ensureStateRegistered 测试 — 重复注册
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateSameIdReturnsSameInfo)
{
    // 同一个 kvStateId 注册两次应返回同一个 stateInfo
    auto metaInfo1 = makeKvMetaInfo("sameId");
    auto kv1 = delegate_->createKVState(0, metaInfo1);

    auto metaInfo2 = makeKvMetaInfo("sameId");
    auto kv2 = delegate_->createKVState(0, metaInfo2);

    // 重复注册去重：stateInfos 中只有一条记录
    ASSERT_EQ(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].stateName, "sameId");
    // 两次返回的 kv 对象非空
    ASSERT_NE(kv1, nullptr);
    ASSERT_NE(kv2, nullptr);
}

// ============================================================================
// HeapRestoreBackendDelegate createMainTableDescriptor 测试 — 实际 MapSerializer 路径
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, DelegateCreateMainTableDescriptorForMapWithRealMapSerializer)
{
    auto* mapSer = new MapSerializer(new IntSerializer(), new LongSerializer());
    serializersToClean_.emplace_back(mapSer);
    auto metaInfo = makeKvMetaInfoWithSerializer("mapReal", mapSer, "MAP");

    auto* desc = omnistream::HeapRestoreBackendDelegate<int>::createMainTableDescriptor(metaInfo);
    ASSERT_NE(desc, nullptr);

    EXPECT_EQ(desc->getType(), StateDescriptor::Type::MAP);
    EXPECT_EQ(desc->getBackendId(), BackendDataType::INVALID_BK);
    EXPECT_EQ(desc->getKeyDataId(), BackendDataType::INT_BK);
    EXPECT_EQ(desc->getValueDataId(), BackendDataType::BIGINT_BK);
    delete desc;
}

// ============================================================================
// HeapRestoreBackendDelegate createMainTableDescriptor 测试 — 实际 ListSerializer 路径
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, DelegateCreateMainTableDescriptorForListWithRealListSerializer)
{
    auto* listSer = new ListSerializer(new LongSerializer());
    serializersToClean_.emplace_back(listSer);
    auto metaInfo = makeKvMetaInfoWithSerializer("listReal", listSer, "LIST");

    auto* desc = omnistream::HeapRestoreBackendDelegate<int>::createMainTableDescriptor(metaInfo);
    ASSERT_NE(desc, nullptr);

    EXPECT_EQ(desc->getType(), StateDescriptor::Type::LIST);
    EXPECT_EQ(desc->getBackendId(), BackendDataType::BIGINT_BK);
    delete desc;
}

} // namespace
