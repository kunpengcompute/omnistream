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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/TypeSerializerSnapshot.h"
#include "core/typeutils/VoidSerializer.h"
#include "core/api/common/state/RestoreStateDescriptor.h"
#include "core/api/common/state/StateDescriptor.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/OperatorRuntimeStateSchemaProvider.h"
#include "runtime/state/heap/HeapRestoreBackendDelegate.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"

namespace {

// ============================================================================
// HeapRestoreBackendDelegate 构造与访问器测试
// ============================================================================

TEST(HeapRestoreBackendDelegateTest, ConstructorStoresBackendAndSerializerAndPrefixBytes)
{
    auto keySer = std::make_shared<LongSerializer>();
    omnistream::HeapRestoreBackendDelegate<long> delegate(nullptr, keySer, 2);

    EXPECT_EQ(delegate.getBackend(), nullptr);
    EXPECT_EQ(delegate.getKeySerializer().get(), keySer.get());
    EXPECT_EQ(delegate.getKeyGroupPrefixBytes(), 2);
}

TEST(HeapRestoreBackendDelegateTest, StateInfosIsInitiallyEmpty)
{
    auto keySer = std::make_shared<LongSerializer>();
    omnistream::HeapRestoreBackendDelegate<long> delegate(nullptr, keySer, 0);

    EXPECT_TRUE(delegate.getStateInfos().empty());
}

// ============================================================================
// createMainTableDescriptor 测试 — 看护 Value / Map / List 三种类型
// ============================================================================

// 构造最小可用的 StateMetaInfoSnapshot。
// createMainTableDescriptor 只通过 getTypeSerializer() 获取原始指针，
// 因此只填充 serializers map，不调用可能抛 NOT_IMPL_EXCEPTION 的 snapshotConfiguration()。
std::shared_ptr<StateMetaInfoSnapshot> makeSnapshot(
    const std::string& name,
    StateMetaInfoSnapshot::BackendStateType backendType,
    const std::string& stateTypeStr,
    TypeSerializer* valSerializer)
{
    std::unordered_map<std::string, std::string> options;
    options[StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] = stateTypeStr;

    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerSnapshots;

    std::unordered_map<std::string, TypeSerializer*> serializers;
    serializers["namespaceSerializer"] = VoidSerializer::INSTANCE;
    if (valSerializer != nullptr) {
        serializers["stateSerializer"] = valSerializer;
    }

    return std::make_shared<StateMetaInfoSnapshot>(name, backendType, options, serializerSnapshots, serializers);
}

// 看护 VALUE 类型 Snapshot 创建 RestoreStateDescriptor 并带回 valueType
TEST(HeapRestoreBackendDelegateTest, CreateMainTableDescriptorForValueTypeReturnsValueDescriptor)
{
    LongSerializer valSer;
    auto snapshot = makeSnapshot("testValue", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "VALUE", &valSer);

    auto* desc = omnistream::HeapRestoreBackendDelegate<long>::createMainTableDescriptor(*snapshot);
    ASSERT_NE(desc, nullptr);

    EXPECT_EQ(desc->getType(), StateDescriptor::Type::VALUE);
    EXPECT_EQ(desc->getName(), "testValue");
    delete desc;
}

// 看护 LIST 类型 Snapshot 创建 RestoreStateDescriptor 并带回元素类型
TEST(HeapRestoreBackendDelegateTest, CreateMainTableDescriptorForListTypeReturnsListDescriptor)
{
    LongSerializer valSer;
    auto snapshot = makeSnapshot("testList", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "LIST", &valSer);

    auto* desc = omnistream::HeapRestoreBackendDelegate<long>::createMainTableDescriptor(*snapshot);
    ASSERT_NE(desc, nullptr);

    EXPECT_EQ(desc->getType(), StateDescriptor::Type::LIST);
    EXPECT_EQ(desc->getName(), "testList");
    delete desc;
}

// 看护 MAP 类型 Snapshot 创建 RestoreStateDescriptor 并带回 key/value 类型
TEST(HeapRestoreBackendDelegateTest, CreateMainTableDescriptorForMapTypeReturnsMapDescriptor)
{
    LongSerializer valSer;
    auto snapshot = makeSnapshot("testMap", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "MAP", &valSer);

    auto* desc = omnistream::HeapRestoreBackendDelegate<long>::createMainTableDescriptor(*snapshot);
    ASSERT_NE(desc, nullptr);

    EXPECT_EQ(desc->getType(), StateDescriptor::Type::MAP);
    EXPECT_EQ(desc->getName(), "testMap");
    delete desc;
}

TEST(HeapRestoreBackendDelegateTest, RuntimeSchemaProviderOverridesStreamingJoinMapKeyType)
{
    omnistream::RowType rowType(true, std::vector<std::string>{"BIGINT"});
    MapSerializer mapSerializer(new RowDataSerializer(&rowType), new JoinTupleSerializer());
    auto snapshot =
        makeSnapshot("left-records", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "MAP", &mapSerializer);
    auto* persistedDesc = omnistream::HeapRestoreBackendDelegate<long>::createMainTableDescriptor(*snapshot);
    ASSERT_NE(persistedDesc, nullptr);
    EXPECT_EQ(persistedDesc->getKeyDataId(), BackendDataType::ROW_BK);
    delete persistedDesc;

    nlohmann::json operatorDescription{
        {"joinType", "LeftOuterJoin"}, {"leftInputSpec", "NoUniqueKey"}, {"rightInputSpec", "NoUniqueKey"}};
    auto provider = omnistream::OperatorRuntimeStateSchemaProviderFactory::create(operatorDescription);
    ASSERT_NE(provider, nullptr);

    auto* desc = omnistream::HeapRestoreBackendDelegate<long>::createMainTableDescriptor(*snapshot, provider.get());
    ASSERT_NE(desc, nullptr);
    EXPECT_EQ(desc->getKeyDataId(), BackendDataType::SHARED_ROW_BK);
    EXPECT_EQ(desc->getValueDataId(), BackendDataType::TUPLE_INT32_INT32);
    EXPECT_EQ(mapSerializer.getKeySerializer()->getBackendId(), BackendDataType::ROW_BK);
    delete desc;
}

// 看护 UNKNOWN 类型退化为 VALUE
TEST(HeapRestoreBackendDelegateTest, CreateMainTableDescriptorForUnknownTypeFallsBackToValue)
{
    LongSerializer valSer;
    auto snapshot = makeSnapshot("testUnknown", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "", &valSer);

    auto* desc = omnistream::HeapRestoreBackendDelegate<long>::createMainTableDescriptor(*snapshot);
    ASSERT_NE(desc, nullptr);

    EXPECT_EQ(desc->getType(), StateDescriptor::Type::VALUE);
    delete desc;
}

// 看护 valSerializer 为 nullptr 时回退为 BIGINT_BK
TEST(HeapRestoreBackendDelegateTest, CreateMainTableDescriptorWithNullSerializerReturnsBintintBackendType)
{
    auto snapshot = makeSnapshot("testNull", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "VALUE", nullptr);

    auto* desc = omnistream::HeapRestoreBackendDelegate<long>::createMainTableDescriptor(*snapshot);
    ASSERT_NE(desc, nullptr);

    EXPECT_EQ(desc->getType(), StateDescriptor::Type::VALUE);
    delete desc;
}

} // namespace
