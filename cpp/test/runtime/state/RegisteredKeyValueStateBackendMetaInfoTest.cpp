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
#include "runtime/state/RegisteredBroadcastStateBackendMetaInfo.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/RegisteredOperatorStateBackendMetaInfo.h"
#include "runtime/state/RegisteredPriorityQueueStateBackendMetaInfo.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/LongSerializer.h"

TEST(RegisteredKeyValueStateBackendMetaInfoTest, MetaInfoSnapshot)
{
    VoidNamespaceSerializer voidSer;
    MapSerializer mapSer(new LongSerializer(), new LongSerializer());
    RegisteredKeyValueStateBackendMetaInfo metaInfo(StateDescriptor::Type::MAP, "testMetaInfo", &voidSer, &mapSer);
    auto stateMetaInfoSnapshot = metaInfo.snapshot();

    EXPECT_EQ(stateMetaInfoSnapshot->getOption("KEYED_STATE_TYPE"), "6");
    EXPECT_EQ(stateMetaInfoSnapshot->getName(), "testMetaInfo");
    EXPECT_EQ(stateMetaInfoSnapshot->getNamespaceSerializer(), &voidSer);
    EXPECT_EQ(stateMetaInfoSnapshot->getValueSerializer(), &mapSer);

    RegisteredKeyValueStateBackendMetaInfo restored(*stateMetaInfoSnapshot);
    EXPECT_EQ(restored.getNamespaceSerializer(), &voidSer);
    EXPECT_EQ(restored.getStateSerializer(), &mapSer);
}

TEST(RegisteredKeyValueStateBackendMetaInfoTest, NormalizesLegacySerializerKeys)
{
    VoidNamespaceSerializer voidSer;
    LongSerializer valueSer;
    std::unordered_map<std::string, TypeSerializer*> serializers{
        {SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY, &voidSer},
        {SerializerJsonInfo::STATE_SERIALIZER_KEY, &valueSer}};

    StateMetaInfoSnapshot snapshot("legacy", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, {}, {}, serializers);

    const auto& normalized = snapshot.getSerializersImmutable();
    EXPECT_EQ(normalized.size(), 2U);
    EXPECT_EQ(normalized.at(StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY), &voidSer);
    EXPECT_EQ(normalized.at(StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY), &valueSer);
    EXPECT_EQ(normalized.count(SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY), 0U);
    EXPECT_EQ(normalized.count(SerializerJsonInfo::STATE_SERIALIZER_KEY), 0U);
    EXPECT_EQ(snapshot.getTypeSerializer(SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY), &voidSer);
    EXPECT_EQ(snapshot.getTypeSerializer(SerializerJsonInfo::STATE_SERIALIZER_KEY), &valueSer);
}

TEST(RegisteredKeyValueStateBackendMetaInfoTest, SerializerJsonUsesAdaptorProtocolKeys)
{
    VoidNamespaceSerializer voidSer;
    LongSerializer valueSer;
    StateMetaInfoSnapshot snapshot(
        "json",
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        {},
        {},
        {{StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY, &voidSer},
         {StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, &valueSer}});

    nlohmann::json serializerJson = nlohmann::json::parse(snapshot.getSerializerJson());
    EXPECT_TRUE(serializerJson.contains(SerializerJsonInfo::NAMESPACE_SERIALIZER_KEY));
    EXPECT_TRUE(serializerJson.contains(SerializerJsonInfo::STATE_SERIALIZER_KEY));
    EXPECT_FALSE(serializerJson.contains(StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY));
    EXPECT_FALSE(serializerJson.contains(StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY));
}

TEST(RegisteredKeyValueStateBackendMetaInfoTest, RejectsConflictingSerializerAliases)
{
    LongSerializer first;
    LongSerializer second;
    std::unordered_map<std::string, TypeSerializer*> serializers{
        {StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, &first}, {SerializerJsonInfo::STATE_SERIALIZER_KEY, &second}};

    EXPECT_THROW(
        StateMetaInfoSnapshot("conflict", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, {}, {}, serializers),
        std::invalid_argument);
}

TEST(RegisteredKeyValueStateBackendMetaInfoTest, PrefersNonNullSerializerAlias)
{
    LongSerializer valueSer;
    std::unordered_map<std::string, TypeSerializer*> serializers{
        {StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, nullptr}, {SerializerJsonInfo::STATE_SERIALIZER_KEY, &valueSer}};

    StateMetaInfoSnapshot snapshot(
        "null-alias", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, {}, {}, serializers);

    EXPECT_EQ(snapshot.getSerializersImmutable().size(), 1U);
    EXPECT_EQ(snapshot.getValueSerializer(), &valueSer);
}

TEST(RegisteredStateBackendMetaInfoTest, OperatorSnapshotRoundTrip)
{
    LongSerializer valueSer;
    RegisteredOperatorStateBackendMetaInfo metaInfo("operator", OperatorStateHandle::Mode::SPLIT_DISTRIBUTE, &valueSer);

    auto snapshot = metaInfo.snapshot();
    EXPECT_EQ(snapshot->getValueSerializer(), &valueSer);

    StateMetaInfoSnapshot restorableSnapshot(
        "operator",
        StateMetaInfoSnapshot::BackendStateType::OPERATOR,
        {{StateMetaInfoSnapshot::OPERATOR_STATE_DISTRIBUTION_MODE, "SPLIT_DISTRIBUTE"}},
        {},
        snapshot->getSerializersImmutable());
    RegisteredOperatorStateBackendMetaInfo restored(restorableSnapshot);
    EXPECT_EQ(restored.getStateSerializer(), &valueSer);
    EXPECT_EQ(restored.getAssignmentMode(), OperatorStateHandle::Mode::SPLIT_DISTRIBUTE);
}

TEST(RegisteredStateBackendMetaInfoTest, BroadcastSnapshotRoundTrip)
{
    LongSerializer keySer;
    LongSerializer valueSer;
    RegisteredBroadcastStateBackendMetaInfo metaInfo(
        "broadcast", OperatorStateHandle::Mode::BROADCAST, &keySer, &valueSer);

    auto snapshot = metaInfo.snapshot();
    EXPECT_EQ(snapshot->getKeySerializer(), &keySer);
    EXPECT_EQ(snapshot->getValueSerializer(), &valueSer);

    StateMetaInfoSnapshot restorableSnapshot(
        "broadcast",
        StateMetaInfoSnapshot::BackendStateType::BROADCAST,
        {{StateMetaInfoSnapshot::OPERATOR_STATE_DISTRIBUTION_MODE, "BROADCAST"}},
        {},
        snapshot->getSerializersImmutable());
    RegisteredBroadcastStateBackendMetaInfo restored(restorableSnapshot);
    EXPECT_EQ(restored.getKeySerializer(), &keySer);
    EXPECT_EQ(restored.getValueSerializer(), &valueSer);
    EXPECT_EQ(restored.getAssignmentMode(), OperatorStateHandle::Mode::BROADCAST);
}

TEST(RegisteredStateBackendMetaInfoTest, PriorityQueueSnapshotRoundTrip)
{
    LongSerializer elementSer;
    RegisteredPriorityQueueStateBackendMetaInfo metaInfo("priority-queue", &elementSer);

    auto snapshot = metaInfo.snapshot();
    EXPECT_EQ(snapshot->getValueSerializer(), &elementSer);

    RegisteredPriorityQueueStateBackendMetaInfo restored(*snapshot);
    EXPECT_EQ(restored.getElementSerializer(), &elementSer);
}
