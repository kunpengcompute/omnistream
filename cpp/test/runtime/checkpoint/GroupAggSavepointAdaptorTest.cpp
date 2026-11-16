/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/type/MapValue.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/TypeSerializerSingleton.h"
#include "core/typeinfo/TypeInfoFactory.h"

// Expose the owned serializer slots so the defensive null-child branches can be
// covered without constructing an invalid MapSerializer (its constructor dereferences both children).
#define private public
#include "core/typeutils/MapSerializer.h"
#undef private

#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/OperatorSavepointAdaptor.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/KeyValueStateIterator.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/heap/HeapFullSnapshotResources.h"
#include "runtime/state/filesystem/FileStateHandle.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/RestoreKVState.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
#include "runtime/state/restore/vb/VectorBatchRestoreHooks.h"
#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"
#include "test/runtime/state/MockSavepointBridge.h"

// The implementation has several schema and conversion helpers that are deliberately
// private. Expose only this adaptor after all of its dependencies have been included.
#define private public
#include "runtime/checkpoint/GroupAggSavepointAdaptor.h"
#undef private

using namespace omnistream;
using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

namespace {

constexpr const char* ACC_STATE_NAME = "accState";
constexpr const char* ACC_VB_STATE_NAME = "accStatevb";
constexpr const char* DISTINCT_STATE_NAME = "distinctAcc_0";
constexpr const char* TIMER_STATE_NAME = "_timer_state/group-agg";
constexpr const char* RAW_TYPE = "RAW('example.DataView', '')";

const nlohmann::json GROUP_AGG_DESCRIPTION = {
    {"aggInfoList", {{"accTypes", {"BIGINT", RAW_TYPE, "INT", "VARCHAR"}}}},
};

std::shared_ptr<StateMetaInfoSnapshot> makeMeta(
    const std::string& name,
    StateMetaInfoSnapshot::BackendStateType backendType,
    const std::string& stateType = "VALUE",
    TypeSerializer* namespaceSerializer = nullptr,
    TypeSerializer* valueSerializer = nullptr)
{
    std::unordered_map<std::string, std::string> options;
    if (!stateType.empty()) {
        options[StateMetaInfoSnapshot::KEYED_STATE_TYPE] = stateType;
    }
    std::unordered_map<std::string, TypeSerializer*> serializers;
    if (namespaceSerializer != nullptr) {
        serializers[StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY] = namespaceSerializer;
    }
    if (valueSerializer != nullptr) {
        serializers[StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY] = valueSerializer;
    }
    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        backendType,
        options,
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        serializers);
}

std::shared_ptr<StateMetaInfoSnapshot> makePriorityQueue(const std::string& name = TIMER_STATE_NAME)
{
    return makeMeta(name, StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE, "");
}

std::shared_ptr<StateMetaInfoSnapshot> makeKeyValue(
    const std::string& name,
    const std::string& stateType = "VALUE",
    TypeSerializer* namespaceSerializer = VoidNamespaceSerializer::INSTANCE,
    TypeSerializer* valueSerializer = LongSerializer::INSTANCE)
{
    return makeMeta(
        name, StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, stateType, namespaceSerializer, valueSerializer);
}

std::vector<int8_t> copyOutput(DataOutputSerializer& output)
{
    const auto* begin = reinterpret_cast<const int8_t*>(output.getData());
    return std::vector<int8_t>(begin, begin + output.getPosition());
}

std::vector<int8_t> serializeRow(RowDataSerializer& serializer, RowData& row)
{
    DataOutputSerializer output(256);
    serializer.serialize(&row, output);
    return copyOutput(output);
}

class EmptyKeyValueStateIterator : public KeyValueStateIterator {
public:
    void next() override
    {
    }
    int keyGroup() const override
    {
        return -1;
    }
    ByteView key() const override
    {
        return {};
    }
    ByteView value() const override
    {
        return {};
    }
    int kvStateId() const override
    {
        return -1;
    }
    const CurrentEntry& current() const override
    {
        return entry_;
    }
    bool isNewKeyValueState() const override
    {
        return false;
    }
    bool isNewKeyGroup() const override
    {
        return false;
    }
    bool isValid() const override
    {
        return false;
    }
    void close() override
    {
        closed = true;
    }

    bool closed = false;

private:
    CurrentEntry entry_;
};

class TestFullSnapshotResources : public FullSnapshotResources {
public:
    explicit TestFullSnapshotResources(std::vector<std::shared_ptr<StateMetaInfoSnapshot>> snapshots = {})
        : metaInfos(std::move(snapshots))
    {
    }

    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() override
    {
        return metaInfos;
    }
    KeyGroupRange* getKeyGroupRange() override
    {
        return &keyGroupRange;
    }
    TypeSerializer* getKeySerializer() override
    {
        return nullptr;
    }
    std::shared_ptr<KeyValueStateIterator> createKVStateIterator() override
    {
        return iterator;
    }
    void cleanup() override
    {
    }

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos;
    KeyGroupRange keyGroupRange{0, 0};
    std::shared_ptr<KeyValueStateIterator> iterator;
};

class EmptyRestoreBackend : public RestoreBackendDelegate {
public:
    std::unique_ptr<RestoreKVState> createKVState(int, const StateMetaInfoSnapshot&) override
    {
        return nullptr;
    }

    std::unique_ptr<RestoreKVStateVB> createKVStateVB(
        int, const StateMetaInfoSnapshot&, const std::vector<omniruntime::type::DataTypeId>&, int) override
    {
        return nullptr;
    }

    std::unique_ptr<RestorePQState> createPQState(int, const StateMetaInfoSnapshot&) override
    {
        return nullptr;
    }
};

class RecordingRestoreKVState : public RestoreKVState {
public:
    void flush() override
    {
    }
    void discard() override
    {
    }
    void setKeyGroupId(int) override
    {
    }

    std::vector<int8_t> key;
    std::vector<int8_t> value;

protected:
    void writeLongEntry(const std::vector<int8_t>& keyBytes, int64_t longValue) override
    {
        key = keyBytes;
        value.resize(sizeof(longValue));
        std::memcpy(value.data(), &longValue, sizeof(longValue));
    }

    void writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView bytes) override
    {
        key = keyBytes;
        const auto* begin = reinterpret_cast<const int8_t*>(bytes.data());
        value.assign(begin, begin + bytes.size());
    }
};

class NullDeserializingSerializer : public TypeSerializer {
public:
    void* deserialize(DataInputView&) override
    {
        return nullptr;
    }
    void serialize(void*, DataOutputSerializer&) override
    {
    }
};

struct RestoredEntry {
    std::vector<int8_t> key;
    std::vector<int8_t> value;
    int keyGroup = -1;
    bool flushed = false;
};

class CapturingRestoreKVState : public RecordingRestoreKVState {
public:
    explicit CapturingRestoreKVState(RestoredEntry& result) : result_(result)
    {
    }
    ~CapturingRestoreKVState() override
    {
        result_.key = key;
        result_.value = value;
    }
    void setKeyGroupId(int id) override
    {
        result_.keyGroup = id;
    }
    void flush() override
    {
        result_.flushed = true;
    }

private:
    RestoredEntry& result_;
};

class CapturingRestoreBackend : public EmptyRestoreBackend {
public:
    std::unique_ptr<RestoreKVState> createKVState(int id, const StateMetaInfoSnapshot& meta) override
    {
        if (meta.getName() == ACC_STATE_NAME) {
            accumulatorSerializer.reset(meta.getValueSerializer());
        }
        return std::make_unique<CapturingRestoreKVState>(entries.at(id));
    }
    std::vector<RestoredEntry> entries{2};
    std::unique_ptr<TypeSerializer> accumulatorSerializer;
};

std::unique_ptr<HeapFullSnapshotResources> makeHeapResources(
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas, KeyGroupRange& range)
{
    return std::make_unique<HeapFullSnapshotResources>(
        std::move(metas),
        std::vector<std::unique_ptr<SingleStateIterator>>{},
        &range,
        nullptr,
        1,
        std::unordered_map<std::string, std::shared_ptr<HeapSnapshotStateData>>{});
}

} // namespace

TEST(GroupAggSavepointAdaptorTest, PrepareBuildsCompactAndFlinkAccumulatorSchemas)
{
    GroupAggSavepointAdaptor adaptor;

    adaptor.prepareForSave(GROUP_AGG_DESCRIPTION);

    EXPECT_EQ(adaptor.flinkAccTypes_, (std::vector<std::string>{"BIGINT", RAW_TYPE, "INT", "VARCHAR"}));
    EXPECT_EQ(adaptor.omniAccTypes_, (std::vector<std::string>{"BIGINT", "INT", "VARCHAR"}));
    EXPECT_EQ(adaptor.flinkToOmniIndex_, (std::vector<int>{0, -1, 1, 2}));
    ASSERT_NE(adaptor.flinkAccSerializer_, nullptr);
    ASSERT_NE(adaptor.omniAccSerializer_, nullptr);
    EXPECT_EQ(adaptor.flinkAccSerializer_->getArity(), 4);
    EXPECT_EQ(adaptor.omniAccSerializer_->getArity(), 3);

    adaptor.prepareForRestore({{"aggInfoList", {{"accTypes", {"INT", 1, RAW_TYPE}}}}});
    EXPECT_EQ(adaptor.flinkAccTypes_, (std::vector<std::string>{"INT", RAW_TYPE}));
    EXPECT_EQ(adaptor.omniAccTypes_, (std::vector<std::string>{"INT"}));
    ASSERT_NE(adaptor.omniAccSerializer_, nullptr);
    EXPECT_EQ(adaptor.omniAccSerializer_->getArity(), 1);

    EXPECT_THROW(adaptor.prepareForSave(nlohmann::json::object()), std::runtime_error);
    EXPECT_THROW(adaptor.prepareForSave({{"aggInfoList", nlohmann::json::array()}}), std::runtime_error);
    EXPECT_THROW(adaptor.prepareForRestore({{"aggInfoList", "invalid"}}), std::runtime_error);
}

TEST(GroupAggSavepointAdaptorTest, FlinkAccumulatorMetadataReconstructsRawFields)
{
    GroupAggSavepointAdaptor adaptor;
    adaptor.prepareForSave(GROUP_AGG_DESCRIPTION);
    auto metadata = nlohmann::json::parse(adaptor.flinkAccSerializer_->toJson());
    std::unique_ptr<TypeInformation> typeInfo(
        TypeInfoFactory::createInternalTypeInfoOfRow(metadata["logicalType"]["fields"]));
    std::unique_ptr<TypeSerializer> serializer(typeInfo->createTypeSerializer());
    auto* rowSerializer = dynamic_cast<RowDataSerializer*>(serializer.get());
    ASSERT_NE(rowSerializer, nullptr);
    EXPECT_EQ(rowSerializer->getArity(), 4);
    auto rebuilt = nlohmann::json::parse(rowSerializer->toJson());
    EXPECT_EQ(rebuilt["logicalType"]["fields"][1]["fieldType"]["type"], "RAW");
    EXPECT_EQ(rebuilt["logicalType"]["fields"][1]["fieldType"]["class"], "example.DataView");
    EXPECT_EQ(rebuilt["logicalType"]["fields"][1]["fieldType"]["serializer"], "");
}

TEST(GroupAggSavepointAdaptorTest, ValidateForSaveAcceptsClosedGroupAggStateSet)
{
    GroupAggSavepointAdaptor adaptor;
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>> states{
        makeKeyValue(ACC_STATE_NAME),
        makeKeyValue(ACC_VB_STATE_NAME),
        makeKeyValue(DISTINCT_STATE_NAME, "MAP"),
        makeKeyValue("distinctAcc_1"),
        makePriorityQueue(),
    };

    EXPECT_NO_THROW(adaptor.validateForSave(states));
    EXPECT_THROW(adaptor.validateForSave({}), std::runtime_error);
}

TEST(GroupAggSavepointAdaptorTest, ValidateForRestoreAcceptsLogicalStatesAndRejectsSaveSideTable)
{
    GroupAggSavepointAdaptor adaptor;
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>> states{
        makeKeyValue(ACC_STATE_NAME),
        makeKeyValue(DISTINCT_STATE_NAME, "MAP"),
        makePriorityQueue(),
    };
    EXPECT_NO_THROW(adaptor.validateForRestore(states));

    auto statesWithVb = states;
    statesWithVb.push_back(makeKeyValue(ACC_VB_STATE_NAME));
    EXPECT_THROW(adaptor.validateForRestore(statesWithVb), std::runtime_error);
}

TEST(GroupAggSavepointAdaptorTest, BuildSavePlanSkipsNullAndVectorBatchMetadataAndMapsLogicalStates)
{
    GroupAggSavepointAdaptor adaptor;
    adaptor.prepareForSave(GROUP_AGG_DESCRIPTION);
    auto acc = makeKeyValue(ACC_STATE_NAME);
    auto distinct = makeKeyValue(DISTINCT_STATE_NAME, "MAP");
    auto timer = makePriorityQueue();
    TestFullSnapshotResources resources({nullptr, makeKeyValue(ACC_VB_STATE_NAME), acc, distinct, timer});

    auto plan = adaptor.buildSavePlan(resources);

    EXPECT_EQ(plan.keyGroupRange, &resources.keyGroupRange);
    EXPECT_EQ(plan.mainStateIds, (std::vector<int>{2, 3, 4}));
    EXPECT_EQ(plan.kvStateIdMapping.at(2), 0);
    EXPECT_EQ(plan.kvStateIdMapping.at(3), 1);
    EXPECT_EQ(plan.kvStateIdMapping.at(4), 2);
    ASSERT_EQ(plan.targetMetaInfos.size(), 3U);
    EXPECT_EQ(plan.targetMetaInfos[0]->getName(), ACC_STATE_NAME);
    EXPECT_EQ(plan.targetMetaInfos[0]->getNamespaceSerializer(), VoidNamespaceSerializer::INSTANCE);
    EXPECT_EQ(plan.targetMetaInfos[0]->getValueSerializer(), adaptor.flinkAccSerializer_.get());
    EXPECT_EQ(plan.targetMetaInfos[1], distinct);
    EXPECT_EQ(plan.targetMetaInfos[2], timer);

    ASSERT_EQ(plan.stateContextSpecs.size(), 3U);
    EXPECT_EQ(plan.stateContextSpecs[0].sourceKvStateId, 2);
    EXPECT_EQ(plan.stateContextSpecs[0].stateType, VectorBatchStateType::KV_TRANSFORM);
    EXPECT_EQ(plan.stateContextSpecs[0].valueSerializer, adaptor.flinkAccSerializer_.get());
    EXPECT_EQ(plan.stateContextSpecs[0].sourceValueSerializer, adaptor.omniAccSerializer_.get());
    EXPECT_EQ(plan.stateContextSpecs[1].stateType, VectorBatchStateType::KV);
    EXPECT_EQ(plan.stateContextSpecs[1].valueSerializer, LongSerializer::INSTANCE);
    EXPECT_EQ(plan.stateContextSpecs[2].stateType, VectorBatchStateType::PQ);
}

TEST(GroupAggSavepointAdaptorTest, BuildSavePlanRejectsUnsupportedBackendAndMissingAccSerializers)
{
    GroupAggSavepointAdaptor adaptor;
    TestFullSnapshotResources unsupported(
        {makeMeta("operator", StateMetaInfoSnapshot::BackendStateType::OPERATOR, "")});
    EXPECT_THROW(adaptor.buildSavePlan(unsupported), std::runtime_error);

    TestFullSnapshotResources unprepared({makeKeyValue(ACC_STATE_NAME)});
    EXPECT_THROW(adaptor.buildSavePlan(unprepared), std::runtime_error);

    GroupAggSavepointAdaptor restoreOnlyAdaptor;
    restoreOnlyAdaptor.prepareForRestore(GROUP_AGG_DESCRIPTION);
    EXPECT_THROW(restoreOnlyAdaptor.buildSavePlan(unprepared), std::runtime_error);

    adaptor.prepareForSave(GROUP_AGG_DESCRIPTION);
    TestFullSnapshotResources missingNamespace({makeKeyValue(ACC_STATE_NAME, "VALUE", nullptr)});
    EXPECT_THROW(adaptor.buildSavePlan(missingNamespace), std::runtime_error);

    adaptor.omniAccSerializer_.reset();
    EXPECT_THROW(adaptor.buildSavePlan(unprepared), std::runtime_error);
}

TEST(GroupAggSavepointAdaptorTest, BuildSavePlanTreatsMapStateAsKv)
{
    GroupAggSavepointAdaptor adaptor;
    auto mapSerializer = std::make_unique<MapSerializer>(new IntSerializer(), new IntSerializer());
    auto mapMeta = makeKeyValue(DISTINCT_STATE_NAME, "MAP", VoidNamespaceSerializer::INSTANCE, mapSerializer.get());

    TestFullSnapshotResources resources({mapMeta});
    auto plan = adaptor.buildSavePlan(resources);
    ASSERT_EQ(plan.stateContextSpecs.size(), 1U);
    EXPECT_EQ(plan.stateContextSpecs[0].stateType, VectorBatchStateType::KV);
}

TEST(GroupAggSavepointAdaptorTest, BuildSaveStateContextsCopiesEveryPlanField)
{
    GroupAggSavepointAdaptor adaptor;
    TestFullSnapshotResources resources({makeKeyValue("zero"), nullptr, makeKeyValue("two")});
    VectorBatchSavePlan plan;
    plan.kvStateIdMapping.emplace(0, 3);
    plan.kvStateIdMapping.emplace(2, 5);
    VectorBatchSavePlan::StateContextSpec first;
    first.sourceKvStateId = 0;
    first.logicalStateName = "zero";
    first.stateType = VectorBatchStateType::KV_TRANSFORM;
    first.valueSerializer = LongSerializer::INSTANCE;
    first.sourceValueSerializer = IntSerializer::INSTANCE;
    VectorBatchSavePlan::StateContextSpec second;
    second.sourceKvStateId = 2;
    second.logicalStateName = "two";
    second.stateType = VectorBatchStateType::KV;
    second.valueSerializer = LongSerializer::INSTANCE;
    plan.stateContextSpecs.push_back(first);
    plan.stateContextSpecs.push_back(second);

    auto contexts = adaptor.buildSaveStateContexts(resources, plan);

    ASSERT_EQ(contexts.size(), 3U);
    EXPECT_TRUE(contexts[0].writable);
    EXPECT_EQ(contexts[0].mappedKvStateId, 3);
    EXPECT_EQ(contexts[0].logicalStateName, "zero");
    EXPECT_EQ(contexts[0].stateType, VectorBatchStateType::KV_TRANSFORM);
    EXPECT_EQ(contexts[0].valueSerializer, LongSerializer::INSTANCE);
    EXPECT_EQ(contexts[0].sourceValueSerializer, IntSerializer::INSTANCE);
    EXPECT_FALSE(contexts[1].isValid());
    EXPECT_TRUE(contexts[2].writable);
    EXPECT_EQ(contexts[2].mappedKvStateId, 5);
    EXPECT_EQ(contexts[2].stateType, VectorBatchStateType::KV);
    EXPECT_EQ(contexts[2].valueSerializer, LongSerializer::INSTANCE);
}

TEST(GroupAggSavepointAdaptorTest, BuildSaveStateContextsRejectsBadSourceIdOrMissingMapping)
{
    GroupAggSavepointAdaptor adaptor;
    TestFullSnapshotResources resources({makeKeyValue("state")});
    VectorBatchSavePlan outOfRange;
    VectorBatchSavePlan::StateContextSpec badSpec;
    badSpec.sourceKvStateId = 1;
    outOfRange.stateContextSpecs.push_back(badSpec);
    EXPECT_THROW(adaptor.buildSaveStateContexts(resources, outOfRange), std::out_of_range);

    VectorBatchSavePlan missingMapping;
    badSpec.sourceKvStateId = 0;
    missingMapping.stateContextSpecs.push_back(badSpec);
    EXPECT_THROW(adaptor.buildSaveStateContexts(resources, missingMapping), std::out_of_range);

    VectorBatchSavePlan empty;
    auto contexts = adaptor.buildSaveStateContexts(resources, empty);
    ASSERT_EQ(contexts.size(), 1U);
    EXPECT_FALSE(contexts[0].writable);
}

TEST(GroupAggSavepointAdaptorTest, ExpandAndCompactCoverSupportedTypesRawAndNull)
{
    GroupAggSavepointAdaptor adaptor;
    const std::vector<std::string> flinkTypes{
        "INT",
        "VARCHAR",
        "CHAR",
        "BIGINT",
        "TIME(3)",
        "TIMESTAMP(3)",
        "TIMESTAMP_LTZ(3)",
        "TIMESTAMP_WITHOUT_TIME_ZONE",
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
        "TIMESTAMP",
        RAW_TYPE,
        "INT",
    };
    adaptor.prepareAccumulatorTypes({{"aggInfoList", {{"accTypes", flinkTypes}}}});

    std::unique_ptr<BinaryRowData> omni(BinaryRowData::createBinaryRowDataWithMem(11));
    omni->setRowKind(RowKind::UPDATE_AFTER);
    omni->setInt(0, 11);
    omni->setStringView(1, "varchar-value");
    omni->setStringView(2, "c");
    omni->setLong(3, 44);
    omni->setLong(4, 55);
    omni->setLong(5, 66);
    omni->setLong(6, 77);
    omni->setLong(7, 88);
    omni->setLong(8, 99);
    omni->setLong(9, 111);
    omni->setNullAt(10);

    auto expanded = adaptor.expandAccumulator(*omni);
    ASSERT_EQ(expanded->getArity(), 12);
    EXPECT_EQ(expanded->getRowKind(), RowKind::UPDATE_AFTER);
    EXPECT_EQ(*expanded->getInt(0), 11);
    EXPECT_EQ(expanded->getStringView(1), "varchar-value");
    EXPECT_EQ(expanded->getStringView(2), "c");
    const std::vector<long> expectedLongs{44, 55, 66, 77, 88, 99, 111};
    for (size_t i = 0; i < expectedLongs.size(); ++i) {
        EXPECT_EQ(*expanded->getLong(static_cast<int>(i + 3)), expectedLongs[i]);
    }
    EXPECT_TRUE(expanded->isNullAt(10));
    EXPECT_TRUE(expanded->isNullAt(11));

    expanded->setLong(10, 123456); // RAW contents are ignored even when the field is non-null.
    expanded->setRowKind(RowKind::DELETE);
    auto compacted = adaptor.compactAccumulator(*expanded);
    ASSERT_EQ(compacted->getArity(), 11);
    EXPECT_EQ(compacted->getRowKind(), RowKind::DELETE);
    EXPECT_EQ(*compacted->getInt(0), 11);
    EXPECT_EQ(compacted->getStringView(1), "varchar-value");
    EXPECT_EQ(compacted->getStringView(2), "c");
    for (size_t i = 0; i < expectedLongs.size(); ++i) {
        EXPECT_EQ(*compacted->getLong(static_cast<int>(i + 3)), expectedLongs[i]);
    }
    EXPECT_TRUE(compacted->isNullAt(10));
}

TEST(GroupAggSavepointAdaptorTest, ExpandAndCompactRejectArityMismatchAndUnsupportedTypes)
{
    GroupAggSavepointAdaptor adaptor;
    adaptor.prepareAccumulatorTypes({{"aggInfoList", {{"accTypes", {"INT", RAW_TYPE}}}}});
    std::unique_ptr<BinaryRowData> wrongOmni(BinaryRowData::createBinaryRowDataWithMem(0));
    std::unique_ptr<BinaryRowData> wrongFlink(BinaryRowData::createBinaryRowDataWithMem(1));
    EXPECT_THROW(adaptor.expandAccumulator(*wrongOmni), std::runtime_error);
    EXPECT_THROW(adaptor.compactAccumulator(*wrongFlink), std::runtime_error);

    adaptor.prepareAccumulatorTypes({{"aggInfoList", {{"accTypes", {"BOOLEAN"}}}}});
    std::unique_ptr<BinaryRowData> unsupported(BinaryRowData::createBinaryRowDataWithMem(1));
    EXPECT_THROW(adaptor.expandAccumulator(*unsupported), std::runtime_error);
    EXPECT_THROW(adaptor.compactAccumulator(*unsupported), std::runtime_error);
}

TEST(GroupAggSavepointAdaptorTest, GetStateTypeClassifiesEveryBackendAndLogicalState)
{
    GroupAggSavepointAdaptor adaptor;
    EXPECT_EQ(adaptor.getStateType(*makePriorityQueue()), RestoreStateType::PQ);
    EXPECT_EQ(adaptor.getStateType(*makeKeyValue(ACC_STATE_NAME)), RestoreStateType::KV_TRANSFORM);
    EXPECT_EQ(adaptor.getStateType(*makeKeyValue(DISTINCT_STATE_NAME)), RestoreStateType::KV);
    EXPECT_EQ(
        adaptor.getStateType(*makeMeta("operator", StateMetaInfoSnapshot::BackendStateType::OPERATOR, "")),
        RestoreStateType::UNSUPPORT);
}

TEST(GroupAggSavepointAdaptorTest, BuildOmniMainMetaInfoCreatesOwnedCompactSerializerAndRecordsSource)
{
    GroupAggSavepointAdaptor adaptor;
    adaptor.prepareForRestore(GROUP_AGG_DESCRIPTION);
    omnistream::RowType flinkType(true, std::vector<std::string>{"BIGINT", RAW_TYPE, "INT", "VARCHAR"});
    RowDataSerializer sourceSerializer(&flinkType);
    auto sourceMeta = makeKeyValue(ACC_STATE_NAME, "VALUE", VoidNamespaceSerializer::INSTANCE, &sourceSerializer);

    auto targetMeta = adaptor.buildOmniMainMetaInfo(7, *sourceMeta);

    EXPECT_EQ(targetMeta.getName(), ACC_STATE_NAME);
    EXPECT_EQ(targetMeta.getBackendStateType(), StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
    EXPECT_EQ(targetMeta.getOption(StateMetaInfoSnapshot::KEYED_STATE_TYPE), "1");
    EXPECT_EQ(targetMeta.getNamespaceSerializer(), VoidNamespaceSerializer::INSTANCE);
    auto* targetSerializer = dynamic_cast<RowDataSerializer*>(targetMeta.getValueSerializer());
    ASSERT_NE(targetSerializer, nullptr);
    EXPECT_EQ(targetSerializer->getArity(), 3);
    EXPECT_NE(targetSerializer, adaptor.omniAccSerializer_.get());
    EXPECT_EQ(adaptor.sourceSerializers_.at(7), &sourceSerializer);
}

TEST(GroupAggSavepointAdaptorTest, BuildOmniMainMetaInfoRejectsWrongStateAndSerializerMetadata)
{
    GroupAggSavepointAdaptor adaptor;
    omnistream::RowType flinkType(true, std::vector<std::string>{"BIGINT", RAW_TYPE, "INT", "VARCHAR"});
    RowDataSerializer sourceSerializer(&flinkType);
    auto valid = makeKeyValue(ACC_STATE_NAME, "VALUE", VoidNamespaceSerializer::INSTANCE, &sourceSerializer);

    EXPECT_THROW(
        adaptor.buildOmniMainMetaInfo(
            0, *makeMeta("other", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "VALUE")),
        std::runtime_error);
    EXPECT_THROW(adaptor.buildOmniMainMetaInfo(0, *valid), std::runtime_error);

    adaptor.prepareForRestore(GROUP_AGG_DESCRIPTION);
    EXPECT_THROW(
        adaptor.buildOmniMainMetaInfo(0, *makeKeyValue(ACC_STATE_NAME, "VALUE", nullptr, &sourceSerializer)),
        std::runtime_error);
    EXPECT_THROW(
        adaptor.buildOmniMainMetaInfo(
            0, *makeKeyValue(ACC_STATE_NAME, "VALUE", VoidNamespaceSerializer::INSTANCE, nullptr)),
        std::runtime_error);
    EXPECT_THROW(
        adaptor.buildOmniMainMetaInfo(
            0, *makeKeyValue(ACC_STATE_NAME, "VALUE", LongSerializer::INSTANCE, &sourceSerializer)),
        std::runtime_error);
}

TEST(GroupAggSavepointAdaptorTest, TransformKVDataCompactsAccumulatorAndPreservesKey)
{
    GroupAggSavepointAdaptor adaptor;
    adaptor.prepareForRestore(GROUP_AGG_DESCRIPTION);
    omnistream::RowType flinkType(true, std::vector<std::string>{"BIGINT", RAW_TYPE, "INT", "VARCHAR"});
    RowDataSerializer flinkSerializer(&flinkType);
    auto sourceMeta = makeKeyValue(ACC_STATE_NAME, "VALUE", VoidNamespaceSerializer::INSTANCE, &flinkSerializer);
    adaptor.buildOmniMainMetaInfo(7, *sourceMeta);

    std::unique_ptr<BinaryRowData> flinkAccumulator(BinaryRowData::createBinaryRowDataWithMem(4));
    flinkAccumulator->setRowKind(RowKind::UPDATE_BEFORE);
    flinkAccumulator->setLong(0, 1001);
    flinkAccumulator->setLong(1, 999); // Non-null RAW placeholder is deliberately discarded.
    flinkAccumulator->setNullAt(2);
    flinkAccumulator->setStringView(3, "compact");
    auto sourceBytes = serializeRow(flinkSerializer, *flinkAccumulator);
    RecordingRestoreKVState writer;
    const std::vector<int8_t> key{1, 2, 3};

    adaptor.transformKVData(key, sourceBytes, 7, &writer);

    EXPECT_EQ(writer.key, key);
    omnistream::RowType omniType(true, std::vector<std::string>{"BIGINT", "INT", "VARCHAR"});
    RowDataSerializer omniSerializer(&omniType);
    DataInputDeserializer input(
        reinterpret_cast<const uint8_t*>(writer.value.data()), static_cast<int>(writer.value.size()), 0);
    auto* compacted = static_cast<BinaryRowData*>(omniSerializer.deserialize(input));
    ASSERT_NE(compacted, nullptr);
    EXPECT_EQ(compacted->getRowKind(), RowKind::UPDATE_BEFORE);
    EXPECT_EQ(*compacted->getLong(0), 1001);
    EXPECT_TRUE(compacted->isNullAt(1));
    EXPECT_EQ(compacted->getStringView(2), "compact");
}

TEST(GroupAggSavepointAdaptorTest, TransformKVDataRejectsUnknownStateAndNullAccumulator)
{
    GroupAggSavepointAdaptor adaptor;
    adaptor.prepareForRestore(GROUP_AGG_DESCRIPTION);
    RecordingRestoreKVState writer;
    const std::vector<int8_t> bytes{1, 2, 3};
    EXPECT_THROW(adaptor.transformKVData(bytes, bytes, 99, &writer), std::runtime_error);

    NullDeserializingSerializer nullSerializer;
    auto sourceMeta = makeKeyValue(ACC_STATE_NAME, "VALUE", VoidNamespaceSerializer::INSTANCE, &nullSerializer);
    adaptor.buildOmniMainMetaInfo(4, *sourceMeta);
    EXPECT_THROW(adaptor.transformKVData(bytes, bytes, 4, &writer), std::runtime_error);
}

TEST(GroupAggSavepointAdaptorTest, RestoreFlowCompactsAccumulatorAndPassesThroughDataViewState)
{
    GroupAggSavepointAdaptor adaptor;
    const nlohmann::json description = {
        {"aggInfoList", {{"accTypes", {RAW_TYPE, "BIGINT"}}}},
    };
    adaptor.prepareForRestore(description);
    omnistream::RowType sourceType(true, std::vector<std::string>{RAW_TYPE, "BIGINT"});
    RowDataSerializer sourceSerializer(&sourceType);
    auto accMeta = makeKeyValue(ACC_STATE_NAME, "VALUE", VoidNamespaceSerializer::INSTANCE, &sourceSerializer);
    auto distinctMeta = makeKeyValue(DISTINCT_STATE_NAME, "MAP");
    adaptor.validateForRestore({accMeta, distinctMeta});
    std::unique_ptr<BinaryRowData> source(BinaryRowData::createBinaryRowDataWithMem(2));
    source->setLong(0, 123); // Flink may persist a non-null DataView placeholder.
    source->setLong(1, 37);
    const auto sourceBytes = serializeRow(sourceSerializer, *source);
    const std::vector<int8_t> accKey{1, 2};
    const std::vector<int8_t> distinctKey{3, 4};
    const std::vector<int8_t> distinctValue{5, 6, 7};
    auto bridge = std::make_shared<NiceMock<MockSavepointBridge>>();
    ON_CALL(*bridge, readMetaData(_))
        .WillByDefault(Return(std::vector<StateMetaInfoSnapshot>{*accMeta, *distinctMeta}));
    ON_CALL(*bridge, getSavepointInputStream(_)).WillByDefault(Return(kMockProvider));
    EXPECT_CALL(*bridge, getKeyGroupEntries(_, _, _, _))
        .WillOnce([&](jobject, int& stateId, bool, std::vector<KeyGroupEntry>& entries) {
            entries = {KeyGroupEntry(0, accKey, sourceBytes), KeyGroupEntry(1, distinctKey, distinctValue)};
            stateId = 0xFFFF;
        });
    auto file = std::make_shared<FileStateHandle>(Path("/tmp/group-agg-savepoint-test"), 128);
    auto handle = std::make_shared<KeyGroupsStateHandle>(KeyGroupRangeOffsets(5, 5, {16}), file);
    SavepointRestoreResultIterator iterator({handle}, bridge);
    CapturingRestoreBackend backend;

    adaptor.restore(iterator, backend);

    auto* serializer = dynamic_cast<RowDataSerializer*>(backend.accumulatorSerializer.get());
    ASSERT_NE(serializer, nullptr);
    EXPECT_EQ(serializer->getArity(), 1);
    const auto& acc = backend.entries[0];
    EXPECT_EQ(acc.key, accKey);
    EXPECT_EQ(acc.keyGroup, 5);
    EXPECT_TRUE(acc.flushed);
    DataInputDeserializer input(reinterpret_cast<const uint8_t*>(acc.value.data()), acc.value.size(), 0);
    auto* restored = static_cast<BinaryRowData*>(serializer->deserialize(input));
    ASSERT_NE(restored, nullptr);
    EXPECT_EQ(*restored->getLong(0), 37);
    EXPECT_EQ(backend.entries[1].key, distinctKey);
    EXPECT_EQ(backend.entries[1].value, distinctValue);
    EXPECT_EQ(backend.entries[1].keyGroup, 5);
    EXPECT_TRUE(backend.entries[1].flushed);
}

TEST(GroupAggSavepointAdaptorTest, SaveDelegatesEmptySnapshotToVectorBatchFlow)
{
    GroupAggSavepointAdaptor adaptor;
    auto bridge = std::make_shared<NiceMock<MockSavepointBridge>>();
    EXPECT_CALL(*bridge, AcquireSavepointOutputStream(1L, _)).WillOnce(Return(kMockProvider));
    EXPECT_CALL(*bridge, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(static_cast<jobject>(nullptr)));
    EXPECT_CALL(*bridge, WriteSavepointMetadata(kMockProvider, _, "key-serializer"));
    EXPECT_CALL(*bridge, GetSavepointOutputStreamPos(kMockProvider)).WillOnce(Return(0L));
    std::unique_ptr<SavepointType> savepointType(SavepointType::savepoint(SavepointFormatType::CANONICAL));
    std::unique_ptr<CheckpointOptions> checkpointOptions(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));
    CheckpointStateOutputStreamProxy stream(bridge, 1L, checkpointOptions.get());
    TestFullSnapshotResources resources;
    auto iterator = std::make_shared<EmptyKeyValueStateIterator>();
    resources.iterator = iterator;
    KeyGroupRangeOffsets offsets(resources.keyGroupRange);

    EXPECT_NO_THROW(adaptor.save(stream, offsets, resources, "key-serializer"));
    EXPECT_TRUE(iterator->closed);
}

TEST(GroupAggSavepointAdaptorTest, RestoreClearsSourceSerializersAndAcceptsEmptyIterator)
{
    GroupAggSavepointAdaptor adaptor;
    adaptor.sourceSerializers_[1] = LongSerializer::INSTANCE;
    SavepointRestoreResultIterator iterator;
    EmptyRestoreBackend backend;

    EXPECT_NO_THROW(adaptor.restore(iterator, backend));
    EXPECT_TRUE(adaptor.sourceSerializers_.empty());
}
