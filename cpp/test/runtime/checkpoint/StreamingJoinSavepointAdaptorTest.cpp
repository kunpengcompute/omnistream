/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeinfo/BasicTypeInfo.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "core/typeutils/JoinTupleSerializer2.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/TupleSerializer.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "runtime/checkpoint/StreamingJoinSavepointAdaptor.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"

using namespace omnistream;

namespace {

nlohmann::json leftOuterDescription()
{
    return {
        {"leftInputTypes", {"BIGINT", "VARCHAR"}},
        {"rightInputTypes", {"BIGINT"}},
    };
}

nlohmann::json innerDescription()
{
    return {
        {"leftInputTypes", {"BIGINT"}},
        {"rightInputTypes", {"BIGINT"}},
    };
}

TypeSerializer* createFlinkOuterValueSerializer()
{
    BasicTypeInfo firstIntType(TYPE_NAME_INT_SERIALIZER);
    BasicTypeInfo secondIntType(TYPE_NAME_INT_SERIALIZER);
    std::vector<TypeInformation*> tupleFieldTypes{&firstIntType, &secondIntType};
    return new Tuple2Serializer(tupleFieldTypes);
}

std::shared_ptr<StateMetaInfoSnapshot> makeMapMetaInfo(
    const std::string& stateName, const std::vector<std::string>& inputTypes, TypeSerializer* valueSerializer)
{
    omnistream::RowType rowType(true, inputTypes);
    auto* mapSerializer = new MapSerializer(new RowDataSerializer(&rowType), valueSerializer);
    RegisteredKeyValueStateBackendMetaInfo metaInfo(
        StateDescriptor::Type::MAP, stateName, VoidNamespaceSerializer::INSTANCE, mapSerializer);
    return metaInfo.snapshot();
}

std::vector<std::shared_ptr<StateMetaInfoSnapshot>> makeFlinkLeftOuterMetaInfos()
{
    return {
        makeMapMetaInfo(
            StreamingJoinSavepointUtil::LEFT_STATE_NAME, {"BIGINT", "VARCHAR"}, createFlinkOuterValueSerializer()),
        makeMapMetaInfo(StreamingJoinSavepointUtil::RIGHT_STATE_NAME, {"BIGINT"}, new IntSerializer()),
    };
}

std::vector<std::shared_ptr<StateMetaInfoSnapshot>> makeOmniLeftOuterMetaInfos()
{
    return {
        makeMapMetaInfo(StreamingJoinSavepointUtil::LEFT_STATE_NAME, {"BIGINT", "VARCHAR"}, new JoinTupleSerializer()),
        makeMapMetaInfo(StreamingJoinSavepointUtil::RIGHT_STATE_NAME, {"BIGINT"}, new IntSerializer()),
    };
}

std::vector<int8_t> copyOutput(DataOutputSerializer& output)
{
    return std::vector<int8_t>(
        reinterpret_cast<int8_t*>(output.getData()),
        reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
}

std::vector<int8_t> makeRowBytes(int64_t value)
{
    std::unique_ptr<BinaryRowData> row(BinaryRowData::createBinaryRowDataWithMem(1));
    row->setLong(0, value);
    BinaryRowDataSerializer serializer(1);
    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    serializer.serialize(row.get(), output);
    return copyOutput(output);
}

std::vector<int8_t> makeExpandedValue(int32_t count, int32_t numAssociations, bool outer)
{
    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    output.writeBoolean(false);
    output.writeInt(count);
    if (outer) {
        output.writeInt(numAssociations);
    }
    return copyOutput(output);
}

std::vector<int8_t> makeAggregatedValue(
    const std::vector<std::tuple<std::vector<int8_t>, int32_t, int32_t>>& entries, bool outer)
{
    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    output.writeInt(static_cast<int32_t>(entries.size()));
    for (const auto& [rowBytes, count, numAssociations] : entries) {
        std::vector<uint8_t> unsignedRowBytes(rowBytes.begin(), rowBytes.end());
        output.write(unsignedRowBytes);
        output.writeBoolean(false);
        output.writeInt(count);
        if (outer) {
            output.writeInt(numAssociations);
        }
    }
    return copyOutput(output);
}

VectorBatchSaveStateContext makeTransformContext(const std::string& stateName)
{
    VectorBatchSaveStateContext context;
    context.writable = true;
    context.mappedKvStateId = 0;
    context.logicalStateName = stateName;
    context.valueSerializer = IntSerializer::INSTANCE;
    context.stateType = VectorBatchStateType::KV_TRANSFORM;
    return context;
}

class NoVbSnapshotResources : public FullSnapshotResources {
public:
    explicit NoVbSnapshotResources(size_t stateCount) : metaInfos_(stateCount)
    {
    }

    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() override
    {
        return metaInfos_;
    }

    KeyGroupRange* getKeyGroupRange() override
    {
        return nullptr;
    }

    TypeSerializer* getKeySerializer() override
    {
        return nullptr;
    }

    std::shared_ptr<KeyValueStateIterator> createKVStateIterator() override
    {
        return nullptr;
    }

    void cleanup() override
    {
    }

private:
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos_;
};

} // namespace

TEST(StreamingJoinSavepointAdaptorTest, FlinkRestoreBuildsStandardRowStateMetadata)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor);
    adaptor.prepareForRestore(leftOuterDescription());

    auto flinkMetaInfos = makeFlinkLeftOuterMetaInfos();
    EXPECT_NO_THROW(adaptor.validateForRestore(flinkMetaInfos));
    EXPECT_EQ(adaptor.getStateType(*flinkMetaInfos[0]), RestoreStateType::KV_TRANSFORMED);
    EXPECT_EQ(adaptor.getStateType(*flinkMetaInfos[1]), RestoreStateType::KV_TRANSFORMED);

    auto leftOmniMeta = adaptor.buildOmniMainMetaInfo(0, *flinkMetaInfos[0]);
    auto* leftMapSerializer = dynamic_cast<MapSerializer*>(leftOmniMeta.getValueSerializer());
    ASSERT_NE(leftMapSerializer, nullptr);
    EXPECT_EQ(leftMapSerializer->getKeySerializer()->getBackendId(), BackendDataType::ROW_BK);
    EXPECT_EQ(leftMapSerializer->getValueSerializer()->getBackendId(), BackendDataType::TUPLE_INT32_INT32);
    auto rightOmniMeta = adaptor.buildOmniMainMetaInfo(1, *flinkMetaInfos[1]);
    auto* rightMapSerializer = dynamic_cast<MapSerializer*>(rightOmniMeta.getValueSerializer());
    ASSERT_NE(rightMapSerializer, nullptr);
    EXPECT_EQ(rightMapSerializer->getKeySerializer()->getBackendId(), BackendDataType::ROW_BK);
    EXPECT_EQ(rightMapSerializer->getValueSerializer()->getBackendId(), BackendDataType::INT_BK);
}

TEST(StreamingJoinSavepointAdaptorTest, PrepareRejectsMalformedAndUnsupportedInputTypes)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);

    auto description = innerDescription();
    description[StreamingJoinSavepointUtil::LEFT_INPUT_TYPES_FIELD] = {"BIGINT", 1};
    EXPECT_THROW(adaptor.prepareForRestore(description), std::runtime_error);

    description = innerDescription();
    description[StreamingJoinSavepointUtil::LEFT_INPUT_TYPES_FIELD] = {"UNKNOWN"};
    EXPECT_THROW(adaptor.prepareForRestore(description), std::runtime_error);

    description = innerDescription();
    description[StreamingJoinSavepointUtil::LEFT_INPUT_TYPES_FIELD] = nlohmann::json::array();
    EXPECT_THROW(adaptor.prepareForRestore(description), std::runtime_error);
}

TEST(StreamingJoinSavepointAdaptorTest, NativeJoinTupleMetadataStaysPojoAndCompatibleSnapshotUsesFlinkTuple)
{
    JoinTupleSerializer serializer;
    auto serializerJson = nlohmann::json::parse(serializer.toJson());
    EXPECT_EQ(serializerJson.at("type").get<int>(), static_cast<int>(SerializerType::POJO));
    EXPECT_EQ(serializerJson.at("element_type").get<std::string>(), TYPE_NAME_JOIN_TUPLE_CLASS);

    auto omniMetaInfos = makeOmniLeftOuterMetaInfos();
    auto flinkMetaInfo = StreamingJoinSavepointUtil::createFlinkMapStateSnapshot(
        StreamingJoinSavepointUtil::LEFT_STATE_NAME, *omniMetaInfos[0], {"BIGINT", "VARCHAR"}, true);
    auto* flinkMapSerializer = dynamic_cast<MapSerializer*>(flinkMetaInfo->getValueSerializer());
    ASSERT_NE(flinkMapSerializer, nullptr);
    auto flinkValueJson = nlohmann::json::parse(flinkMapSerializer->getValueSerializer()->toJson());
    EXPECT_EQ(flinkValueJson.at("type").get<int>(), static_cast<int>(SerializerType::TUPLE));
    EXPECT_EQ(flinkValueJson.at("element_type").get<std::string>(), "org.apache.flink.api.java.tuple.Tuple2");
    ASSERT_TRUE(flinkValueJson.at("fieldSerializers").is_array());
    EXPECT_EQ(flinkValueJson.at("fieldSerializers").size(), 2);
}

TEST(StreamingJoinSavepointAdaptorTest, SaveValidationAcceptsCurrentSchemaAndRejectsLegacyHashSchema)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor);
    adaptor.prepareForSave(leftOuterDescription());

    auto currentMetaInfos = makeOmniLeftOuterMetaInfos();
    EXPECT_NO_THROW(adaptor.validateForSave(currentMetaInfos));

    auto legacyLeft = std::make_shared<StateMetaInfoSnapshot>(
        StreamingJoinSavepointUtil::LEFT_STATE_NAME,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        std::unordered_map<std::string, std::string>{{"KEYED_STATE_TYPE", "MAP"}},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        std::unordered_map<std::string, TypeSerializer*>{
            {StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY, VoidNamespaceSerializer::INSTANCE},
            {StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY,
             new MapSerializer(new XxH128_hashSerializer(), new JoinTupleSerializer2())}});
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> legacyMetaInfos{legacyLeft, currentMetaInfos[1]};
    EXPECT_THROW(adaptor.validateForSave(legacyMetaInfos), std::runtime_error);
}

TEST(StreamingJoinSavepointAdaptorTest, SaveValidationAcceptsOptionalRegisteredSideTables)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor);
    adaptor.prepareForSave(leftOuterDescription());
    auto metaInfos = makeOmniLeftOuterMetaInfos();
    metaInfos.push_back(makeMapMetaInfo(
        std::string(StreamingJoinSavepointUtil::LEFT_STATE_NAME) + "vb", {"BIGINT"}, IntSerializer::INSTANCE));
    metaInfos.push_back(makeMapMetaInfo(
        std::string(StreamingJoinSavepointUtil::RIGHT_STATE_NAME) + "vb", {"BIGINT"}, IntSerializer::INSTANCE));

    EXPECT_NO_THROW(adaptor.validateForSave(metaInfos));
}

TEST(StreamingJoinSavepointAdaptorTest, SaveContextsUseRowStateConversion)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    NoVbSnapshotResources resources(2);
    VectorBatchSavePlan plan;
    plan.kvStateIdMapping.emplace(0, 0);
    plan.kvStateIdMapping.emplace(1, 1);
    plan.stateContextSpecs.push_back(
        {0, StreamingJoinSavepointUtil::LEFT_STATE_NAME, IntSerializer::INSTANCE, VectorBatchStateType::KV_TRANSFORM});
    plan.stateContextSpecs.push_back(
        {1, StreamingJoinSavepointUtil::RIGHT_STATE_NAME, IntSerializer::INSTANCE, VectorBatchStateType::KV_TRANSFORM});

    auto contexts = adaptor.buildSaveStateContexts(resources, plan);

    ASSERT_EQ(contexts.size(), 2);
    EXPECT_TRUE(contexts[0].isValid());
    EXPECT_TRUE(contexts[1].isValid());
}
