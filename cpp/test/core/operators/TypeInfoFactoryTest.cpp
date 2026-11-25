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
#include "typeinfo/TypeInfoFactory.h"
#include "table/types/logical/LogicalType.h"
#include "typeinfo/StringTypeInfo.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "execution/Environment.h"
#include "basictypes/String.h"
#include "runtime/state/VoidNamespace.h"
#include "streaming/api/operators/TimerHeapInternalTimer.h"
#include "streaming/api/operators/StreamTaskStateInitializerImpl.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "api/common/TaskInfoImpl.h"
#include "streaming/api/operators/StreamOperatorStateHandler.h"
#include "typeinfo/LongTypeInfo.h"
#include "test/core/operators/source/User.h"
#include "basictypes/ReflectMacros.h"
#include <nlohmann/json.hpp>
#include <limits>
#include <memory>
#include <stdexcept>
#include <iostream>

// 测试用例定义
TEST(TypeInfoFactoryTest, DISABLED_PojoValueStateTest)
{
    std::string pojo = "{\n"
                       "\t\"fieldSerializers\": [\n"
                       "\t\t{\n"
                       "\t\t\t\"serializerName\": \"org.apache.flink.api.common.typeutils.base.LongSerializer\"\n"
                       "\t\t},\n"
                       "\t\t{\n"
                       "\t\t\t\"serializerName\": \"org.apache.flink.api.common.typeutils.base.StringSerializer\"\n"
                       "\t\t}\n"
                       "\t],\n"
                       "\t\"serializerName\": \"org.apache.flink.api.java.typeutils.runtime.PojoSerializer\",\n"
                       "\t\"fields\": [\n"
                       "\t\t\"age\",\n"
                       "\t\t\"name\"\n"
                       "\t],\n"
                       "\t\"clazz\": \"User\"\n"
                       "}";
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(pojo);
    TypeInformation* typeInfo = TypeInfoFactory::createDataStreamTypeInfo(opDescriptionJSON);
    omnistream::RuntimeEnvironmentV2* env = new omnistream::RuntimeEnvironmentV2();
    StreamTaskStateInitializerImpl initializer(env);
    StreamOperatorStateContextImpl<long>* context =
        initializer.streamOperatorStateContext<long>(new LongSerializer(), nullptr, nullptr);
    auto stateHandler = new StreamOperatorStateHandler<long>(context);
    auto keyStore = stateHandler->getKeyedStateStore();
    auto vs = new DataStreamValueStateDescriptor("vs", typeInfo);
    vs->SetStateSerializer(typeInfo->createTypeSerializer());
    auto stateVs = keyStore->getState(vs);
    auto value = stateVs->value();
    ASSERT_EQ(value, nullptr);
    auto user = new User();
    user->name = new String("tiantao");
    user->age = 30L;
    stateVs->update(user);
    auto stateUser = reinterpret_cast<User*>(stateVs->value());
    ASSERT_EQ(stateUser->name->getValue(), "tiantao");
    ASSERT_EQ(stateUser->age, 30L);
    delete typeInfo;
}

TEST(TypeInfoFactoryTest, DISABLED_PojoMapStateTest)
{
    std::string pojo = "{\n"
                       "\t\"fieldSerializers\": [\n"
                       "\t\t{\n"
                       "\t\t\t\"serializerName\": \"org.apache.flink.api.common.typeutils.base.LongSerializer\"\n"
                       "\t\t},\n"
                       "\t\t{\n"
                       "\t\t\t\"serializerName\": \"org.apache.flink.api.common.typeutils.base.StringSerializer\"\n"
                       "\t\t}\n"
                       "\t],\n"
                       "\t\"serializerName\": \"org.apache.flink.api.java.typeutils.runtime.PojoSerializer\",\n"
                       "\t\"fields\": [\n"
                       "\t\t\"age\",\n"
                       "\t\t\"name\"\n"
                       "\t],\n"
                       "\t\"clazz\": \"User\"\n"
                       "}";
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(pojo);
    TypeInformation* typeInfo = TypeInfoFactory::createDataStreamTypeInfo(opDescriptionJSON);
    omnistream::RuntimeEnvironmentV2* env = new omnistream::RuntimeEnvironmentV2();
    StreamTaskStateInitializerImpl initializer(env);
    StreamOperatorStateContextImpl<long>* context =
        initializer.streamOperatorStateContext<long>(new LongSerializer(), nullptr, nullptr);
    auto stateHandler = new StreamOperatorStateHandler<long>(context);
    auto keyStore = stateHandler->getKeyedStateStore();
    auto keyInfo = new LongTypeInfo("long");

    auto ms = new DataStreamMapStateDescriptor(
        "ms", new BasicTypeInfo(LongSerializer::INSTANCE, TYPE_NAME_LONG_SERIALIZER), typeInfo);
    auto stateMs = keyStore->getMapState(ms);
    auto iterator = stateMs->iterator();
    bool hasNext = iterator->hasNext();
    ASSERT_EQ(hasNext, false);

    auto user = new User();
    user->name = new String("tiantao");
    user->age = 30L;
    auto givenKey = new Long(50);
    stateMs->put(givenKey, user);
    auto valueUser = reinterpret_cast<User*>(stateMs->get(givenKey).value());
    ASSERT_EQ(valueUser->name->getValue(), "tiantao");
    ASSERT_EQ(valueUser->age, 30L);

    iterator = stateMs->iterator();
    hasNext = iterator->hasNext();
    ASSERT_EQ(hasNext, true);
    auto entry = reinterpret_cast<MapEntry*>(iterator->next());
    valueUser = reinterpret_cast<User*>(entry->getValue());
    ASSERT_EQ(valueUser->name->getValue(), "tiantao");
    ASSERT_EQ(valueUser->age, 30L);

    auto key = reinterpret_cast<Long*>(entry->getKey());
    ASSERT_EQ(key->getValue(), 50L);

    hasNext = iterator->hasNext();
    ASSERT_EQ(hasNext, false);

    bool contains = stateMs->contains(givenKey);
    ASSERT_EQ(contains, true);

    stateMs->remove(givenKey);
    contains = stateMs->contains(givenKey);
    ASSERT_EQ(contains, false);
    ASSERT_EQ(stateMs->get(givenKey).has_value(), false);

    iterator = stateMs->iterator();
    hasNext = iterator->hasNext();
    ASSERT_EQ(hasNext, false);
}

// 测试用例定义
TEST(TypeInfoFactoryTest, CreateTypeInfoStringTest)
{
    TypeInformation* typeInfo = TypeInfoFactory::createTypeInfo(TYPE_NAME_STRING);
    EXPECT_TRUE(typeInfo != nullptr);
    delete typeInfo;
}

TEST(TypeInfoFactoryTest, CreateTypeInfoUnsupportedTypeTest)
{
    EXPECT_THROW(
        {
            try {
                TypeInformation* typeInfo = TypeInfoFactory::createTypeInfo("UnsupportedType");
                delete typeInfo;
            } catch (const std::exception& e) {
                THROW_LOGIC_EXCEPTION("Exception caught: " + std::string(e.what()));
            }
        },
        std::exception);
}

TEST(TypeInfoFactoryTest, CreateInternalTypeInfoRowTest)
{
    const json& rowType = R"(
            [{"type":"Long"}, {"type":"Long"}]
    )"_json;

    TypeInformation* typeInfo = TypeInfoFactory::createTupleTypeInfo(rowType);
    EXPECT_TRUE(typeInfo != nullptr);
    delete typeInfo;
}

TEST(TypeInfoFactoryTest, CreateTupleTypeInfoNonArrayTest)
{
    json fieldType = {{"type", "BIGINT"}};

    EXPECT_THROW(
        {
            try {
                TypeInformation* typeInfo = TypeInfoFactory::createTupleTypeInfo(fieldType);
                delete typeInfo;
            } catch (const std::exception& e) {
                THROW_LOGIC_EXCEPTION("Exception caught: " + std::string(e.what()));
            }
        },
        std::exception);
}

TEST(TypeInfoFactoryTest, CreateCommittableMessageInfoTest)
{
    TypeInformation* typeInfo = TypeInfoFactory::createCommittableMessageInfo();
    EXPECT_TRUE(typeInfo != nullptr);
    delete typeInfo;
}

TEST(TypeInfoFactoryTest, CreateBasicInternalTypeInfoTest)
{
    EXPECT_THROW(
        {
            try {
                TypeInformation* typeInfo = TypeInfoFactory::createBasicInternalTypeInfo("UnsupportedType");
                delete typeInfo;
            } catch (const std::exception& e) {
                THROW_LOGIC_EXCEPTION("Exception caught: " + std::string(e.what()));
            }
        },
        std::exception);
}

TEST(TypeInfoFactoryTest, CreateLogicalTypeBigIntTest)
{
    LogicalType* logicalType = TypeInfoFactory::createLogicalType("'BIGINT'");
    EXPECT_TRUE(logicalType->getTypeId() == BasicLogicalType::BIGINT->getTypeId());
    delete logicalType;
}

TEST(TypeInfoFactoryTest, CreateLogicalTypeTimeWithoutTimeZoneTest)
{
    LogicalType* logicalType = TypeInfoFactory::createLogicalType("'TIME_WITHOUT_TIME_ZONE'");
    EXPECT_TRUE(logicalType->getTypeId() == BasicLogicalType::TIME_WITHOUT_TIME_ZONE->getTypeId());
    delete logicalType;
}

TEST(TypeInfoFactoryTest, CreateLogicalTypeTimestampWithoutTimeZoneTest)
{
    LogicalType* logicalType = TypeInfoFactory::createLogicalType("'TIMESTAMP_WITHOUT_TIME_ZONE'");
    EXPECT_TRUE(logicalType->getTypeId() == BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE->getTypeId());
    delete logicalType;
}

TEST(TypeInfoFactoryTest, CreateLogicalTypeTimestampWithTimeZoneTest)
{
    LogicalType* logicalType = TypeInfoFactory::createLogicalType("'TIMESTAMP_WITH_TIME_ZONE'");
    EXPECT_TRUE(logicalType->getTypeId() == BasicLogicalType::TIMESTAMP_WITH_TIME_ZONE->getTypeId());
    delete logicalType;
}

TEST(TypeInfoFactoryTest, CreateLogicalTypeTimestampWithLocalTimeZoneTest)
{
    LogicalType* logicalType = TypeInfoFactory::createLogicalType("'TIMESTAMP_WITH_LOCAL_TIME_ZONE'");
    EXPECT_TRUE(logicalType->getTypeId() == BasicLogicalType::TIMESTAMP_WITH_LOCAL_TIME_ZONE->getTypeId());
    delete logicalType;
}

TEST(TypeInfoFactoryTest, CreateDataTypeWithChildrenTest)
{
    const nlohmann::json logicalTypeJson = nlohmann::json::parse(R"({
        "type": "MAP",
        "nullable": false,
        "children": [
            {"type": "VARCHAR", "nullable": false},
            {
                "type": "ARRAY",
                "nullable": true,
                "children": [{"type": "BIGINT", "nullable": true}]
            }
        ]
    })");

    std::unique_ptr<LogicalType> logicalType(TypeInfoFactory::createDataType(logicalTypeJson));

    EXPECT_EQ(logicalType->getTypeId(), omniruntime::type::DataTypeId::OMNI_MAP);
    EXPECT_FALSE(logicalType->isNullable());
    ASSERT_EQ(logicalType->getChildren().size(), 2);
    EXPECT_EQ(logicalType->getChildren()[1]->getTypeId(), omniruntime::type::DataTypeId::OMNI_ARRAY);
    nlohmann::json expectedJson = logicalTypeJson;
    expectedJson["children"][0]["length"] = std::numeric_limits<int>::max();
    EXPECT_EQ(logicalType->toJson(), expectedJson);
}

TEST(TypeInfoFactoryTest, CreateDataTypeVarCharRoundTripTest)
{
    const nlohmann::json logicalTypeJson = nlohmann::json::parse(R"({
        "type": "VARCHAR",
        "nullable": true,
        "length": 32
    })");

    std::unique_ptr<LogicalType> logicalType(TypeInfoFactory::createDataType(logicalTypeJson));

    EXPECT_EQ(logicalType->toJson(), logicalTypeJson);
}

TEST(TypeInfoFactoryTest, CreateDataTypeTimestampRoundTripTest)
{
    const nlohmann::json logicalTypeJson = nlohmann::json::parse(R"({
        "type": "TIMESTAMP_WITHOUT_TIME_ZONE",
        "nullable": false,
        "precision": 3
    })");

    std::unique_ptr<LogicalType> logicalType(TypeInfoFactory::createDataType(logicalTypeJson));

    EXPECT_EQ(logicalType->toJson(), logicalTypeJson);
}

TEST(TypeInfoFactoryTest, CreateDataTypeTimeRoundTripTest)
{
    const nlohmann::json logicalTypeJson = nlohmann::json::parse(R"({
        "type": "TIME_WITHOUT_TIME_ZONE",
        "nullable": true,
        "precision": 4
    })");

    std::unique_ptr<LogicalType> logicalType(TypeInfoFactory::createDataType(logicalTypeJson));

    EXPECT_EQ(logicalType->toJson(), logicalTypeJson);
}

TEST(TypeInfoFactoryTest, CreateDataTypeNullNullableDefaultsToTrueTest)
{
    const nlohmann::json logicalTypeJson = nlohmann::json::parse(R"({
        "type": "VARCHAR",
        "nullable": null,
        "length": 32
    })");

    std::unique_ptr<LogicalType> logicalType(TypeInfoFactory::createDataType(logicalTypeJson));

    EXPECT_TRUE(logicalType->isNullable());
}

TEST(TypeInfoFactoryTest, CreateDataTypeBasicLeafRoundTripTest)
{
    const nlohmann::json logicalTypeJson = nlohmann::json::parse(R"({
        "type": "DOUBLE",
        "nullable": false
    })");

    std::unique_ptr<LogicalType> logicalType(TypeInfoFactory::createDataType(logicalTypeJson));

    EXPECT_EQ(logicalType->toJson(), logicalTypeJson);
}

TEST(TypeInfoFactoryTest, CreateTimerSerializerFallsBackOnEmptyClassNameTest)
{
    const nlohmann::json timerJson = nlohmann::json::parse(R"({
        "serializerName": "org.apache.flink.streaming.api.operators.TimerSerializer",
        "keySerializer": {
            "serializerName": "org.apache.flink.api.common.typeutils.base.StringSerializer",
            "serializerInstanceClazz": ""
        },
        "namespaceSerializer": {
            "serializerName": "org.apache.flink.runtime.state.VoidNamespaceSerializer",
            "serializerInstanceClazz": ""
        }
    })");

    std::unique_ptr<TypeSerializer> serializer;
    std::unique_ptr<TypeInformation> typeInfo(TypeInfoFactory::createDataStreamTypeInfo(timerJson));
    ASSERT_NE(typeInfo, nullptr);

    serializer.reset(typeInfo->createTypeSerializer());
    ASSERT_NE(serializer, nullptr);
    serializer->setSelfBufferReusable(false);

    auto* timer = static_cast<TimerHeapInternalTimer<Object*, Object*>*>(serializer->GetBuffer());
    ASSERT_NE(timer, nullptr);
    ASSERT_NE(dynamic_cast<String*>(timer->getKey()), nullptr);
    ASSERT_NE(dynamic_cast<VoidNamespace*>(timer->getNamespace()), nullptr);

    timer->putRefCount();
    typeInfo.reset();
}

TEST(TypeInfoFactoryTest, MapSerializerDescriptionsPreserveKeyAndValueSchema)
{
    const auto canonical = nlohmann::json::parse(R"({
        "serializerName": "org.apache.flink.api.common.typeutils.base.MapSerializer",
        "keySerializer": {
            "serializerName": "org.apache.flink.api.common.typeutils.base.LongSerializer"
        },
        "valueSerializer": {
            "serializerName": "org.apache.flink.api.common.typeutils.base.StringSerializer"
        }
    })");
    auto legacy = canonical;
    legacy["fieldSerializers"] = nlohmann::json::array({canonical["keySerializer"], canonical["valueSerializer"]});
    legacy.erase("keySerializer");
    legacy.erase("valueSerializer");
    auto emptyChildren = legacy;
    emptyChildren["keySerializer"] = nlohmann::json::object();
    emptyChildren["valueSerializer"] = nullptr;

    for (const auto& description : {canonical, legacy, emptyChildren}) {
        std::unique_ptr<TypeInformation> typeInfo(TypeInfoFactory::createDataStreamTypeInfo(description));
        ASSERT_NE(typeInfo, nullptr);
        std::unique_ptr<TypeSerializer> serializer(typeInfo->createTypeSerializer());
        auto* mapSerializer = dynamic_cast<MapSerializer*>(serializer.get());
        ASSERT_NE(mapSerializer, nullptr);
        const auto metadata = nlohmann::json::parse(mapSerializer->toJson());
        EXPECT_EQ(
            nlohmann::json::parse(metadata["keySerializer"].get<std::string>())["type"],
            static_cast<int>(SerializerType::LONG));
        EXPECT_EQ(
            nlohmann::json::parse(metadata["valueSerializer"].get<std::string>())["type"],
            static_cast<int>(SerializerType::STRING_T));
    }
}

TEST(TypeInfoFactoryTest, MapSerializerRejectsDescriptionsWithoutBothChildSchemas)
{
    const auto missingChildren = nlohmann::json::parse(R"({
        "serializerName": "org.apache.flink.api.common.typeutils.base.MapSerializer",
        "clazz": "java.util.HashMap"
    })");
    auto incompleteLegacy = missingChildren;
    incompleteLegacy["fieldSerializers"] =
        nlohmann::json::array({{{"serializerName", "org.apache.flink.api.common.typeutils.base.LongSerializer"}}});
    auto nullChildren = missingChildren;
    nullChildren["keySerializer"] = nullptr;
    nullChildren["valueSerializer"] = nullptr;

    for (const auto& description : {missingChildren, incompleteLegacy, nullChildren}) {
        EXPECT_THROW(
            { std::unique_ptr<TypeInformation> typeInfo(TypeInfoFactory::createDataStreamTypeInfo(description)); },
            std::runtime_error);
    }
}
