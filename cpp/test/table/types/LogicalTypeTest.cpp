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

#include "table/types/logical/RawType.h"
#include <memory>
#include <gtest/gtest.h>
#include "table/types/logical/LogicalType.h"
#include <string.h>
#include "OmniOperatorJIT/core/src/type/data_type.h"

TEST(LogicalTypeTest, flinkTypeToOmniTypeIdTest)
{
    // types without extra info
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("BIGINT"), omniruntime::type::DataTypeId::OMNI_LONG);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("BOOLEAN"), omniruntime::type::DataTypeId::OMNI_BOOLEAN);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("DATE"), omniruntime::type::DataTypeId::OMNI_DATE32);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("DOUBLE"), omniruntime::type::DataTypeId::OMNI_DOUBLE);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("FLOAT"), omniruntime::type::DataTypeId::OMNI_INT);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("INT"), omniruntime::type::DataTypeId::OMNI_INT);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("SMALLINT"), omniruntime::type::DataTypeId::OMNI_SHORT);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("TINYINT"), omniruntime::type::DataTypeId::OMNI_SHORT);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("BYTES"), omniruntime::type::DataTypeId::OMNI_VARCHAR);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("STRING"), omniruntime::type::DataTypeId::OMNI_VARCHAR);

    // types with extra info (precision, length, scale...)
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("ARRAY<INT>"), omniruntime::type::DataTypeId::OMNI_ARRAY);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("ARRAY<DECIMAL(10, 2)>"), omniruntime::type::DataTypeId::OMNI_ARRAY);

    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("BINARY(2147483647)"), omniruntime::type::DataTypeId::OMNI_VARCHAR);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("CHAR(2147483647)"), omniruntime::type::DataTypeId::OMNI_CHAR);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("DECIMAL(10, 2)"), omniruntime::type::DataTypeId::OMNI_DECIMAL64);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("MAP<STRING, BIGINT>"), omniruntime::type::DataTypeId::OMNI_MAP);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("MULTISET<STRING>"), omniruntime::type::DataTypeId::OMNI_MULTISET);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("RAW('class', 'snapshot')"), omniruntime::type::DataTypeId::OMNI_CONTAINER);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("ROW<myField INT, myOtherField BOOLEAN>"),
        omniruntime::type::DataTypeId::OMNI_CONTAINER);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("SYMBOL('class')"), omniruntime::type::DataTypeId::OMNI_CONTAINER);

    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("VARBINARY(2147483647)"), omniruntime::type::DataTypeId::OMNI_VARCHAR);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("VARCHAR(2147483647)"), omniruntime::type::DataTypeId::OMNI_VARCHAR);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("TIME(9)"), omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE);
    // INTERVALs
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL DAY(6)"), omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL DAY(6) TO HOUR"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL DAY(6) TO MINUTE"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL DAY(6) TO SECOND(9)"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL HOUR"), omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL HOUR TO MINUTE"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL HOUR TO SECOND(9)"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL MINUTE"), omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL MINUTE TO SECOND(9)"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL SECOND(9)"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);

    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL YEAR(4)"), omniruntime::type::DataTypeId::OMNI_INTERVAL_MONTHS);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL YEAR(4) TO MONTH"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_MONTHS);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL MONTH"), omniruntime::type::DataTypeId::OMNI_INTERVAL_MONTHS);

    // TIMESTAMPs
    // TODO: currently we only use the 64 bit OMNI_TIMESTAMP
    //  EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("TIMESTAMP(9)"),
    //  omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("TIMESTAMP(9)"), omniruntime::type::DataTypeId::OMNI_TIMESTAMP);

    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("TIMESTAMP(9) WITH TIME ZONE"),
        omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("TIMESTAMP_LTZ(9)"),
        omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE);

    // types with " NOT NULL" or " NOT NULL *PROCTIME*"
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("BIGINT NOT NULL"), omniruntime::type::DataTypeId::OMNI_LONG);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("BOOLEAN NOT NULL"), omniruntime::type::DataTypeId::OMNI_BOOLEAN);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("DATE NOT NULL"), omniruntime::type::DataTypeId::OMNI_DATE32);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("DOUBLE NOT NULL"), omniruntime::type::DataTypeId::OMNI_DOUBLE);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("FLOAT NOT NULL"), omniruntime::type::DataTypeId::OMNI_INT);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("INT NOT NULL"), omniruntime::type::DataTypeId::OMNI_INT);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("SMALLINT NOT NULL"), omniruntime::type::DataTypeId::OMNI_SHORT);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("TINYINT NOT NULL"), omniruntime::type::DataTypeId::OMNI_SHORT);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("BYTES NOT NULL"), omniruntime::type::DataTypeId::OMNI_VARCHAR);
    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("STRING NOT NULL"), omniruntime::type::DataTypeId::OMNI_VARCHAR);

    EXPECT_EQ(LogicalType::flinkTypeToOmniTypeId("ARRAY<INT> NOT NULL"), omniruntime::type::DataTypeId::OMNI_ARRAY);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("ARRAY<DECIMAL(10, 2)> NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_ARRAY);

    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("BINARY(2147483647) NOT NULL"), omniruntime::type::DataTypeId::OMNI_VARCHAR);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("CHAR(2147483647) NOT NULL"), omniruntime::type::DataTypeId::OMNI_CHAR);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("DECIMAL(10, 2) NOT NULL"), omniruntime::type::DataTypeId::OMNI_DECIMAL64);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("MAP<STRING, BIGINT> NOT NULL"), omniruntime::type::DataTypeId::OMNI_MAP);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("MULTISET<STRING> NOT NULL"), omniruntime::type::DataTypeId::OMNI_MULTISET);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("RAW('class', 'snapshot') NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_CONTAINER);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("ROW<myField INT, myOtherField BOOLEAN> NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_CONTAINER);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("SYMBOL('class') NOT NULL"), omniruntime::type::DataTypeId::OMNI_CONTAINER);

    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("VARBINARY(2147483647) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_VARCHAR);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("VARCHAR(2147483647) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_VARCHAR);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("TIME(9) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE);

    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL DAY(6) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL DAY(6) TO HOUR NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL DAY(6) TO MINUTE NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL DAY(6) TO SECOND(9) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL HOUR NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL HOUR TO MINUTE NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL HOUR TO SECOND(9) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL MINUTE NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL MINUTE TO SECOND(9) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL SECOND(9) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_DAY_TIME);

    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL YEAR(4) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_MONTHS);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL YEAR(4) TO MONTH NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_MONTHS);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("INTERVAL MONTH NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_INTERVAL_MONTHS);

    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("TIMESTAMP(3) NOT NULL"), omniruntime::type::DataTypeId::OMNI_TIMESTAMP);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("TIMESTAMP(9) WITH TIME ZONE NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("TIMESTAMP_LTZ(9) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("TIMESTAMP_LTZ(3) NOT NULL *PROCTIME*"),
        omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE);
}

TEST(LogicalTypeTest, FlinkTypeToOmniTypePreservesNullabilityAndParameters)
{
    std::unique_ptr<LogicalType> timestamp(LogicalType::flinkTypeToOmniType("TIMESTAMP(3) NOT NULL"));
    EXPECT_FALSE(timestamp->isNullable());
    EXPECT_EQ(timestamp->toJson().at("precision"), 3);

    std::unique_ptr<LogicalType> processingTime(
        LogicalType::flinkTypeToOmniType("TIMESTAMP_LTZ(3) NOT NULL *PROCTIME*"));
    EXPECT_FALSE(processingTime->isNullable());
    EXPECT_EQ(processingTime->toJson().at("precision"), 3);

    std::unique_ptr<LogicalType> nullableVarchar(LogicalType::flinkTypeToOmniType("VARCHAR(128)"));
    EXPECT_TRUE(nullableVarchar->isNullable());
    EXPECT_EQ(nullableVarchar->toJson().at("length"), 128);
}

TEST(LogicalTypeTest, FlinkCharTypePreservesTypeIdAndLength)
{
    std::unique_ptr<LogicalType> nullableType(LogicalType::flinkTypeToOmniType("CHAR(12)"));

    ASSERT_NE(nullableType, nullptr);
    EXPECT_TRUE(nullableType->isNullable());
    EXPECT_EQ(nullableType->getTypeId(), omniruntime::type::DataTypeId::OMNI_CHAR);
    EXPECT_EQ(nullableType->toJson().at("length"), 12);

    std::unique_ptr<LogicalType> notNullType(LogicalType::flinkTypeToOmniType("CHAR(8) NOT NULL"));
    ASSERT_NE(notNullType, nullptr);
    EXPECT_FALSE(notNullType->isNullable());
    EXPECT_EQ(notNullType->getTypeId(), omniruntime::type::DataTypeId::OMNI_CHAR);
    EXPECT_EQ(notNullType->toJson().at("length"), 8);
}

TEST(LogicalTypeTest, RawTypeAllowsEmptySerializerMetadata)
{
    std::unique_ptr<LogicalType> type(LogicalType::flinkTypeToOmniType("RAW('example.Type', '')"));
    auto* rawType = dynamic_cast<omnistream::RawType*>(type.get());
    ASSERT_NE(rawType, nullptr);
    EXPECT_EQ(rawType->getClassName(), "example.Type");
    EXPECT_TRUE(rawType->getSerializerString().empty());
}
