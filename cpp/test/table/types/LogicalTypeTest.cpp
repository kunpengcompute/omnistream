#include <gtest/gtest.h>
#include <memory>
#include "table/types/logical/LogicalType.h"
#include "table/types/logical/RawType.h"
#include <string.h>
#include "OmniOperatorJIT/core/src/type/data_type.h"

TEST(LogicalTypeTest, PlannerTimestampLtzType)
{
    EXPECT_EQ(
        LogicalType::flinkTypeToOmniTypeId("TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL"),
        omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE);
}

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

TEST(LogicalTypeTest, RawTypeAllowsEmptySerializerMetadata)
{
    std::unique_ptr<LogicalType> type(LogicalType::flinkTypeToOmniType("RAW('example.Type', '')"));
    auto* rawType = dynamic_cast<omnistream::RawType*>(type.get());
    ASSERT_NE(rawType, nullptr);
    EXPECT_EQ(rawType->getClassName(), "example.Type");
    EXPECT_TRUE(rawType->getSerializerString().empty());
}

TEST(LogicalTypeTest, FlinkTypeToOmniTypePreservesNullability)
{
    auto logicalTypeDeleter = [](LogicalType* type) {
        if (!LogicalType::isSharedLogicalType(type)) {
            delete type;
        }
    };

    using LogicalTypePtr = std::unique_ptr<LogicalType, decltype(logicalTypeDeleter)>;
    LogicalTypePtr nullableTimestamp(
        LogicalType::flinkTypeToOmniType("TIMESTAMP_WITHOUT_TIME_ZONE(3)"), logicalTypeDeleter);
    LogicalTypePtr notNullTimestamp(
        LogicalType::flinkTypeToOmniType("TIMESTAMP_WITHOUT_TIME_ZONE(3) NOT NULL"), logicalTypeDeleter);
    LogicalTypePtr notNullBigint(LogicalType::flinkTypeToOmniType("BIGINT NOT NULL *PROCTIME*"), logicalTypeDeleter);
    LogicalTypePtr notNullChar(LogicalType::flinkTypeToOmniType("CHAR(12) NOT NULL"), logicalTypeDeleter);

    ASSERT_NE(nullableTimestamp, nullptr);
    ASSERT_NE(notNullTimestamp, nullptr);
    ASSERT_NE(notNullBigint, nullptr);
    ASSERT_NE(notNullChar, nullptr);
    EXPECT_TRUE(nullableTimestamp->isNullable());
    EXPECT_FALSE(notNullTimestamp->isNullable());
    EXPECT_FALSE(notNullBigint->isNullable());
    EXPECT_FALSE(notNullChar->isNullable());
    EXPECT_EQ(notNullChar->getTypeId(), omniruntime::type::DataTypeId::OMNI_CHAR);
    EXPECT_EQ(notNullChar->toJson().at("length"), 12);
    EXPECT_EQ(notNullTimestamp->toJson().at("precision"), 3);
    EXPECT_FALSE(notNullTimestamp->toJson().at("nullable"));
}
