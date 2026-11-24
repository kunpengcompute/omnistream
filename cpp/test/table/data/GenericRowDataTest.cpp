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

#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "table/typeutils/RawValueDataSerializer.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"
#include "table/types/logical/RawType.h"
#include "table/data/binary/BinaryRowData.h"
#include <vector>
#include <memory>
#include "table/data/GenericRowData.h"
#include <gtest/gtest.h>

TEST(GenericRowDataTest, ConstructorTest_1)
{
    GenericRowData genericRowData(3);
    EXPECT_EQ(genericRowData.getRowKind(), RowKind::INSERT);
    EXPECT_EQ(genericRowData.getArity(), 3);
    EXPECT_EQ(genericRowData.getTypeIDs().size(), 3);
}

TEST(GenericRowDataTest, ConstructorTest_2)
{
    std::vector<int> typeIDs({0, 1, 2});
    GenericRowData genericRowData(typeIDs, RowKind::INSERT);
    EXPECT_EQ(genericRowData.getRowKind(), RowKind::INSERT);
    EXPECT_EQ(genericRowData.getArity(), 3);
    EXPECT_EQ(genericRowData.getTypeIDs().size(), 3);
    EXPECT_EQ(genericRowData.getTypeIDs(), typeIDs);
}

TEST(GenericRowDataTest, ConstructorTest_3)
{
    std::vector<int> typeIDs({0, 1, 2});
    GenericRowData genericRowData(typeIDs);
    EXPECT_EQ(genericRowData.getRowKind(), RowKind::INSERT);
    EXPECT_EQ(genericRowData.getArity(), 3);
    EXPECT_EQ(genericRowData.getTypeIDs().size(), 3);
    EXPECT_EQ(genericRowData.getTypeIDs(), typeIDs);
}

TEST(GenericRowDataTest, SetFieldTest)
{
    std::vector<int> typeIDs({0, 0});
    GenericRowData genericRowData(typeIDs);
    genericRowData.setField(0, 1);
    genericRowData.setField(1, 2);

    EXPECT_EQ(genericRowData.getField(0), 1);
    EXPECT_EQ(genericRowData.getField(1), 2);
}

TEST(GenericRowDataTest, TimeStampTest)
{
    std::vector<int> typeIDs({12});
    GenericRowData genericRowData(typeIDs);
    TimestampData timeStampData(1000, 1);
    genericRowData.setField(0, &timeStampData);

    EXPECT_EQ(genericRowData.getTimestamp(0).getMillisecond(), 1000);
}

TEST(GenericRowDataTest, RawTypeFieldGetterTest)
{
    GenericRowData row(1);
    const uint8_t bytes[] = {0x01, 0x00, 0x02};
    BinaryRawValueData rawValue(bytes, sizeof(bytes));
    row.setField(0, &rawValue);
    omnistream::RawType rawType(true, "test.RawValue", "serializer-snapshot");
    std::unique_ptr<FieldGetter> getter(RowData::createFieldGetter(&rawType, 0));

    EXPECT_EQ(getter->getFieldOrNull(&row), &rawValue);
}

TEST(GenericRowDataTest, RawValueRowDataSerializerRoundTrip)
{
    omnistream::RawType rawType(true, "test.RawValue", "serializer-snapshot");
    std::vector<omnistream::RowField> fields;
    fields.emplace_back("raw", &rawType);
    omnistream::RowType rowType(true, fields);
    RowDataSerializer serializer(&rowType);

    GenericRowData row(1);
    const uint8_t bytes[] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
    BinaryRawValueData rawValue(bytes, sizeof(bytes));
    row.setField(0, &rawValue);

    DataOutputSerializer output(64);
    serializer.serialize(&row, output);
    DataInputDeserializer input(output.getData(), output.getPosition(), 0);
    auto* decoded = static_cast<BinaryRowData*>(serializer.deserialize(input));
    auto* decodedRaw = static_cast<BinaryRawValueData*>(decoded->getRawValue(0));
    ASSERT_NE(decodedRaw, nullptr);
    EXPECT_EQ(decodedRaw->toBytes(), std::vector<uint8_t>(bytes, bytes + sizeof(bytes)));
}

TEST(GenericRowDataTest, RawValueDataSerializerMetadata)
{
    RawValueDataSerializer serializer("test.RawValue", "serializer-snapshot");

    EXPECT_STREQ(serializer.getName(), "RawValueDataSerializer");
    EXPECT_TRUE(serializer.toJson().empty());
}

TEST(GenericRowDataTest, RawValueDataSerializerRejectsDirectNonNullAccess)
{
    RawValueDataSerializer serializer("test.RawValue", "serializer-snapshot");
    const uint8_t bytes[] = {0x01};
    BinaryRawValueData rawValue(bytes, sizeof(bytes));
    DataOutputSerializer output(16);
    EXPECT_THROW(serializer.serialize(&rawValue, output), std::runtime_error);

    uint8_t inputByte = 0;
    DataInputDeserializer input(&inputByte, 1, 0);
    EXPECT_THROW(serializer.deserialize(input), std::runtime_error);
}
