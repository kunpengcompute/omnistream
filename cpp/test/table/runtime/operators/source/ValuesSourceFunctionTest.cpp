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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "basictypes/Object.h"
#include "runtime/executiongraph/TaskInformationPOD.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/source/ValuesSourceFunction.h"
#include "types/logical/LogicalType.h"

using namespace omnistream;
using omniruntime::type::DataTypeId;
using omniruntime::type::Decimal128;
using omniruntime::type::Decimal64DataType;
using omniruntime::type::Decimal128DataType;

namespace {

using StringVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;

std::vector<DataTypeId> ToTypeIds(const std::vector<std::string>& types)
{
    std::vector<DataTypeId> ids;
    ids.reserve(types.size());
    for (const auto& type : types) {
        ids.push_back(LogicalType::flinkTypeToOmniTypeId(type));
    }
    return ids;
}

std::vector<int32_t> ToPrecisions(const std::vector<std::string>& types)
{
    std::vector<int32_t> precisions;
    precisions.reserve(types.size());
    for (const auto& type : types) {
        precisions.push_back(LogicalType::parseDecimalPrecisionScale(type).first);
    }
    return precisions;
}

std::vector<int32_t> ToScales(const std::vector<std::string>& types)
{
    std::vector<int32_t> scales;
    scales.reserve(types.size());
    for (const auto& type : types) {
        scales.push_back(LogicalType::parseDecimalPrecisionScale(type).second);
    }
    return scales;
}

class CollectingSourceContext : public SourceContext {
public:
    void collect(void* element) override
    {
        collected.push_back(static_cast<omnistream::VectorBatch*>(element));
    }

    void collectWithTimestamp(void* element, int64_t /*timestamp*/) override
    {
        collect(element);
    }

    void emitWatermark(Watermark* /*mark*/) override
    {
    }

    void markAsTemporarilyIdle() override
    {
    }

    Object* getCheckpointLock() override
    {
        return &lock;
    }

    void close() override
    {
    }

    std::vector<omnistream::VectorBatch*> collected;
    Object lock;
};

void SetRuntimeContext(ValuesSourceFunction& source, int indexOfSubtask)
{
    auto* runtimeEnv = new omnistream::RuntimeEnvironmentV2();
    if (indexOfSubtask != 0) {
        omnistream::TaskInformationPOD taskInfo;
        taskInfo.setIndexOfSubtask(indexOfSubtask);
        runtimeEnv->setTaskConfiguration(taskInfo);
    }
    auto* runtimeCtx = new StreamingRuntimeContext<int>();
    runtimeCtx->setEnvironment(runtimeEnv);
    source.setRuntimeContext(runtimeCtx);
}

} // namespace

TEST(ValuesSourceFunctionTest, ConvertBasicTypesPreservesRowsAndNulls)
{
    nlohmann::json tuples = nlohmann::json::array();
    tuples.push_back(nlohmann::json::array({"1", "Bob"}));
    tuples.push_back(nlohmann::json::array({"2", "Alice"}));
    tuples.push_back(nlohmann::json::array({"3", nullptr}));

    auto typeIds = ToTypeIds({"INT", "VARCHAR"});
    auto precisions = ToPrecisions({"INT", "VARCHAR"});
    auto scales = ToScales({"INT", "VARCHAR"});

    auto* batch = ValuesSourceFunction::convert(tuples, typeIds, precisions, scales);
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->GetRowCount(), 3);
    EXPECT_EQ(batch->GetVectorCount(), 2);

    auto* intVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(0));
    auto* strVec = reinterpret_cast<StringVector*>(batch->Get(1));

    EXPECT_FALSE(intVec->IsNull(0));
    EXPECT_EQ(intVec->GetValue(0), 1);
    EXPECT_EQ(intVec->GetValue(1), 2);
    EXPECT_EQ(intVec->GetValue(2), 3);

    EXPECT_EQ(std::string(strVec->GetValue(0)), "Bob");
    EXPECT_EQ(std::string(strVec->GetValue(1)), "Alice");
    EXPECT_TRUE(strVec->IsNull(2));

    delete batch;
}

TEST(ValuesSourceFunctionTest, ConvertAllNativeTypesPreservesTypePrecision)
{
    nlohmann::json tuples = nlohmann::json::array();
    tuples.push_back(nlohmann::json::array({"42", "7", "3.14", "true", "1970-01-02", "12:30:00",
                                            "1970-01-01 00:00:01.500", "1970-01-01 00:00:00Z",
                                            "hello", "x", "12.34", "123456789012345.12345"}));

    const std::vector<std::string> types = {"BIGINT", "INT", "DOUBLE", "BOOLEAN", "DATE", "TIME(3)",
                                            "TIMESTAMP(3)", "TIMESTAMP(3) WITH LOCAL TIME ZONE",
                                            "VARCHAR(10)", "CHAR(4)", "DECIMAL(10,2)", "DECIMAL(20,5)"};
    auto typeIds = ToTypeIds(types);
    auto precisions = ToPrecisions(types);
    auto scales = ToScales(types);

    auto* batch = ValuesSourceFunction::convert(tuples, typeIds, precisions, scales);
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(batch->GetVectorCount(), 12);

    auto* bigintVec = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(0));
    auto* intVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(1));
    auto* doubleVec = reinterpret_cast<omniruntime::vec::Vector<double>*>(batch->Get(2));
    auto* boolVec = reinterpret_cast<omniruntime::vec::Vector<bool>*>(batch->Get(3));
    auto* dateVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(4));
    auto* timeVec = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(5));
    auto* timestampVec = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(6));
    auto* timestampLtzVec = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(7));
    auto* varcharVec = reinterpret_cast<StringVector*>(batch->Get(8));
    auto* charVec = reinterpret_cast<StringVector*>(batch->Get(9));
    auto* decimal64Vec = reinterpret_cast<omniruntime::vec::Vector<long>*>(batch->Get(10));
    auto* decimal128Vec = reinterpret_cast<omniruntime::vec::Vector<Decimal128>*>(batch->Get(11));

    EXPECT_EQ(bigintVec->GetValue(0), 42L);
    EXPECT_EQ(intVec->GetValue(0), 7);
    EXPECT_DOUBLE_EQ(doubleVec->GetValue(0), 3.14);
    EXPECT_EQ(boolVec->GetValue(0), true);
    EXPECT_EQ(dateVec->GetValue(0), 1);
    EXPECT_EQ(timeVec->GetValue(0), 45000000L);
    EXPECT_EQ(timestampVec->GetValue(0), 1500L);
    EXPECT_EQ(timestampLtzVec->GetValue(0), 0L);
    EXPECT_EQ(std::string(varcharVec->GetValue(0)), "hello");
    EXPECT_EQ(std::string(charVec->GetValue(0)), "x");
    EXPECT_EQ(decimal64Vec->GetValue(0), 1234L);
    EXPECT_EQ(decimal128Vec->GetValue(0), Decimal128("12345678901234512345"));

    auto decimal64Type = std::dynamic_pointer_cast<Decimal64DataType>(decimal64Vec->GetDataType());
    ASSERT_NE(decimal64Type, nullptr);
    EXPECT_EQ(decimal64Type->GetPrecision(), 10);
    EXPECT_EQ(decimal64Type->GetScale(), 2);

    auto decimal128Type = std::dynamic_pointer_cast<Decimal128DataType>(decimal128Vec->GetDataType());
    ASSERT_NE(decimal128Type, nullptr);
    EXPECT_EQ(decimal128Type->GetPrecision(), 20);
    EXPECT_EQ(decimal128Type->GetScale(), 5);

    delete batch;
}

TEST(ValuesSourceFunctionTest, ConvertBooleanAndNumericJsonCells)
{
    nlohmann::json tuples = nlohmann::json::array();
    tuples.push_back(nlohmann::json::array({true, 5, 2.5}));

    auto typeIds = ToTypeIds({"BOOLEAN", "INT", "DOUBLE"});
    auto precisions = ToPrecisions({"BOOLEAN", "INT", "DOUBLE"});
    auto scales = ToScales({"BOOLEAN", "INT", "DOUBLE"});

    auto* batch = ValuesSourceFunction::convert(tuples, typeIds, precisions, scales);
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->GetRowCount(), 1);

    auto* boolVec = reinterpret_cast<omniruntime::vec::Vector<bool>*>(batch->Get(0));
    auto* intVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(1));
    auto* doubleVec = reinterpret_cast<omniruntime::vec::Vector<double>*>(batch->Get(2));

    EXPECT_FALSE(boolVec->IsNull(0));
    EXPECT_EQ(boolVec->GetValue(0), true);
    EXPECT_EQ(intVec->GetValue(0), 5);
    EXPECT_DOUBLE_EQ(doubleVec->GetValue(0), 2.5);

    delete batch;
}

TEST(ValuesSourceFunctionTest, ConvertEmptyTuplesProducesEmptyBatch)
{
    nlohmann::json tuples = nlohmann::json::array();
    auto typeIds = ToTypeIds({"INT"});
    auto precisions = ToPrecisions({"INT"});
    auto scales = ToScales({"INT"});

    auto* batch = ValuesSourceFunction::convert(tuples, typeIds, precisions, scales);
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->GetRowCount(), 0);
    EXPECT_EQ(batch->GetVectorCount(), 0);

    delete batch;
}

TEST(ValuesSourceFunctionTest, ConvertDecimalPrecisionBoundary)
{
    const std::vector<std::string> types = {"DECIMAL(10,2)"};
    auto typeIds = ToTypeIds(types);
    auto precisions = ToPrecisions(types);
    auto scales = ToScales(types);

    nlohmann::json positive = nlohmann::json::array();
    positive.push_back(nlohmann::json::array({"12.34"}));
    auto* positiveBatch = ValuesSourceFunction::convert(positive, typeIds, precisions, scales);
    ASSERT_NE(positiveBatch, nullptr);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(positiveBatch->Get(0))->GetValue(0), 1234L);
    delete positiveBatch;

    nlohmann::json negative = nlohmann::json::array();
    negative.push_back(nlohmann::json::array({"-12.34"}));
    auto* negativeBatch = ValuesSourceFunction::convert(negative, typeIds, precisions, scales);
    ASSERT_NE(negativeBatch, nullptr);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(negativeBatch->Get(0))->GetValue(0), -1234L);
    delete negativeBatch;

    nlohmann::json exact = nlohmann::json::array();
    exact.push_back(nlohmann::json::array({"12345678.12"}));
    auto* exactBatch = ValuesSourceFunction::convert(exact, typeIds, precisions, scales);
    ASSERT_NE(exactBatch, nullptr);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(exactBatch->Get(0))->GetValue(0), 1234567812L);
    delete exactBatch;

    nlohmann::json overflow = nlohmann::json::array();
    overflow.push_back(nlohmann::json::array({"123456789.12"}));
    auto* overflowBatch = ValuesSourceFunction::convert(overflow, typeIds, precisions, scales);
    ASSERT_NE(overflowBatch, nullptr);
    // Aligns with Flink DecimalData.fromBigDecimal: a precision overflow produces a
    // null value instead of raising an exception.
    EXPECT_TRUE(reinterpret_cast<omniruntime::vec::Vector<long>*>(overflowBatch->Get(0))->IsNull(0));
    delete overflowBatch;
}

TEST(ValuesSourceFunctionTest, ConvertDecimalHalfUpRounding)
{
    const std::vector<std::string> types = {"DECIMAL(10,2)"};
    auto typeIds = ToTypeIds(types);
    auto precisions = ToPrecisions(types);
    auto scales = ToScales(types);

    nlohmann::json roundUp = nlohmann::json::array();
    roundUp.push_back(nlohmann::json::array({"12.345"}));
    auto* roundUpBatch = ValuesSourceFunction::convert(roundUp, typeIds, precisions, scales);
    ASSERT_NE(roundUpBatch, nullptr);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(roundUpBatch->Get(0))->GetValue(0), 1235L);
    delete roundUpBatch;

    nlohmann::json roundDown = nlohmann::json::array();
    roundDown.push_back(nlohmann::json::array({"12.344"}));
    auto* roundDownBatch = ValuesSourceFunction::convert(roundDown, typeIds, precisions, scales);
    ASSERT_NE(roundDownBatch, nullptr);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(roundDownBatch->Get(0))->GetValue(0), 1234L);
    delete roundDownBatch;

    nlohmann::json negative = nlohmann::json::array();
    negative.push_back(nlohmann::json::array({"-12.345"}));
    auto* negativeBatch = ValuesSourceFunction::convert(negative, typeIds, precisions, scales);
    ASSERT_NE(negativeBatch, nullptr);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(negativeBatch->Get(0))->GetValue(0), -1235L);
    delete negativeBatch;

    nlohmann::json carry = nlohmann::json::array();
    carry.push_back(nlohmann::json::array({"99.995"}));
    auto* carryBatch = ValuesSourceFunction::convert(carry, typeIds, precisions, scales);
    ASSERT_NE(carryBatch, nullptr);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(carryBatch->Get(0))->GetValue(0), 10000L);
    delete carryBatch;
}

TEST(ValuesSourceFunctionTest, ConvertDecimal128HalfUpRounding)
{
    const std::vector<std::string> types = {"DECIMAL(20,5)"};
    auto typeIds = ToTypeIds(types);
    auto precisions = ToPrecisions(types);
    auto scales = ToScales(types);

    nlohmann::json roundUp = nlohmann::json::array();
    roundUp.push_back(nlohmann::json::array({"123456789012345.123456"}));
    auto* batch = ValuesSourceFunction::convert(roundUp, typeIds, precisions, scales);
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<Decimal128>*>(batch->Get(0))->GetValue(0),
              Decimal128("12345678901234512346"));
    delete batch;
}

TEST(ValuesSourceFunctionTest, ConvertDecimalScientificNotation)
{
    nlohmann::json tuples = nlohmann::json::array();
    tuples.push_back(nlohmann::json::array({"1.0E-7", "1E+2", "1.23E+3", "1.235E+1", "-1.0E-7", "+1E+2"}));

    const std::vector<std::string> types = {"DECIMAL(10,7)", "DECIMAL(10,2)", "DECIMAL(10,2)", "DECIMAL(10,1)",
                                            "DECIMAL(10,7)", "DECIMAL(10,2)"};
    auto typeIds = ToTypeIds(types);
    auto precisions = ToPrecisions(types);
    auto scales = ToScales(types);

    auto* batch = ValuesSourceFunction::convert(tuples, typeIds, precisions, scales);
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->GetRowCount(), 1);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(batch->Get(0))->GetValue(0), 1L);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(batch->Get(1))->GetValue(0), 10000L);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(batch->Get(2))->GetValue(0), 123000L);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(batch->Get(3))->GetValue(0), 124L);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(batch->Get(4))->GetValue(0), -1L);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(batch->Get(5))->GetValue(0), 10000L);
    delete batch;
}

TEST(ValuesSourceFunctionTest, ConvertDecimal128ScientificNotation)
{
    nlohmann::json tuples = nlohmann::json::array();
    tuples.push_back(nlohmann::json::array({"1.0E-7", "1.235E+1"}));

    const std::vector<std::string> types = {"DECIMAL(20,7)", "DECIMAL(20,1)"};
    auto typeIds = ToTypeIds(types);
    auto precisions = ToPrecisions(types);
    auto scales = ToScales(types);

    auto* batch = ValuesSourceFunction::convert(tuples, typeIds, precisions, scales);
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<Decimal128>*>(batch->Get(0))->GetValue(0),
              Decimal128("1"));
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<Decimal128>*>(batch->Get(1))->GetValue(0),
              Decimal128("124"));
    delete batch;
}

TEST(ValuesSourceFunctionTest, ConvertScientificNotationJsonNumberCell)
{
    nlohmann::json tuples = nlohmann::json::array();
    tuples.push_back(nlohmann::json::array({0.0000001}));

    auto typeIds = ToTypeIds({"DECIMAL(10,7)"});
    auto precisions = ToPrecisions({"DECIMAL(10,7)"});
    auto scales = ToScales({"DECIMAL(10,7)"});

    auto* batch = ValuesSourceFunction::convert(tuples, typeIds, precisions, scales);
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<long>*>(batch->Get(0))->GetValue(0), 1L);
    delete batch;
}

TEST(ValuesSourceFunctionTest, ConvertMalformedDecimalLiteralsThrow)
{
    const std::vector<std::string> types = {"DECIMAL(10,2)"};
    auto typeIds = ToTypeIds(types);
    auto precisions = ToPrecisions(types);
    auto scales = ToScales(types);

    const std::vector<std::string> malformed = {"1.2.3", "abc", "1.0E", "1e+"};
    for (const auto& literal : malformed) {
        nlohmann::json tuples = nlohmann::json::array();
        tuples.push_back(nlohmann::json::array({literal}));
        EXPECT_THROW(ValuesSourceFunction::convert(tuples, typeIds, precisions, scales), std::invalid_argument)
            << "malformed literal: " << literal;
    }
}

TEST(ValuesSourceFunctionTest, ConvertDecimalRoundingOverflowBecomesNull)
{
    const std::vector<std::string> types = {"DECIMAL(10,2)"};
    auto typeIds = ToTypeIds(types);
    auto precisions = ToPrecisions(types);
    auto scales = ToScales(types);

    nlohmann::json overflow = nlohmann::json::array();
    overflow.push_back(nlohmann::json::array({"99999999.995"}));
    auto* batch = ValuesSourceFunction::convert(overflow, typeIds, precisions, scales);
    ASSERT_NE(batch, nullptr);
    EXPECT_TRUE(reinterpret_cast<omniruntime::vec::Vector<long>*>(batch->Get(0))->IsNull(0));
    delete batch;
}

TEST(ValuesSourceFunctionTest, ConstructorRejectsUnsupportedMappableType)
{
    const std::vector<std::string> unsupportedTypes = {
        "SMALLINT", "TINYINT", "FLOAT", "BYTES", "BINARY(10)", "VARBINARY(10)",
        "ARRAY<INT>", "MAP<STRING, BIGINT>", "MULTISET<STRING>", "ROW<myField INT>",
        "RAW('class', 'snapshot')", "SYMBOL('class')", "INTERVAL DAY(6)", "INTERVAL YEAR(4)",
        "TIMESTAMP(9) WITH TIME ZONE"};
    for (const auto& type : unsupportedTypes) {
        nlohmann::json config;
        config["format"] = "values";
        config["tuples"] = nlohmann::json::array({nlohmann::json::array({"x"})});
        config["outputTypes"] = nlohmann::json::array({type});
        config["outputNames"] = nlohmann::json::array({"a"});
        config["batch"] = true;
        EXPECT_THROW(ValuesSourceFunction{config}, std::runtime_error) << "unsupported type: " << type;
    }
}

TEST(ValuesSourceFunctionTest, ConvertInvalidLiteralsThrow)
{
    nlohmann::json badDate = nlohmann::json::array();
    badDate.push_back(nlohmann::json::array({"not-a-date"}));
    auto dateTypeIds = ToTypeIds({"DATE"});
    auto datePrecisions = ToPrecisions({"DATE"});
    auto dateScales = ToScales({"DATE"});
    EXPECT_THROW(ValuesSourceFunction::convert(badDate, dateTypeIds, datePrecisions, dateScales),
                 std::invalid_argument);

    nlohmann::json badTime = nlohmann::json::array();
    badTime.push_back(nlohmann::json::array({"ab:cd:ef"}));
    auto timeTypeIds = ToTypeIds({"TIME(3)"});
    auto timePrecisions = ToPrecisions({"TIME(3)"});
    auto timeScales = ToScales({"TIME(3)"});
    EXPECT_THROW(ValuesSourceFunction::convert(badTime, timeTypeIds, timePrecisions, timeScales),
                 std::runtime_error);

    nlohmann::json badTimestamp = nlohmann::json::array();
    badTimestamp.push_back(nlohmann::json::array({"not a timestamp"}));
    auto timestampTypeIds = ToTypeIds({"TIMESTAMP(3)"});
    auto timestampPrecisions = ToPrecisions({"TIMESTAMP(3)"});
    auto timestampScales = ToScales({"TIMESTAMP(3)"});
    EXPECT_THROW(ValuesSourceFunction::convert(badTimestamp, timestampTypeIds, timestampPrecisions,
                                               timestampScales),
                 std::runtime_error);

    nlohmann::json ltzWithoutZ = nlohmann::json::array();
    ltzWithoutZ.push_back(nlohmann::json::array({"1970-01-01 00:00:00"}));
    auto ltzTypeIds = ToTypeIds({"TIMESTAMP(3) WITH LOCAL TIME ZONE"});
    auto ltzPrecisions = ToPrecisions({"TIMESTAMP(3) WITH LOCAL TIME ZONE"});
    auto ltzScales = ToScales({"TIMESTAMP(3) WITH LOCAL TIME ZONE"});
    EXPECT_THROW(ValuesSourceFunction::convert(ltzWithoutZ, ltzTypeIds, ltzPrecisions, ltzScales),
                 std::invalid_argument);
}

TEST(ValuesSourceFunctionTest, ConvertTupleRowWidthMismatchThrows)
{
    auto typeIds = ToTypeIds({"INT", "VARCHAR"});
    auto precisions = ToPrecisions({"INT", "VARCHAR"});
    auto scales = ToScales({"INT", "VARCHAR"});

    nlohmann::json tuples = nlohmann::json::array();
    tuples.push_back(nlohmann::json::array({"1"}));
    EXPECT_THROW(ValuesSourceFunction::convert(tuples, typeIds, precisions, scales), std::runtime_error);
}

TEST(ValuesSourceFunctionTest, ConvertUnsupportedNativeTypeThrows)
{
    const std::vector<DataTypeId> unsupportedIds = {DataTypeId::OMNI_ARRAY, DataTypeId::OMNI_MAP,
                                                    DataTypeId::OMNI_CONTAINER, DataTypeId::OMNI_INTERVAL_DAY_TIME,
                                                    DataTypeId::OMNI_INTERVAL_MONTHS, DataTypeId::OMNI_FLOAT};
    nlohmann::json tuples = nlohmann::json::array();
    tuples.push_back(nlohmann::json::array({"x"}));
    for (DataTypeId typeId : unsupportedIds) {
        EXPECT_THROW(ValuesSourceFunction::convert(tuples, {typeId}, {18}, {0}), std::runtime_error)
            << "unsupported native typeId=" << static_cast<int>(typeId);
    }
}

TEST(ValuesSourceFunctionTest, ConstructorRejectsMissingOutputTypes)
{
    nlohmann::json config;
    config["format"] = "values";
    config["tuples"] = nlohmann::json::array();
    EXPECT_THROW(ValuesSourceFunction{config}, std::invalid_argument);
}

TEST(ValuesSourceFunctionTest, ConstructorRejectsUnknownOutputType)
{
    nlohmann::json config;
    config["format"] = "values";
    config["tuples"] = nlohmann::json::array();
    config["outputTypes"] = nlohmann::json::array({"NOTYPE(1)"});
    EXPECT_THROW(ValuesSourceFunction{config}, std::runtime_error);
}

TEST(ValuesSourceFunctionTest, RunEmitsSingleBatchAndThenEnds)
{
    nlohmann::json config;
    config["format"] = "values";
    config["tuples"] = nlohmann::json::array({nlohmann::json::array({"1", "Bob"}),
                                              nlohmann::json::array({"2", "Alice"})});
    config["outputTypes"] = nlohmann::json::array({"INT", "VARCHAR"});
    config["outputNames"] = nlohmann::json::array({"a", "b"});
    config["batch"] = true;

    ValuesSourceFunction source(config);
    SetRuntimeContext(source, 0);

    CollectingSourceContext ctx;
    source.run(&ctx);
    ASSERT_EQ(ctx.collected.size(), 1u);

    auto* batch = ctx.collected[0];
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->GetRowCount(), 2);
    auto* intVec = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(0));
    auto* strVec = reinterpret_cast<StringVector*>(batch->Get(1));
    EXPECT_EQ(intVec->GetValue(0), 1);
    EXPECT_EQ(std::string(strVec->GetValue(0)), "Bob");
    EXPECT_EQ(intVec->GetValue(1), 2);
    EXPECT_EQ(std::string(strVec->GetValue(1)), "Alice");
    delete batch;
    ctx.collected.clear();

    source.run(&ctx);
    EXPECT_EQ(ctx.collected.size(), 0u);

    source.close();
}

TEST(ValuesSourceFunctionTest, RunPreservesDecimalPrecisionAndScale)
{
    nlohmann::json config;
    config["format"] = "values";
    config["tuples"] = nlohmann::json::array({nlohmann::json::array({"12.34", "123456789012345.12345"})});
    config["outputTypes"] = nlohmann::json::array({"DECIMAL(10,2)", "DECIMAL(20,5)"});
    config["outputNames"] = nlohmann::json::array({"d64", "d128"});
    config["batch"] = true;

    ValuesSourceFunction source(config);
    SetRuntimeContext(source, 0);

    CollectingSourceContext ctx;
    source.run(&ctx);
    ASSERT_EQ(ctx.collected.size(), 1u);

    auto* batch = ctx.collected[0];
    ASSERT_NE(batch, nullptr);
    auto* decimal64Vec = reinterpret_cast<omniruntime::vec::Vector<long>*>(batch->Get(0));
    auto* decimal128Vec = reinterpret_cast<omniruntime::vec::Vector<Decimal128>*>(batch->Get(1));
    EXPECT_EQ(decimal64Vec->GetValue(0), 1234L);
    EXPECT_EQ(decimal128Vec->GetValue(0), Decimal128("12345678901234512345"));

    auto decimal64Type = std::dynamic_pointer_cast<Decimal64DataType>(decimal64Vec->GetDataType());
    ASSERT_NE(decimal64Type, nullptr);
    EXPECT_EQ(decimal64Type->GetPrecision(), 10);
    EXPECT_EQ(decimal64Type->GetScale(), 2);

    auto decimal128Type = std::dynamic_pointer_cast<Decimal128DataType>(decimal128Vec->GetDataType());
    ASSERT_NE(decimal128Type, nullptr);
    EXPECT_EQ(decimal128Type->GetPrecision(), 20);
    EXPECT_EQ(decimal128Type->GetScale(), 5);

    delete batch;
    source.close();
}

TEST(ValuesSourceFunctionTest, RunEmptyTuplesCollectsNothing)
{
    nlohmann::json config;
    config["format"] = "values";
    config["tuples"] = nlohmann::json::array();
    config["outputTypes"] = nlohmann::json::array({"INT"});
    config["outputNames"] = nlohmann::json::array({"a"});
    config["batch"] = true;

    ValuesSourceFunction source(config);
    SetRuntimeContext(source, 0);

    CollectingSourceContext ctx;
    source.run(&ctx);
    EXPECT_EQ(ctx.collected.size(), 0u);

    source.close();
}

TEST(ValuesSourceFunctionTest, RunSkipsNonZeroSubtask)
{
    nlohmann::json config;
    config["format"] = "values";
    config["tuples"] = nlohmann::json::array({nlohmann::json::array({"1", "Bob"})});
    config["outputTypes"] = nlohmann::json::array({"INT", "VARCHAR"});
    config["outputNames"] = nlohmann::json::array({"a", "b"});
    config["batch"] = true;

    ValuesSourceFunction source(config);
    SetRuntimeContext(source, 1);

    CollectingSourceContext ctx;
    source.run(&ctx);
    EXPECT_EQ(ctx.collected.size(), 0u);

    source.close();
}

TEST(ValuesSourceFunctionTest, CancelPreventsEmission)
{
    nlohmann::json config;
    config["format"] = "values";
    config["tuples"] = nlohmann::json::array({nlohmann::json::array({"1", "Bob"})});
    config["outputTypes"] = nlohmann::json::array({"INT", "VARCHAR"});
    config["outputNames"] = nlohmann::json::array({"a", "b"});
    config["batch"] = true;

    ValuesSourceFunction source(config);
    SetRuntimeContext(source, 0);
    source.cancel();

    CollectingSourceContext ctx;
    source.run(&ctx);
    EXPECT_EQ(ctx.collected.size(), 0u);

    source.close();
}
