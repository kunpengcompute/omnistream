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

#include "streaming/api/operators/StreamCalcBatch.h"
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include "OutputTest.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"

using json = nlohmann::json;

omnistream::VectorBatch* BuildInputVectorBatch()
{
    int rowCnt = 10;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        col0[i] = 0;
        col1[i] = i;
        col2[i] = 2 * i;
    }

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    std::cout << "input vectorbatch created" << std::endl;
    return vb;
}

omnistream::VectorBatch* BuildStringVectorBatch(std::string* strings, int rowCount)
{
    auto vector = omniruntime::TestUtil::CreateVarcharVector(strings, rowCount);

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    vb->Append(vector);

    return vb;
}

omnistream::VectorBatch* ProcessAndGetOutput(std::string desc, omnistream::VectorBatch* inputRecord)
{
    json parsedJson = json::parse(desc);

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    StreamCalcBatch streamCalcBatchOp(parsedJson, output);
    streamCalcBatchOp.open();

    StreamRecord* record = new StreamRecord(inputRecord);
    streamCalcBatchOp.processBatch(record);
    auto out = output->getAll()[0];
    return reinterpret_cast<omnistream::VectorBatch*>(out);
}

TEST(StreamCalcBatchTest, VectorbatchSimpleProjection)
{
    // OMNI_LONG = 2, input [long, long, long], select col0, col2
    std::string desc =
        R"DELIM({"originDescription":"[21]:Calc(select=[category,price, final])","inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"indices":[{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":2}],"condition" :null})DELIM";
    auto inputRecord = BuildInputVectorBatch();
    auto outputRecord = ProcessAndGetOutput(desc, inputRecord);
    // Check size
    int rowCnt = inputRecord->GetRowCount();
    EXPECT_EQ(rowCnt, outputRecord->GetRowCount());
    // Check value
    auto outcol0 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord->Get(0));
    auto outcol1 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord->Get(1));

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(outcol0->GetValue(i), 0);
        EXPECT_EQ(outcol1->GetValue(i), 2 * i);
    }
}

TEST(StreamCalcBatchTest, VectorbatchSimpleFilter)
{
    // OMNI_LONG = 2, input [long, long, long], select col0, col2, where col1 == 2
    std::string desc =
        R"DELIM({"originDescription":"[21]:Calc(select=[category,price, final])","inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],)DELIM"
        R"DELIM("indices":[{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":2}],)DELIM"
        R"DELIM("condition" :{"exprType":"BINARY","returnType":4,"operator":"EQUAL",)DELIM"
        R"DELIM("left":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":1},)DELIM"
        R"DELIM("right":{"exprType":"LITERAL","dataType":2,"isNull":false,"value":2}}})DELIM";
    auto inputRecord = BuildInputVectorBatch();
    auto outputRecord = ProcessAndGetOutput(desc, inputRecord);
    // there will be only one row, [0,4]
    EXPECT_EQ(1, outputRecord->GetRowCount());
    auto outcol0 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord->Get(0));
    auto outcol1 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord->Get(1));
    EXPECT_EQ(outcol0->GetValue(0), 0);
    EXPECT_EQ(outcol1->GetValue(0), 4);
}

TEST(StreamCalcBatchTest, VectorbatchExpressionAdd)
{
    // OMNI_LONG = 2, input [long, long, long], select col1+col2
    std::string desc =
        R"DELIM({"originDescription":"[21]:Calc(select=[category,price, final])","inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":["BIGINT"],)DELIM"
        R"DELIM("indices":[{"exprType":"BINARY","returnType":2,"operator":"ADD",)DELIM"
        R"DELIM("left":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":1},)DELIM"
        R"DELIM("right":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":2}}],)DELIM"
        R"DELIM("condition" :null})DELIM";
    auto inputRecord = BuildInputVectorBatch();
    auto outputRecord = ProcessAndGetOutput(desc, inputRecord);
    // Check size
    EXPECT_EQ(10, outputRecord->GetRowCount());
    // Check value
    auto outcol0 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord->Get(0));
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(outcol0->GetValue(i), 3 * i);
    }
}

TEST(StreamCalcBatchTest, OpenThrowsForUnsupportedProjectionExpr)
{
    std::string desc =
        R"DELIM({"originDescription":"[21]:Calc(select=[unsupported])","inputTypes":["BIGINT"],"outputTypes":["BIGINT"],"indices":[{"exprType":"FUNCTION","returnType":2,"function_name":"not_supported_fn","arguments":[{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":0}]}],"condition":null})DELIM";
    json parsedJson = json::parse(desc);

    OutputTestVectorBatch output;
    StreamCalcBatch streamCalcBatchOp(parsedJson, &output);

    EXPECT_THROW(streamCalcBatchOp.open(), std::runtime_error);
}

TEST(StreamCalcBatchTest, DISABLED_VectorbatchExpressionCountChar)
{
    // OMNI_INT = 1, input [varchar, char], CountChar(col1, 'a')
    // Counts the number of 'a' in each row of the column
    std::string desc =
        R"DELIM({"originDescription":"[21]:Calc(select=[names])","inputTypes":["VARCHAR(2147483647)", "CHAR"],"outputTypes":["BIGINT"],)DELIM"
        R"DELIM("indices":[{"exprType":"FUNCTION","returnType":2,"function_name":"CountChar", "arguments":[{"exprType":"FIELD_REFERENCE","dataType":15,"colVal":0,"width":200}, {"exprType": "LITERAL" ,"dataType":16,"isNull":false,"value":"a", "width":200}]}]})DELIM";

    int rowCnt = 4;
    std::vector<std::string> inputStr = {"Hasan", "Aaaron", "Mark", "Lisa"};
    auto inputRecord = BuildStringVectorBatch(inputStr.data(), rowCnt);

    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    VarcharVector* incol0 = static_cast<VarcharVector*>(inputRecord->Get(0));

    std::cout << incol0->GetValue(0) << std::endl;
    std::cout << incol0->GetValue(1) << std::endl;
    std::cout << incol0->GetValue(2) << std::endl;
    std::cout << incol0->GetValue(3) << std::endl;

    auto outputRecord = ProcessAndGetOutput(desc, inputRecord);
    auto outcol0 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord->Get(0));

    std::cout << outcol0->GetValue(0) << std::endl;
    std::cout << outcol0->GetValue(1) << std::endl;
    std::cout << outcol0->GetValue(2) << std::endl;
    std::cout << outcol0->GetValue(3) << std::endl;

    EXPECT_EQ(outcol0->GetValue(0), 2);
    EXPECT_EQ(outcol0->GetValue(1), 2);
    EXPECT_EQ(outcol0->GetValue(2), 1);
    EXPECT_EQ(outcol0->GetValue(3), 1);
}

TEST(StreamCalcBatchTest, DISABLED_VectorbatchExpressionSplitIndex)
{
    // OMNI_INT = 1, input [varchar, char, int], SplitIndex(col1, ',', 1)
    // Splits col1 based on delimiter ',' and returns the string at index 1.
    std::string desc = R"DELIM(
{"originDescription":"[21]:Calc(select=[names])",
"inputTypes":["VARCHAR(2147483647)", "CHAR", "BIGINT"],
"outputTypes":["VARCHAR(2147483647)"],
"indices":[
    {"exprType":"FUNCTION","returnType":15,"function_name":"SplitIndex",
    "arguments":[
        {"exprType":"FIELD_REFERENCE","dataType":15,"colVal":0,"width":200},
        {"exprType": "LITERAL" ,"dataType":16,"isNull":false,"value":",", "width":200},
        {"exprType": "LITERAL" ,"dataType":1,"isNull":false,"value":1}
    ]
    }
]
})DELIM";

    int rowCnt = 4;
    std::vector<std::string> inputStr = {"Jack,John,Mary", "Jack,Johnny,Mary", "Jack,Mary", "Jack"};
    auto inputRecord = BuildStringVectorBatch(inputStr.data(), rowCnt);

    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    VarcharVector* incol0 = static_cast<VarcharVector*>(inputRecord->Get(0));

    std::cout << incol0->GetValue(0) << std::endl;
    std::cout << incol0->GetValue(1) << std::endl;
    std::cout << incol0->GetValue(2) << std::endl;
    std::cout << incol0->GetValue(3) << std::endl;

    auto outputRecord = ProcessAndGetOutput(desc, inputRecord);
    VarcharVector* outcol0 = static_cast<VarcharVector*>(outputRecord->Get(0));

    std::cout << outcol0->GetValue(0) << std::endl;
    std::cout << outcol0->GetValue(1) << std::endl;
    std::cout << outcol0->GetValue(2) << std::endl;
    std::cout << outcol0->GetValue(3) << std::endl;

    EXPECT_EQ(outcol0->GetValue(0), "John");
    EXPECT_EQ(outcol0->GetValue(1), "Johnny");
    EXPECT_EQ(outcol0->GetValue(2), "Mary");
    EXPECT_EQ(outcol0->GetValue(3), "");
}

TEST(StreamCalcBatchTest, VectorbatchExpressionModulus)
{
    std::string desc = R"DELIM({"originDescription": null,
                                "inputTypes": ["BIGINT", "BIGINT"],
                                "outputTypes": ["BIGINT", "BIGINT"],
                                "indices": [
                                    {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 0},
                                    {"exprType": "BINARY", "returnType": 2, "operator": "MODULUS",
                                        "left":  {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 1},
                                        "right": {"exprType": "LITERAL", "dataType": 2, "isNull": false, "value": 10000}
                                    }
                                ],
                                "condition": null
                        })DELIM";

    int nrow = 4;
    std::vector<long> col1(nrow);
    std::vector<long> col2(nrow);

    for (int i = 0; i < nrow; i++) {
        col1[i] = i;
        col2[i] = 123456 * (i + 1);
    }

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col2.data()));

    auto outputRecord = ProcessAndGetOutput(desc, vb);

    auto outcol1 = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord->Get(1));
    EXPECT_EQ(outputRecord->GetRowCount(), nrow);
    EXPECT_EQ(outputRecord->GetVectorCount(), 2);
    EXPECT_EQ(outcol1->GetValue(0), 3456);
    EXPECT_EQ(outcol1->GetValue(1), 6912);
    EXPECT_EQ(outcol1->GetValue(2), 368);
    EXPECT_EQ(outcol1->GetValue(3), 3824);
}

TEST(StreamCalcBatchTest, DISABLED_Q1Test)
{
    std::string Q1description = R"delimiter({
    "originDescription": null,
    "inputTypes": [
      "BIGINT",
      "BIGINT",
      "BIGINT",
      "STRING",
      "STRING",
      "BIGINT",
      "STRING"
    ],
    "outputTypes": [
      "BIGINT",
      "BIGINT",
      "DECIMAL(23, 3)",
      "BIGINT",
      "STRING"
    ],
    "indices": [
      {
        "exprType": "FIELD_REFERENCE",
        "dataType": 2,
        "colVal": 0
      },
      {
        "exprType": "FIELD_REFERENCE",
        "dataType": 2,
        "colVal": 1
      },
      {
        "exprType": "BINARY",
        "returnType": 7,
        "precision": 24,
        "scale": 3,
        "operator": "MULTIPLY",
        "left": {
          "exprType": "LITERAL",
          "dataType": 6,
          "precision": 24,
          "scale": 3,
          "isNull": false,
          "value": 908
        },
        "right": {
          "exprType": "FIELD_REFERENCE",
          "dataType": 2,
          "colVal": 2
        }
      },
      {
        "exprType": "FIELD_REFERENCE",
        "dataType": 2,
        "colVal": 5
      },
      {
        "exprType": "FIELD_REFERENCE",
        "dataType": 15,
        "width": 2147483647,
        "colVal": 6
      }
    ],
    "condition": null
  })delimiter";

    auto inputRecord = new omnistream::VectorBatch(1);

    inputRecord->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, new int64_t(10)));
    inputRecord->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, new int64_t(100)));
    inputRecord->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, new int64_t(1000)));
    inputRecord->Append(omniruntime::TestUtil::CreateVarcharVector(new std::string("test1"), 1));
    inputRecord->Append(omniruntime::TestUtil::CreateVarcharVector(new std::string("test2"), 1));
    inputRecord->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, new int64_t(10000)));
    inputRecord->Append(omniruntime::TestUtil::CreateVarcharVector(new std::string("test3"), 1));

    auto out = ProcessAndGetOutput(Q1description, inputRecord);

    auto outputBatch = new omnistream::VectorBatch(1);

    outputBatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, new int64_t(10)));
    outputBatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, new int64_t(100)));
    outputBatch->Append(omniruntime::TestUtil::CreateVector<Decimal128>(1, new Decimal128(int128_t(908))));
    outputBatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, new int64_t(10000)));
    outputBatch->Append(omniruntime::TestUtil::CreateVarcharVector(new std::string("test3"), 1));

    EXPECT_TRUE(omniruntime::TestUtil::VecBatchMatch(out, outputBatch));
}

TEST(StreamCalcBatchTest, HashCodeFunctionTest)
{
    std::string desc = R"DELIM({
            "originDescription": null,
            "inputTypes": ["BIGINT"],
            "outputTypes": ["INT"],
            "indices": [
                {
                "exprType": "FUNCTION",
                "function_name": "mm3hash",
                "returnType": 1,
                "arguments": [
                    {
                    "exprType": "FIELD_REFERENCE",
                    "dataType": 2,
                    "colVal": 0
                    },
                    {
                    "exprType": "LITERAL",
                    "dataType": 1,
                    "isNull":false,
                    "value": 0
                    }
                    ]
                }
            ],
            "condition": null
            }
            )DELIM";

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(1);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, new int64_t(1)));

    EXPECT_NO_THROW(ProcessAndGetOutput(desc, vb));
}

TEST(StreamCalcBatchTest, VectorbatchLower)
{
    // Select col1, date_format(col4) where col0=1 and col3 in [10,100]
    std::string desc = R"DELIM(
{
"originDescription":null,
"inputTypes":["STRING"],
"outputTypes":["STRING"],
"indices":[
    {"exprType":"FUNCTION",
    "returnType":15,
    "width":10,
    "function_name":"lower",
    "arguments": {"arg1":{"exprType":"FIELD_REFERENCE","dataType":15,"colVal":0, "width":10}}
    }
]
}
 )DELIM";

    int nrow = 4;
    std::vector<std::string> col0 = {"APPLE", "BANANA", "STRABERRY", "PEAR"};

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col0.data(), nrow));
    auto outputRecord = ProcessAndGetOutput(desc, vb);

    std::string filename = "/tmp/streamcalc_output.txt";
    outputRecord->writeToFile(filename);
}

TEST(StreamCalcBatchTest, CodegenSEARCH_LongAndVarchar)
{
    // Select IS TRUE col1 between [0, 200] where col2 in ["hello_world0", "hello_world1"]
    std::string desc = R"DELIM({"originDescription":null,
"inputTypes":["BIGINT","BIGINT", "STRING"],
"outputTypes":["BOOLEAN"],
"indices":[
    {
        "exprType":"BETWEEN",
        "returnType":4,
        "value":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":1},
        "lower_bound":{"exprType":"LITERAL","isNull":false,"value":100,"dataType":2},
        "upper_bound":{"exprType":"LITERAL","isNull":false,"value":200,"dataType":2}
    }
],
"condition":{"exprType":"IN","returnType":4,
"arguments":{
    "value":{"exprType":"FIELD_REFERENCE","dataType":15, "width":12, "colVal":2},
    "arg0":{"exprType":"LITERAL","isNull":false,"value":"hello_world0","width":12, "dataType":15},
    "arg1":{"exprType":"LITERAL","isNull":false,"value":"hello_world1","width":12, "dataType":15}
}
}
})DELIM";
    int nrow = 6;
    std::vector<int> col0 = {0, 1, 2, 3, 4, 5};
    std::vector<long> col1 = {2, 143, 144, 45, 46, 47};
    std::vector<std::string> col2 = {
        "hello_world0", "hello_world1", "hello_world2", "hello_world3", "hello_world4", "hello_world5"};

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVector<int32_t>(nrow, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col2.data(), nrow));

    auto outputRecord = ProcessAndGetOutput(desc, vb);
    EXPECT_EQ(outputRecord->GetRowCount(), 1);
    std::string filename = "/tmp/streamcalc_output.txt";
    outputRecord->writeToFile(filename);
}

TEST(StreamCalcBatchTest, DISABLED_HourFunctionTest)
{
    std::string desc = R"DELIM(
            {
              "originDescription": null,
              "inputTypes": ["TIMESTAMP(3)"],
              "outputTypes": ["INT"],
              "indices": [
                {
                  "exprType": "FUNCTION",
                  "returnType": 1,
                  "function_name": "get_hour",
                  "arguments": [
                    { "exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 0 }
                  ]
                }
              ],
              "condition": null
            })DELIM";

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(1);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, new int64_t(52200000)));

    auto out = ProcessAndGetOutput(desc, vb);

    auto expected = new omnistream::VectorBatch(1);
    expected->Append(omniruntime::TestUtil::CreateVector<int32_t>(1, new int32_t(14)));

    EXPECT_TRUE(omniruntime::TestUtil::VecBatchMatch(out, expected));
}

TEST(StreamCalcBatchTest, Q12ProctimeTest)
{
    std::string desc = R"DELIM({
            "originDescription": null,
            "inputTypes": [
                "BIGINT"
            ],
            "outputTypes": [
                "TIMESTAMP_LTZ(3)"
            ],
            "indices": [
                {"exprType":"PROCTIME","returnType": 24}
            ],
            "condition": null
            }
            )DELIM";

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(1);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, new int64_t(1)));

    EXPECT_NO_THROW(ProcessAndGetOutput(desc, vb));
}

TEST(StreamCalcBatchTest, ProctimeMaterializeDateFormatTest)
{
    std::string desc = R"DELIM({
            "originDescription": "[2]:Calc(select=[id, DATE_FORMAT(PROCTIME_MATERIALIZE(PROCTIME()), 'yyyy-MM-dd') AS pt_day])",
            "inputTypes": [
                "INTEGER"
            ],
            "outputTypes": [
                "INTEGER",
                "VARCHAR(2147483647)"
            ],
            "indices": [
                {
                    "exprType": "FIELD_REFERENCE",
                    "dataType": 1,
                    "colVal": 0
                },
                {
                    "exprType": "FUNCTION",
                    "returnType": 15,
                    "width": 10,
                    "function_name": "from_unixtime_without_tz",
                    "arguments": [
                        {
                            "exprType": "PROCTIME",
                            "returnType": 24,
                            "dataType": 2
                        },
                        {
                            "exprType": "LITERAL",
                            "dataType": 15,
                            "width": 10,
                            "isNull": false,
                            "value": "%Y-%m-%d"
                        }
                    ]
                }
            ],
            "condition": null
            }
            )DELIM";

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(1);
    vb->Append(omniruntime::TestUtil::CreateVector<int32_t>(1, new int32_t(1)));

    auto outputRecord = ProcessAndGetOutput(desc, vb);
    auto outcol = reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
        outputRecord->Get(1));
    auto value = outcol->GetValue(0);

    EXPECT_EQ(outputRecord->GetRowCount(), 1);
    EXPECT_EQ(outputRecord->GetVectorCount(), 2);
    EXPECT_EQ(value.size(), 10);
    EXPECT_EQ(value[4], '-');
    EXPECT_EQ(value[7], '-');
}

TEST(StreamCalcBatchTest, JsonValueProjectionTest)
{
    std::string desc = R"DELIM({
            "originDescription": "[2]:Calc(select=[JSON_VALUE(c_json_data, '$.id') AS id, JSON_VALUE(c_json_data, '$.user.name') AS name, JSON_VALUE(c_json_data, '$.user.age') AS age, JSON_VALUE(c_json_data, '$.user.tags[2]') AS tags, JSON_VALUE(c_json_data, '$.scores[0]') AS scores, JSON_VALUE(c_json_data, '$.metadata.version') AS version, JSON_VALUE(c_json_data, '$.metadata.source') AS source])",
            "inputTypes": [
                "VARCHAR(2147483647)"
            ],
            "outputTypes": [
                "VARCHAR(2147483647)",
                "VARCHAR(2147483647)",
                "VARCHAR(2147483647)",
                "VARCHAR(2147483647)",
                "VARCHAR(2147483647)",
                "VARCHAR(2147483647)",
                "VARCHAR(2147483647)"
            ],
            "indices": [
                {
                    "exprType": "FUNCTION",
                    "function_name": "json_value",
                    "width": 2147483647,
                    "arguments": [
                        {
                            "exprType": "FIELD_REFERENCE",
                            "dataType": 15,
                            "width": 2147483647,
                            "colVal": 0
                        },
                        {
                            "exprType": "LITERAL",
                            "dataType": 16,
                            "isNull": false,
                            "width": 5,
                            "value": "$.id"
                        }
                    ],
                    "returnType": 15
                },
                {
                    "exprType": "FUNCTION",
                    "function_name": "json_value",
                    "width": 2147483647,
                    "arguments": [
                        {
                            "exprType": "FIELD_REFERENCE",
                            "dataType": 15,
                            "width": 2147483647,
                            "colVal": 0
                        },
                        {
                            "exprType": "LITERAL",
                            "dataType": 16,
                            "isNull": false,
                            "width": 12,
                            "value": "$.user.name"
                        }
                    ],
                    "returnType": 15
                },
                {
                    "exprType": "FUNCTION",
                    "function_name": "json_value",
                    "width": 2147483647,
                    "arguments": [
                        {
                            "exprType": "FIELD_REFERENCE",
                            "dataType": 15,
                            "width": 2147483647,
                            "colVal": 0
                        },
                        {
                            "exprType": "LITERAL",
                            "dataType": 16,
                            "isNull": false,
                            "width": 11,
                            "value": "$.user.age"
                        }
                    ],
                    "returnType": 15
                },
                {
                    "exprType": "FUNCTION",
                    "function_name": "json_value",
                    "width": 2147483647,
                    "arguments": [
                        {
                            "exprType": "FIELD_REFERENCE",
                            "dataType": 15,
                            "width": 2147483647,
                            "colVal": 0
                        },
                        {
                            "exprType": "LITERAL",
                            "dataType": 16,
                            "isNull": false,
                            "width": 14,
                            "value": "$.user.tags[2]"
                        }
                    ],
                    "returnType": 15
                },
                {
                    "exprType": "FUNCTION",
                    "function_name": "json_value",
                    "width": 2147483647,
                    "arguments": [
                        {
                            "exprType": "FIELD_REFERENCE",
                            "dataType": 15,
                            "width": 2147483647,
                            "colVal": 0
                        },
                        {
                            "exprType": "LITERAL",
                            "dataType": 16,
                            "isNull": false,
                            "width": 11,
                            "value": "$.scores[0]"
                        }
                    ],
                    "returnType": 15
                },
                {
                    "exprType": "FUNCTION",
                    "function_name": "json_value",
                    "width": 2147483647,
                    "arguments": [
                        {
                            "exprType": "FIELD_REFERENCE",
                            "dataType": 15,
                            "width": 2147483647,
                            "colVal": 0
                        },
                        {
                            "exprType": "LITERAL",
                            "dataType": 16,
                            "isNull": false,
                            "width": 18,
                            "value": "$.metadata.version"
                        }
                    ],
                    "returnType": 15
                },
                {
                    "exprType": "FUNCTION",
                    "function_name": "json_value",
                    "width": 2147483647,
                    "arguments": [
                        {
                            "exprType": "FIELD_REFERENCE",
                            "dataType": 15,
                            "width": 2147483647,
                            "colVal": 0
                        },
                        {
                            "exprType": "LITERAL",
                            "dataType": 16,
                            "isNull": false,
                            "width": 17,
                            "value": "$.metadata.source"
                        }
                    ],
                    "returnType": 15
                }
            ],
            "condition": null
            })DELIM";

    std::string inputJson =
        R"({"id":721,"user":{"name":"Alice","age":53,"tags":["tag0","tag1","tag2"]},"scores":[79.39,86.38,71.8],"metadata":{"version":"1.0","source":"web"}})";
    auto vb = BuildStringVectorBatch(&inputJson, 1);

    auto outputRecord = ProcessAndGetOutput(desc, vb);
    EXPECT_EQ(outputRecord->GetRowCount(), 1);
    EXPECT_EQ(outputRecord->GetVectorCount(), 7);

    using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto out0 = reinterpret_cast<VarcharVector*>(outputRecord->Get(0));
    auto out1 = reinterpret_cast<VarcharVector*>(outputRecord->Get(1));
    auto out2 = reinterpret_cast<VarcharVector*>(outputRecord->Get(2));
    auto out3 = reinterpret_cast<VarcharVector*>(outputRecord->Get(3));
    auto out4 = reinterpret_cast<VarcharVector*>(outputRecord->Get(4));
    auto out5 = reinterpret_cast<VarcharVector*>(outputRecord->Get(5));
    auto out6 = reinterpret_cast<VarcharVector*>(outputRecord->Get(6));

    EXPECT_EQ(out0->GetValue(0), "721");
    EXPECT_EQ(out1->GetValue(0), "Alice");
    EXPECT_EQ(out2->GetValue(0), "53");
    EXPECT_EQ(out3->GetValue(0), "tag2");
    EXPECT_EQ(out4->GetValue(0), "79.39");
    EXPECT_EQ(out5->GetValue(0), "1.0");
    EXPECT_EQ(out6->GetValue(0), "web");
}

TEST(StreamCalcBatchTest, Q21RegexpExtractTest)
{
    std::string desc = R"DELIM(
{"originDescription":null,
    "inputTypes":["STRING"],
    "outputTypes":["STRING"],
    "indices":[
        {"exprType":"FUNCTION","returnType":15,"width":100,
        "function_name":"regex_extract_null",
        "arguments":[
            {"exprType":"FIELD_REFERENCE","dataType":15,"width":100,"colVal":0},
            {"exprType":"LITERAL","dataType":16,"width":23,"isNull":false,"value":"(&|^)channel_id=([^&]*)"},
            {"exprType":"LITERAL","dataType":1,"isNull":false,"value":2}
            ]
        }
    ],
    "condition": null
}
)DELIM";
    int nrow = 3;
    std::vector<std::string> col2 = {
        "channel_id=123&user=abc", "user=abc&channel_id=456&source=xyz", "source=xyz&channel_id=789"};
    std::vector<std::string> col_expected = {"123", "456", "789"};

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col2.data(), nrow));

    auto outputRecord = ProcessAndGetOutput(desc, vb);

    omnistream::VectorBatch* vb_expected = new omnistream::VectorBatch(nrow);
    vb_expected->Append(omniruntime::TestUtil::CreateVarcharVector(col_expected.data(), nrow));

    EXPECT_TRUE(omniruntime::TestUtil::VecBatchMatch(outputRecord, vb_expected));
    std::string filename = "/tmp/streamcalc_output.txt";
    outputRecord->writeToFile(filename);
}

TEST(StreamCalcBatchTest, DISABLED_VectorbatchExpressionDateFormat)
{
    std::string desc = R"DELIM({"originDescription": null,
            "inputTypes": ["BIGINT", "BIGINT"],
            "outputTypes": ["BIGINT", "VARCHAR(2147483647)", "VARCHAR(2147483647)"],
            "indices": [
                        {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 0},
                        {"exprType": "FUNCTION", "returnType": 15, "width": 10, "function_name": "from_unixtime_without_tz",
                            "arguments": [
                                {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 1},
                                {"exprType": "LITERAL", "dataType": 15, "isNull": false, "value": "%Y-%m-%d", "width":10}
                            ]
                        },
                        {"exprType": "FUNCTION", "returnType": 15, "width": 5, "function_name": "from_unixtime_without_tz",
                            "arguments": [
                                {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 1},
                                {"exprType": "LITERAL", "dataType": 15, "isNull": false, "value": "%H:%M", "width":5}
                            ]
                        }],
            "condition": null
        })DELIM";

    int nrow = 3;
    std::vector<long> col1(nrow);
    std::vector<long> col2(nrow);

    for (int i = 0; i < nrow; i++) {
        col1[i] = i;
        col2[i] = 1740484255000 + (3543215555 * i);
    }

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col2.data()));

    auto outputRecord = ProcessAndGetOutput(desc, vb);

    auto outcol1 =
        reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
            outputRecord->Get(1));
    auto outcol2 =
        reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
            outputRecord->Get(2));

    EXPECT_EQ(outputRecord->GetRowCount(), nrow);
    EXPECT_EQ(outputRecord->GetVectorCount(), 3);
    EXPECT_EQ(outcol1->GetValue(0), "2025-02-25");
    EXPECT_EQ(outcol1->GetValue(1), "2025-04-07");
    EXPECT_EQ(outcol1->GetValue(2), "2025-05-18");
    EXPECT_EQ(outcol2->GetValue(0), "11:50");
    EXPECT_EQ(outcol2->GetValue(1), "12:04");
    EXPECT_EQ(outcol2->GetValue(2), "12:18");
}

TEST(StreamCalcBatchTest, DISABLED_CASE_WHEN)
{
    std::string desc = R"DELIM(
{"condition":null,
"indices":[
{
"Case1":{"exprType":"WHEN",
"result":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"dayTime","width":9},
"returnType":15,
"width":9,
"when":{"exprType":"BINARY",
"left":{"colVal":0,"dataType":2,"exprType":"FIELD_REFERENCE"},
"operator":"GREATER_THAN_OR_EQUAL",
"returnType":4,
"right":{"dataType":2,"exprType":"LITERAL","isNull":false,"value":88}
}
},
"Case2":{"exprType":"WHEN",
"result":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"dayTime","width":9},
"returnType":15,
"width":9,
"when":{"exprType":"BINARY",
"left":{"colVal":0,"dataType":2,"exprType":"FIELD_REFERENCE"},
"operator":"LESS_THAN_OR_EQUAL",
"returnType":4,
"right":{"dataType":2,"exprType":"LITERAL","isNull":false,"value":8}
}
},
"else":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"otherTime","width":9},
"exprType":"SWITCH_GENERAL","returnType":15,"width":9,
"numOfCases":2
}],
"inputTypes":["BIGINT"],"originDescription":null,
"outputTypes":["VARCHAR(9) NOT NULL"]}
)DELIM";

    int nrow = 3;
    std::vector<long> col1{1, 100, 40};

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col1.data()));

    auto outputRecord = ProcessAndGetOutput(desc, vb);

    auto outcol1 =
        reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
            outputRecord->Get(0));
    std::string filename = "/tmp/flink_switch.txt";
    outputRecord->writeToFile(filename);
    EXPECT_EQ(outputRecord->GetRowCount(), nrow);
    EXPECT_EQ(outputRecord->GetVectorCount(), 1);
    EXPECT_EQ(outcol1->GetValue(0), "dayTime");
    EXPECT_EQ(outcol1->GetValue(1), "dayTime");
    EXPECT_EQ(outcol1->GetValue(2), "otherTime");
}

TEST(StreamCalcBatchTest, DISABLED_COMPARE_CAST)
{
    std::string desc = R"DELIM(
{"condition":
    {"exprType":"BINARY",
    "left":{"exprType":"BINARY",
        "left":{"colVal":0,"dataType":1,"exprType":"FIELD_REFERENCE"},
        "operator":"EQUAL","returnType":4,
        "right":{"dataType":1,"exprType":"LITERAL","isNull":false,"value":2}
    },
    "operator":"AND",
    "returnType":4,
    "right":{"exprType":"BINARY",
        "left":{"arguments":
                    [{"arguments":
                        [{"colVal":23,"dataType":15,"exprType":"FIELD_REFERENCE","width":2147483647},
                        {"dataType":16,"exprType":"LITERAL","isNull":false,"value":"(&|^)channel_id=([^&]*)","width":23},
                        {"dataType":1,"exprType":"LITERAL","isNull":false,"value":2}],
                    "exprType":"FUNCTION",
                    "function_name":"regex_extract_null",
                    "returnType":15,"width":2147483647}
                    ],
                "exprType":"IS_NOT_NULL",
                "returnType":4
                },
        "operator":"OR",
        "returnType":4,
        "right":{"arguments":
                    [{"arguments":{"arg0":{"colVal":22,"dataType":15,"exprType":"FIELD_REFERENCE","width":2147483647}},
                    "exprType":"FUNCTION",
                    "function_name":"lower",
                    "returnType":15,"width":2147483647},
                    {"dataType":15,"exprType":"LITERAL","isNull":false,"value":"apple","width":2147483647},
                    {"dataType":15,"exprType":"LITERAL","isNull":false,"value":"baidu","width":2147483647},
                    {"dataType":15,"exprType":"LITERAL","isNull":false,"value":"facebook","width":2147483647},
                    {"dataType":15,"exprType":"LITERAL","isNull":false,"value":"google","width":2147483647}],"exprType":"IN","returnType":4}}},"indices":[{"colVal":19,"dataType":2,"exprType":"FIELD_REFERENCE"},{"colVal":20,"dataType":2,"exprType":"FIELD_REFERENCE"},{"colVal":21,"dataType":2,"exprType":"FIELD_REFERENCE"},{"colVal":22,"dataType":15,"exprType":"FIELD_REFERENCE","width":2147483647},{"Case1":{"exprType":"WHEN","result":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"0","width":2147483647},"returnType":15,"when":{"exprType":"BINARY","left":{"arguments":{"arg0":{"colVal":22,"dataType":15,"exprType":"FIELD_REFERENCE","width":2147483647}},"exprType":"FUNCTION","function_name":"lower","returnType":15,"width":2147483647},"operator":"EQUAL","returnType":4,"right":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"apple","width":2147483647}},"width":2147483647},"Case2":{"exprType":"WHEN","result":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"1","width":2147483647},"returnType":15,"when":{"exprType":"BINARY","left":{"arguments":{"arg0":{"colVal":22,"dataType":15,"exprType":"FIELD_REFERENCE","width":2147483647}},"exprType":"FUNCTION","function_name":"lower","returnType":15,"width":2147483647},"operator":"EQUAL","returnType":4,"right":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"google","width":2147483647}},"width":2147483647},"Case3":{"exprType":"WHEN","result":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"2","width":2147483647},"returnType":15,"when":{"exprType":"BINARY","left":{"arguments":{"arg0":{"colVal":22,"dataType":15,"exprType":"FIELD_REFERENCE","width":2147483647}},"exprType":"FUNCTION","function_name":"lower","returnType":15,"width":2147483647},"operator":"EQUAL","returnType":4,"right":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"facebook","width":2147483647}},"width":2147483647},"Case4":{"exprType":"WHEN","result":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"3","width":2147483647},"returnType":15,"when":{"exprType":"BINARY","left":{"arguments":{"arg0":{"colVal":22,"dataType":15,"exprType":"FIELD_REFERENCE","width":2147483647}},"exprType":"FUNCTION","function_name":"lower","returnType":15,"width":2147483647},"operator":"EQUAL","returnType":4,"right":{"dataType":15,"exprType":"LITERAL","isNull":false,"value":"baidu","width":2147483647}},"width":2147483647},"else":{"arguments":[{"colVal":23,"dataType":15,"exprType":"FIELD_REFERENCE","width":2147483647},{"dataType":16,"exprType":"LITERAL","isNull":false,"value":"(&|^)channel_id=([^&]*)","width":23},{"dataType":1,"exprType":"LITERAL","isNull":false,"value":2}],"exprType":"FUNCTION","function_name":"regex_extract_null","returnType":15,"width":2147483647},"exprType":"SWITCH_GENERAL","numOfCases":4,"returnType":15,"width":2147483647}],"inputTypes":["INTEGER","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","VARCHAR(2147483647)","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)"],"originDescription":null,"outputTypes":["BIGINT","BIGINT","BIGINT","STRING","STRING"]}
)DELIM";

    int nrow = 3;
    std::vector<long> col1{100, 200, 300};
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(nrow, col1.data()));
    json parsedJson = json::parse(desc);

    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    StreamCalcBatch streamCalcBatchOp(parsedJson, output);
    streamCalcBatchOp.open();

    StreamRecord* record = new StreamRecord(vb);
    streamCalcBatchOp.processBatch(record);
    auto out = output->getAll()[0];
    std::string filename = "/tmp/flink_compare_cast.txt";
    reinterpret_cast<omnistream::VectorBatch*>(out)->writeToFile(filename);
}

TEST(StreamCalcBatchTest, DISABLED_RegexpExtractNullTest)
{
    std::string desc = R"DELIM({"originDescription":null,
                                    "inputTypes":["STRING"],
                                    "outputTypes":["STRING"],
                                    "indices":[
                                        {"exprType":"FUNCTION","returnType":15,"width":100,
                                        "function_name":"regex_extract_null",
                                        "arguments":{
                                            "arg0":{"exprType":"FIELD_REFERENCE","dataType":15,"width":100,"colVal":0},
                                            "arg1":{"exprType":"LITERAL","dataType":16,"width":23,"isNull":false,"value":"(&|^)channel_id=([^&]*)"},
                                            "arg2":{"exprType":"LITERAL","dataType":1,"isNull":false,"value":2}
                                        }
                                        }
                                    ],
                                    "condition": null
                                }
                                )DELIM";
    int nrow = 3;
    std::vector<std::string> col_input = {
        "channel_id=123&user=abc", "user=abc&source=xyz", "source=xyz&channel_id=789"};
    std::vector<std::string> col_expected = {
        "Input string is set to NULL, should return NULL", "Match not found, should return NULL", "789"};

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(nrow);
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col_input.data(), nrow));
    vb->Get(0)->SetNull(0);
    auto outputRecord = ProcessAndGetOutput(desc, vb);

    omnistream::VectorBatch* vb_expected = new omnistream::VectorBatch(nrow);
    vb_expected->Append(omniruntime::TestUtil::CreateVarcharVector(col_expected.data(), nrow));
    vb_expected->Get(0)->SetNull(0);
    vb_expected->Get(0)->SetNull(1);

    EXPECT_TRUE(omniruntime::TestUtil::VecBatchMatch(outputRecord, vb_expected));
}

TEST(StreamCalcBatchTest, BinaryRowProjectionPreservesNullsTypesAndTimestamp)
{
    auto description = json::parse(R"({"inputTypes":["BIGINT","INT","BOOLEAN","VARCHAR"],
        "outputTypes":["VARCHAR","INT","BOOLEAN","BIGINT"],"InputRecordType":"BINARY_ROW_DATA",
        "indices":[{"exprType":"FIELD_REFERENCE","dataType":15,"colVal":3},
        {"exprType":"FIELD_REFERENCE","dataType":1,"colVal":1},
        {"exprType":"FIELD_REFERENCE","dataType":4,"colVal":2},
        {"exprType":"FIELD_REFERENCE","dataType":2,"colVal":0}],"condition":null})");
    BatchOutputTest output;
    StreamCalcBatch op(description, &output);
    op.open();
    auto* row = BinaryRowData::createBinaryRowDataWithMem(4);
    row->setNullAt(0);
    row->setInt(1, 7);
    row->setBool(2, true);
    row->setNullAt(3);
    row->setRowKind(RowKind::UPDATE_AFTER);
    auto* input = new StreamRecord(row, 1234L);
    input->setValueType(RecordValueType::BINARY_ROW_DATA);
    op.processBatch(input);
    auto result = std::unique_ptr<omnistream::VectorBatch>(output.getVectorBatch());
    auto record = std::unique_ptr<StreamRecord>(output.getStreamRecord());
    ASSERT_EQ(result->GetRowCount(), 1);
    EXPECT_TRUE(result->Get(0)->IsNull(0));
    EXPECT_EQ(static_cast<omniruntime::vec::Vector<int32_t>*>(result->Get(1))->GetValue(0), 7);
    EXPECT_TRUE(static_cast<omniruntime::vec::Vector<bool>*>(result->Get(2))->GetValue(0));
    EXPECT_TRUE(result->Get(3)->IsNull(0));
    EXPECT_EQ(result->getRowKind(0), RowKind::UPDATE_AFTER);
    EXPECT_EQ(result->getTimestamp(0), 1234L);
    EXPECT_EQ(record->getTimestamp(), 1234L);
}
