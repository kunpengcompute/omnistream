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

#include "../../streaming/api/operators/StreamCalcBatch.cpp"
#include "../../runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "../../runtime/state/TaskStateManager.h"
#include "../../streaming/api/operators/KeyedProcessOperator.h"
#include "table/runtime/operators/join/stream/StreamingJoinOperator.h"
#include "../../table/runtime/operators/join/window/WindowJoinOperator.h"
#include "../../table/runtime/operators/rank/FastTop1Function.h"
#include "../../table/data/vectorbatch/VectorBatch.h"
#include "../../test/core/operators/OutputTest.h"

std::string calc2Description =
    R"delimiter({"originDescription":"[2]:Calc(select=[event_type, auction, CASE((event_type = 0), person.dateTime, (event_type = 1), auction.dateTime, bid.dateTime) AS dateTime])","inputTypes":["INTEGER","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","VARCHAR(2147483647)","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)"],"outputTypes":["INTEGER","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)"],"indices":[{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":9},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":10},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":11},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":12},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":13},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":14},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":15},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":16},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":17},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":18},{"exprType":"SWITCH_GENERAL","returnType":2,"numOfCases":2,"Case1":{"when":{"exprType":"BINARY","returnType":4,"operator":"EQUAL","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":0},"right":{"exprType":"LITERAL","dataType":1,"isNull":false,"value":0}},"result":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":7},"exprType":"WHEN","returnType":2},"Case2":{"when":{"exprType":"BINARY","returnType":4,"operator":"EQUAL","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":0},"right":{"exprType":"LITERAL","dataType":1,"isNull":false,"value":1}},"result":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":14},"exprType":"WHEN","returnType":2},"else":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":24}}],"condition":null})delimiter";
nlohmann::json calc2json = nlohmann::json::parse(calc2Description);
std::string calc4Description =
    R"delimiter({"originDescription":"[4]:Calc(select=[auction.id AS id, auction.itemName AS itemName, auction.description AS description, auction.initialBid AS initialBid, auction.reserve AS reserve, CAST(dateTime AS TIMESTAMP(3)) AS dateTime, auction.expires AS expires, auction.seller AS seller, auction.category AS category, auction.extra AS extra], where=[(event_type = 1)])","inputTypes":["INTEGER","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)"],"outputTypes":["BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)"],"indices":[{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":1},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":2},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":3},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":4},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":5},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":11},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":7},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":8},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":9},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":10}],"condition":{"exprType":"BINARY","returnType":4,"operator":"EQUAL","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":0},"right":{"exprType":"LITERAL","dataType":1,"isNull":false,"value":1}}})delimiter";
nlohmann::json calc4json = nlohmann::json::parse(calc4Description);
std::string calc6Description =
    R"delimiter({"originDescription":"[6]:Calc(select=[event_type, bid, CASE((event_type = 0), person.dateTime, (event_type = 1), auction.dateTime, bid.dateTime) AS dateTime])","inputTypes":["INTEGER","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","VARCHAR(2147483647)","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)"],"outputTypes":["INTEGER","BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)"],"indices":[{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":19},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":20},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":21},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":22},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":23},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":24},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":25},{"exprType":"SWITCH_GENERAL","returnType":2,"numOfCases":2,"Case1":{"when":{"exprType":"BINARY","returnType":4,"operator":"EQUAL","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":0},"right":{"exprType":"LITERAL","dataType":1,"isNull":false,"value":0}},"result":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":7},"exprType":"WHEN","returnType":2},"Case2":{"when":{"exprType":"BINARY","returnType":4,"operator":"EQUAL","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":0},"right":{"exprType":"LITERAL","dataType":1,"isNull":false,"value":1}},"result":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":14},"exprType":"WHEN","returnType":2},"else":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":24}}],"condition":null})delimiter";
nlohmann::json calc6json = nlohmann::json::parse(calc6Description);
std::string calc8Description =
    R"delimiter({"inputTypes":["INTEGER","BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)"],"indices":[{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":1},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":2},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":3},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":4},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":5},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":8},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":7}],"condition":{"exprType":"BINARY","left":{"exprType":"FIELD_REFERENCE","dataType":1,"colVal":0},"right":{"exprType":"LITERAL","dataType":1,"isNull":false,"value":2},"returnType":4,"operator":"EQUAL"},"outputTypes":["BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)"],"originDescription":"[8]:Calc(select=[bid.auction AS auction, bid.bidder AS bidder, bid.price AS price, bid.channel AS channel, bid.url AS url, CAST(dateTime AS TIMESTAMP(3)) AS dateTime, bid.extra AS extra], where=[(event_type = 2)])"})delimiter";
nlohmann::json calc8json = nlohmann::json::parse(calc8Description);
std::string joinDescription =
    R"delimiter({"rightJoinKey":[0],"leftUniqueKeys":[],"leftInputSpec":"NoUniqueKey","filterNulls":[true],"rightUniqueKeys":[],"rightInputSpec":"NoUniqueKey","nonEquiCondition":{"exprType":"BINARY","left":{"exprType":"BINARY","left":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":15},"right":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":5},"returnType":4,"operator":"GREATER_THAN_OR_EQUAL"},"right":{"exprType":"BINARY","left":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":15},"right":{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":6},"returnType":4,"operator":"LESS_THAN_OR_EQUAL"},"returnType":4,"operator":"AND"},"joinType":"InnerJoin","rightInputTypes":["BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)"],"outputTypes":["BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)"],"leftInputTypes":["BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)"],"leftJoinKey":[0],"originDescription":"[10]:Join(joinType=[InnerJoin], where=[((id = auction) AND (dateTime0 >= dateTime) AND (dateTime0 <= expires))], select=[id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra, auction, bidder, price, channel, url, dateTime0, extra0], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])"})delimiter";
nlohmann::json joinjson = nlohmann::json::parse(joinDescription);
std::string calc11Description =
    R"delimiter({"inputTypes":["BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)"],"indices":[{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":0},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":1},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":2},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":3},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":4},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":5},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":6},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":7},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":8},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":9},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":10},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":11},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":12},{"exprType":"FIELD_REFERENCE","dataType":2,"colVal":15},{"exprType":"FIELD_REFERENCE","dataType":15,"width":2147483647,"colVal":16}],"condition":null,"outputTypes":["BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)"],"originDescription":"[11]:Calc(select=[id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra, auction, bidder, price, dateTime0, extra0])"})delimiter";
nlohmann::json calc11json = nlohmann::json::parse(calc11Description);
std::string rankDescription =
    R"delimiter({"originDescription":null,"inputTypes":["BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)"],"outputTypes":["BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","TIMESTAMP_WITHOUT_TIME_ZONE(3)","BIGINT","BIGINT","VARCHAR(2147483647)","BIGINT","BIGINT","BIGINT","TIMESTAMP_WITHOUT_TIME_ZONE(3)","VARCHAR(2147483647)"],"partitionKey":[0],"outputRankNumber":false,"rankRange":"rankStart=1, rankEnd=1","generateUpdateBefore":false,"processFunction":"FastTop1Function","sortFieldIndices":[12,13],"sortAscendingOrders":[false,true],"sortNullsIsLast":[true,false]})delimiter";
nlohmann::json rankjson = nlohmann::json::parse(rankDescription);

#include <random>

int rand_int(int min, int max)
{
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<int> distribution(min, max);
    return distribution(generator);
}

long rand_long(long min, long max)
{
    static thread_local std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<long> distribution(min, max);
    return distribution(generator);
}

std::string rand_string(size_t length)
{
    static const char charset[] = "abcdefghijklmnopqrstuvwxyz"
                                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                  "0123456789";
    static thread_local std::mt19937 generator(std::random_device{}());
    static std::uniform_int_distribution<size_t> dist(0, sizeof(charset) - 2);

    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result += charset[dist(generator)];
    }
    return result;
}

enum class Typing {
    INT,
    LONG,
    STRING
};

StreamRecord* makeBatch(std::vector<Typing> types)
{
    int size = 1000;
    auto batch = new omnistream::VectorBatch(size);
    for (auto type : types) {
        if (type == Typing::INT) {
            auto vec = new omniruntime::vec::Vector<int>(size);
            for (int i = 0; i < size; ++i) {
                vec->SetValue(i, rand_int(1, 1000000));
            }
            batch->Append(vec);
        } else if (type == Typing::LONG) {
            auto vec = new omniruntime::vec::Vector<int64_t>(size);
            for (int i = 0; i < size; ++i) {
                vec->SetValue(i, rand_long(1, 1000000));
            }
            batch->Append(vec);
        } else if (type == Typing::STRING) {
            auto vec = new omniruntime::vec::Vector<omniruntime::LargeStringContainer<std::string_view>>(size);
            for (int i = 0; i < size; ++i) {
                auto str = rand_string(10);
                std::string_view str_view(str);
                vec->SetValue(i, str_view);
            }
            batch->Append(vec);
        }
    }
    return new StreamRecord(batch);
}

TEST(q9, test)
{
    auto output = new DeletingOutput();

    auto calc2 = new StreamCalcBatch(calc2json, output);
    auto calc4 = new StreamCalcBatch(calc4json, output);
    auto calc6 = new StreamCalcBatch(calc6json, output);
    auto calc8 = new StreamCalcBatch(calc8json, output);
    auto join = new StreamingJoinOperator<RowData*>(joinjson, output);
    auto calc11 = new StreamCalcBatch(calc11json, output);
    AbstractTopNFunction<RowData*>* func = new FastTop1Function<RowData*>(rankjson);
    auto* key = new KeyedProcessOperator(func, output, rankjson);

    calc2->setup();
    calc4->setup();
    calc6->setup();
    calc8->setup();
    join->setup();
    calc11->setup();
    key->setup();
    key->setDescription(rankjson);

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);

    join->initializeState(initializer, new BinaryRowDataSerializer(1));
    key->initializeState(initializer, new BinaryRowDataSerializer(1));

    calc2->open();
    calc4->open();
    calc6->open();
    calc8->open();
    join->open();
    calc11->open();
    key->open();

    calc2->processBatch(makeBatch(
        {Typing::INT,  Typing::LONG,   Typing::STRING, Typing::STRING, Typing::STRING, Typing::STRING, Typing::STRING,
         Typing::LONG, Typing::STRING, Typing::LONG,   Typing::STRING, Typing::STRING, Typing::LONG,   Typing::LONG,
         Typing::LONG, Typing::LONG,   Typing::LONG,   Typing::LONG,   Typing::STRING, Typing::LONG,   Typing::LONG,
         Typing::LONG, Typing::STRING, Typing::STRING, Typing::LONG,   Typing::STRING}));

    calc4->processBatch(makeBatch(
        {Typing::INT,
         Typing::LONG,
         Typing::STRING,
         Typing::STRING,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::STRING,
         Typing::LONG}));

    calc6->processBatch(makeBatch(
        {Typing::INT,  Typing::LONG,   Typing::STRING, Typing::STRING, Typing::STRING, Typing::STRING, Typing::STRING,
         Typing::LONG, Typing::STRING, Typing::LONG,   Typing::STRING, Typing::STRING, Typing::LONG,   Typing::LONG,
         Typing::LONG, Typing::LONG,   Typing::LONG,   Typing::LONG,   Typing::STRING, Typing::LONG,   Typing::LONG,
         Typing::LONG, Typing::STRING, Typing::STRING, Typing::LONG,   Typing::STRING}));

    calc8->processBatch(makeBatch(
        {Typing::INT,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::STRING,
         Typing::STRING,
         Typing::LONG,
         Typing::STRING,
         Typing::LONG}));

    calc11->processBatch(makeBatch(
        {Typing::LONG,
         Typing::STRING,
         Typing::STRING,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::STRING,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::STRING,
         Typing::STRING,
         Typing::LONG,
         Typing::STRING}));

    key->processBatch(makeBatch(
        {Typing::LONG,
         Typing::STRING,
         Typing::STRING,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::STRING,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::STRING}));

    join->processBatch1(makeBatch(
        {Typing::LONG,
         Typing::STRING,
         Typing::STRING,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::LONG,
         Typing::STRING}));
    join->processBatch2(makeBatch(
        {Typing::LONG, Typing::LONG, Typing::LONG, Typing::STRING, Typing::STRING, Typing::LONG, Typing::STRING}));

    calc2->close();
    calc4->close();
    calc6->close();
    calc8->close();
    calc11->close();
    key->close();
    join->close();

    delete calc2;
    delete calc4;
    delete calc6;
    delete calc8;
    delete calc11;
    delete key;
    delete join;

    EXPECT_TRUE(true);
}
