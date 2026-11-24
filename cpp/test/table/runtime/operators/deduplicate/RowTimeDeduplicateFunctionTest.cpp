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

#include "table/runtime/operators/deduplicate/RowTimeDeduplicateFunction.h"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <type_traits>
#include <vector>
#include <nlohmann/json.hpp>
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "functions/Collector.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "core/typeutils/LongSerializer.h"
#include <gtest/gtest.h>
#include "table/data/binary/BinaryRowData.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/EmbeddedRocksDBStateBackend.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "../../../../core/operators/OutputTest.h"
#include "table/data/util/VectorBatchUtil.h"
#include <string.h>
#include "table/data/RowKind.h"

#if defined(__has_include)
#if __has_include(<malloc.h>)
#include <malloc.h>
#endif
#endif

using namespace omnistream;

std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";

std::string description = R"DELIM({"input_channels":[0],
                        "operators":[{"description":{
        "miniBatchSize": -1,
        "inputTypes": [
          "BIGINT",
          "BIGINT",
          "BIGINT",
          "VARCHAR"
        ],
        "keepLastRow": true,
        "isCompactChanges": false,
        "minRetentionTime": 0,
        "rowtimeIndex": 2,
        "isRowtime": true,
        "generateUpdateBefore": true,
        "outputTypes": [
          "BIGINT",
          "BIGINT",
          "BIGINT",
          "VARCHAR(2147483647)"
        ],
        "generateInsert": true,
        "originDescription": null,
        "grouping": [
          0, 1
        ]
      }}],
    "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

std::string ConfigStr_deduplicate =
    R"delimiter({
      "name": "Deduplicate(keep=[LastRow], key=[$1, $2], order=[ROWTIME])",
      "description": {
        "miniBatchSize": -1,
        "inputTypes": [
          "BIGINT",
          "BIGINT",
          "BIGINT",
          "VARCHAR(2147483647)"
        ],
        "keepLastRow": true,
        "isCompactChanges": false,
        "minRetentionTime": 0,
        "rowtimeIndex": 2,
        "isRowtime": true,
        "generateUpdateBefore": true,
        "outputTypes": [
          "BIGINT",
          "BIGINT",
          "BIGINT",
          "VARCHAR(2147483647)"
        ],
        "generateInsert": true,
        "grouping": [
          0,
          1
        ],
        "originDescription": null
      },
      "id": "org.apache.flink.streaming.api.operators.KeyedProcessOperator"
    })delimiter";

std::string ConfigStr_calc =
    R"delimiter({
    "name" : "Calc(select=[id, category])",
    "description":{
        "originDescription":null,
        "inputTypes":["BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)"],
        "outputTypes":["BIGINT","BIGINT"],
        "indices":[
            {"exprType":"FIELD_REFERENCE","dataType":9,"colVal":0},
            {"exprType":"FIELD_REFERENCE","dataType":9,"colVal":1}],
        "condition":null
    },
        "id":"StreamExecCalc"
    })delimiter";

std::string ConfigStr_sink =
    R"delimiter({
    "name" : "blackHole sink",
    "description":{
        "inputTypes":["BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)"]
    },
        "id":"org.apache.flink.table.runtime.operators.sink.SinkOperator"
    })delimiter";

omnistream::VectorBatch* getBatch1()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,       timestamp
    0     , 1000         , 1,      100
    */

    int rowCount = 1;

    auto vbatch = new omnistream::VectorBatch(rowCount);
    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);

    vKey->SetValue(0, 0);
    // vKey->SetValue(1, 0);
    // vKey->SetValue(2, 0);
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vWindowEndTimeLeft->SetValue(0, 1000);
    // vWindowEndTimeLeft->SetValue(1, 1001);
    // vWindowEndTimeLeft->SetValue(2, 1000);
    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    vValLeft->SetValue(0, 1);
    // vValLeft->SetValue(0, 2);
    // vValLeft->SetValue(0, 3);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
        std::string str = "string1_" + std::to_string(i);
        std::string_view value(str.data(), str.size());
        stringVecLeft->SetValue(i, value);
    }
    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 100);
    // vbatch->setTimestamp(1, 101);
    // vbatch->setTimestamp(2, 102);
    return vbatch;
}

omnistream::VectorBatch* getBatch2()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp
    0     , 1000         , 0,     103
    0     , 1001         , 5,     104
    1     , 1000         , 6,     105
    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 0);
    // for (int j = 0; j < rowCount; j++) {
    //     vKey->SetValue(j, j);
    // }
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int j = 0; j < rowCount; j++) {
        vWindowEndTimeLeft->SetValue(j, 1000 + j);
    }

    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vValLeft->SetValue(0, 9);
    // vValLeft->SetValue(1, 5);
    // vValLeft->SetValue(2, 6);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
        std::string str = "string2_" + std::to_string(i);
        std::string_view value(str.data(), str.size());
        stringVecLeft->SetValue(i, value);
    }
    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 103);
    // vbatch->setTimestamp(1, 104);
    // vbatch->setTimestamp(2, 105);
    return vbatch;
}

omnistream::VectorBatch* getBatch3()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp
    0     , 1000         , 0,     103

    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 1);
    // for (int j = 0; j < rowCount; j++) {
    //     vKey->SetValue(j, j);
    // }
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int j = 0; j < rowCount; j++) {
        vWindowEndTimeLeft->SetValue(j, 1000 + j);
    }

    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vValLeft->SetValue(0, 9);
    // vValLeft->SetValue(1, 5);
    // vValLeft->SetValue(2, 6);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
        std::string str = "string3_" + std::to_string(i);
        std::string_view value(str.data(), str.size());
        stringVecLeft->SetValue(i, value);
    }
    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 106);
    return vbatch;
}

omnistream::VectorBatch* getSameKeyBatch(
    const std::vector<int64_t>& rowtimes, int64_t timestampBase, int64_t key0Value = 7, int64_t key1Value = 7000)
{
    const int32_t rowCount = static_cast<int32_t>(rowtimes.size());
    auto* vbatch = new omnistream::VectorBatch(rowCount);

    auto* key0 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto* key1 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto* rowtime = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto* payload = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(rowCount);
    for (int32_t row = 0; row < rowCount; ++row) {
        key0->SetValue(row, key0Value);
        key1->SetValue(row, key1Value);
        rowtime->SetValue(row, rowtimes[row]);
        std::string value = "rowtime_" + std::to_string(rowtimes[row]);
        std::string_view valueView(value);
        payload->SetValue(row, valueView);
        vbatch->setTimestamp(row, timestampBase + row);
    }
    vbatch->Append(key0);
    vbatch->Append(key1);
    vbatch->Append(rowtime);
    vbatch->Append(payload);
    return vbatch;
}

omnistream::VectorBatch* getOutput1()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp  rowkind
    0     , 1000         , 0,     100      rowkind::insert 0
    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 0);
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    vWindowEndTimeLeft->SetValue(0, 1000);

    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vValLeft->SetValue(0, 1);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);

    std::string str1 = "string1_" + std::to_string(0);
    std::string_view value1(str1.data(), str1.size());
    stringVecLeft->SetValue(0, value1);

    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 100);
    vbatch->setRowKind(0, RowKind::INSERT);

    return vbatch;
}

omnistream::VectorBatch* getOutput2()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp, rowkind
    0     , 1000         , 1,     103     rowkind::UPDATE_BEFORE
    0     , 1000         , 9,     103     RowKind::UPDATE_AFTER
    */

    int rowCount = 2;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 0);
    vKey->SetValue(1, 0);
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    vWindowEndTimeLeft->SetValue(0, 1000);
    vWindowEndTimeLeft->SetValue(1, 1000);

    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vValLeft->SetValue(0, 1);
    vValLeft->SetValue(1, 9);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);

    std::string str1 = "string1_" + std::to_string(0);
    std::string_view value1(str1.data(), str1.size());
    stringVecLeft->SetValue(0, value1);

    std::string str2 = "string2_" + std::to_string(0);
    std::string_view value2(str2.data(), str2.size());
    stringVecLeft->SetValue(1, value2);

    vbatch->Append(stringVecLeft.release());

    // Flink emits UPDATE_BEFORE and UPDATE_AFTER while processing the current
    // record, so both carry the current record timestamp. The previous row's
    // transport timestamp is intentionally not part of ValueState<RowData*>.
    vbatch->setTimestamp(0, 103);
    vbatch->setTimestamp(1, 103);
    // vbatch->setTimestamp(2, 105);
    vbatch->setRowKind(0, RowKind::UPDATE_BEFORE);
    vbatch->setRowKind(1, RowKind::UPDATE_AFTER);
    return vbatch;
}

omnistream::VectorBatch* getOutput3()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp, rowkind
    0     , 1000         , 1,     100     rowkind::UPDATE_BEFORE
    0     , 1000         , 9,     103     RowKind::UPDATE_AFTER
    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 1);
    // for (int j = 0; j < rowCount; j++) {
    //     vKey->SetValue(j, j);
    // }
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int j = 0; j < rowCount; j++) {
        vWindowEndTimeLeft->SetValue(j, 1000 + j);
    }

    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vValLeft->SetValue(0, 9);
    // vValLeft->SetValue(1, 5);
    // vValLeft->SetValue(2, 6);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
        std::string str = "string3_" + std::to_string(i);
        std::string_view value(str.data(), str.size());
        stringVecLeft->SetValue(i, value);
    }
    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 106);
    vbatch->setRowKind(0, RowKind::INSERT);
    return vbatch;
}

omnistream::VectorBatch* getOutput4()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp  rowkind
    0     , 1000         , 0,     103      rowkind::insert 0
    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 0);
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    vWindowEndTimeLeft->SetValue(0, 1000);

    vbatch->Append(vWindowEndTimeLeft);

    // // Value Column
    // auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    // vValLeft->SetValue(0, 1);
    // vbatch->Append(vValLeft);

    // auto stringVecLeft =
    //     std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);

    // std::string str1 = "string1_" + std::to_string(0);
    // std::string_view value1(str1.data(), str1.size());
    // stringVecLeft->SetValue(0, value1);

    // vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 100);
    vbatch->setRowKind(0, RowKind::INSERT);

    return vbatch;
}

omnistream::VectorBatch* getOutput5()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp, rowkind
    0     , 1000         , 1,     100     rowkind::UPDATE_BEFORE
    0     , 1000         , 9,     103     RowKind::UPDATE_AFTER
    */

    int rowCount = 2;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 0);
    vKey->SetValue(1, 0);
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    vWindowEndTimeLeft->SetValue(0, 1000);
    vWindowEndTimeLeft->SetValue(1, 1000);

    vbatch->Append(vWindowEndTimeLeft);

    // // Value Column
    // auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    // vValLeft->SetValue(0, 1);
    // vValLeft->SetValue(1, 9);
    // vbatch->Append(vValLeft);

    // auto stringVecLeft =
    //     std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);

    // std::string str1 = "string1_" + std::to_string(0);
    // std::string_view value1(str1.data(), str1.size());
    // stringVecLeft->SetValue(0, value1);

    // std::string str2 = "string2_" + std::to_string(0);
    // std::string_view value2(str2.data(), str2.size());
    // stringVecLeft->SetValue(1, value2);

    // vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 100);
    vbatch->setTimestamp(1, 103);
    // vbatch->setTimestamp(2, 105);
    vbatch->setRowKind(0, RowKind::UPDATE_BEFORE);
    vbatch->setRowKind(1, RowKind::UPDATE_AFTER);
    return vbatch;
}

omnistream::VectorBatch* getOutput6()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp, rowkind
    0     , 1000         , 1,     100     rowkind::UPDATE_BEFORE
    0     , 1000         , 9,     103     RowKind::UPDATE_AFTER
    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 1);
    // for (int j = 0; j < rowCount; j++) {
    //     vKey->SetValue(j, j);
    // }
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int j = 0; j < rowCount; j++) {
        vWindowEndTimeLeft->SetValue(j, 1000 + j);
    }

    vbatch->Append(vWindowEndTimeLeft);

    // // Value Column
    // auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    // vValLeft->SetValue(0, 9);
    // // vValLeft->SetValue(1, 5);
    // // vValLeft->SetValue(2, 6);
    // vbatch->Append(vValLeft);

    // auto stringVecLeft =
    //     std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    // for (int i = 0; i < rowCount; i++) {
    //     std::string str = "string3_" + std::to_string(i);
    //     std::string_view value(str.data(), str.size());
    //     stringVecLeft->SetValue(i, value);
    // }
    // vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 106);
    vbatch->setRowKind(0, RowKind::INSERT);
    return vbatch;
}

template <typename T>
bool compareCol(omniruntime::vec::BaseVector* col1, omniruntime::vec::BaseVector* col2)
{
    if (col1->GetSize() != col2->GetSize()) {
        std::cout << "size wrong" << std::endl;
        return false;
    }

    if (col1->GetTypeId() != col2->GetTypeId()) {
        std::cout << "id wrong" << std::endl;
        return false;
    }

    omniruntime::vec::Vector<T>* col1cast = static_cast<omniruntime::vec::Vector<T>*>(col1);
    omniruntime::vec::Vector<T>* col2cast = static_cast<omniruntime::vec::Vector<T>*>(col2);

    if (col1cast->GetNullCount() != col2cast->GetNullCount()) {
        std::cout << "null count wrong" << std::endl;
        return false;
    }

    for (int i = 0; i < col1cast->GetSize(); i++) {
        auto c1v = col1cast->GetValue(i);
        auto c2v = col2cast->GetValue(i);
        auto c1null = col1cast->IsNull(i);
        auto c2null = col2cast->IsNull(i);
        if (c1null == c2null && c1null == true) {
            return true;
        }

        if (c1v != c2v) {
            std::cout << "value1 is: " << c1v << " value2 is:" << c2v << std::endl;
            return false;
        }
    }
    return true;
};

bool batchesEqual(omnistream::VectorBatch* b1, omnistream::VectorBatch* b2)
{
    if (b1->GetVectorCount() != b2->GetVectorCount()) {
        std::cout << "b1 count is: " << b1->GetVectorCount() << " b2 count is: " << b2->GetVectorCount() << std::endl;
        return false;
    }

    if (b1->GetRowCount() != b2->GetRowCount()) {
        return false;
    }

    // compare rowkind
    for (int i = 0; i < b1->GetRowCount(); i++) {
        if ((int)b1->getRowKind(i) != (int)b2->getRowKind(i)) {
            std::cout << "rowkind wrong b1: " << (int)b1->getRowKind(i) << " b2: " << (int)b2->getRowKind(i)
                      << std::endl;
            return false;
        }
    }

    // compare timestamp
    for (int i = 0; i < b1->GetRowCount(); i++) {
        if ((int)b1->getTimestamp(i) != (int)b2->getTimestamp(i)) {
            std::cout << "timestamp wrong b1: " << (int)b1->getRowKind(i) << " b2: " << (int)b2->getRowKind(i)
                      << std::endl;
            return false;
        }
    }

    return compareCol<int64_t>(b1->Get(0), b2->Get(0)) && compareCol<int64_t>(b1->Get(1), b2->Get(1)) &&
           compareCol<int64_t>(b1->Get(2), b2->Get(2)) &&
           compareCol<omniruntime::vec::LargeStringContainer<std::string_view>>(b1->Get(3), b2->Get(3));
};

bool batchesEqual2(omnistream::VectorBatch* b1, omnistream::VectorBatch* b2)
{
    if (b1->GetVectorCount() != b2->GetVectorCount()) {
        std::cout << "b1 count is: " << b1->GetVectorCount() << " b2 count is: " << b2->GetVectorCount() << std::endl;
        return false;
    }

    if (b1->GetRowCount() != b2->GetRowCount()) {
        return false;
    }

    // compare rowkind
    for (int i = 0; i < b1->GetRowCount(); i++) {
        if ((int)b1->getRowKind(i) != (int)b2->getRowKind(i)) {
            std::cout << "rowkind wrong b1: " << (int)b1->getRowKind(i) << " b2: " << (int)b2->getRowKind(i)
                      << std::endl;
            return false;
        }
    }

    // compare timestamp
    for (int i = 0; i < b1->GetRowCount(); i++) {
        if ((int)b1->getTimestamp(i) != (int)b2->getTimestamp(i)) {
            std::cout << "timestamp wrong b1: " << (int)b1->getRowKind(i) << " b2: " << (int)b2->getRowKind(i)
                      << std::endl;
            return false;
        }
    }

    return compareCol<int64_t>(b1->Get(0), b2->Get(0)) && compareCol<int64_t>(b1->Get(1), b2->Get(1));
};

TEST(RowTimeDeduplicateTest, UpdateBeforeKeepLastRowTimeTest)
{
    auto inputVB1 = getBatch1();
    auto inputVB2 = getBatch2();
    auto inputVB3 = getBatch3();

    // VectorBatchUtil::printVectorBatch(inputVB2);

    json parsedJson = json::parse(description);
    OperatorConfig opConfig(
        uniqueName,                                                   // uniqueName:
        "Deduplicate(keep=[LastRow], key=[$1, $2], order=[ROWTIME])", // Name
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest* output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;

    auto* keyedOp = dynamic_cast<KeyedProcessOperator<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*>*>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));

    // keyedOp->setup();

    // initiate keyedState
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
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedOp->initializeState(initializer, ser);
    keyedOp->open();
    keyedOp->processBatch(new StreamRecord(inputVB1));

    // 状态后端为空，插入测试
    omnistream::VectorBatch* outputVB1_1 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    auto outputVB1 = getOutput1();
    EXPECT_TRUE(batchesEqual(outputVB1, outputVB1_1));

    keyedOp->processBatch(new StreamRecord(inputVB2));
    // 插入重复数据，回撤测试
    omnistream::VectorBatch* outputVB2_2 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    auto outputVB2 = getOutput2();
    EXPECT_TRUE(batchesEqual(outputVB2, outputVB2_2));

    // 状态后端有数据，插入测试
    keyedOp->processBatch(new StreamRecord(inputVB3));
    omnistream::VectorBatch* outputVB3_3 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    auto outputVB3 = getOutput3();
    EXPECT_TRUE(batchesEqual(outputVB3, outputVB3_3));

    std::cout << "finish test" << std::endl;

    delete outputVB1;
    delete outputVB2;
    delete outputVB3;
}

TEST(RowTimeDeduplicateTest, KeepLastSelectsSingleWinnerForSameKeyWithinBatch)
{
    json parsedJson = json::parse(description);
    OperatorConfig opConfig(
        uniqueName,
        "Deduplicate(keep=[LastRow], key=[$1, $2], order=[ROWTIME])",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    auto* output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto* keyedOp = dynamic_cast<KeyedProcessOperator<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*>*>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));
    ASSERT_NE(keyedOp, nullptr);

    auto* env = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env);
    auto* keyFields = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    auto* keySerializer = new RowDataSerializer(new omnistream::RowType(false, *keyFields));

    keyedOp->initializeState(initializer, keySerializer);
    keyedOp->open();

    // KeepLast resolves an equal maximum rowtime in favor of the later row.
    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({3, 7, 7, 5}, 100)));
    auto* firstOutput = output->getVectorBatch();
    ASSERT_NE(firstOutput, nullptr);
    ASSERT_EQ(firstOutput->GetRowCount(), 1);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(firstOutput->Get(2))->GetValue(0), 7);
    EXPECT_EQ(firstOutput->getTimestamp(0), 102);
    EXPECT_EQ(firstOutput->getRowKind(0), RowKind::INSERT);

    // An older row in a later batch must not replace the rowtime=7 state.
    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({6}, 200)));
    auto* secondOutput = output->getVectorBatch();
    ASSERT_NE(secondOutput, nullptr);
    EXPECT_EQ(secondOutput->GetRowCount(), 0);

    // Across batches KeepLast also treats an equal rowtime as a replacement.
    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({7}, 300)));
    auto* equalOutput = output->getVectorBatch();
    ASSERT_NE(equalOutput, nullptr);
    ASSERT_EQ(equalOutput->GetRowCount(), 2);
    EXPECT_EQ(equalOutput->getRowKind(0), RowKind::UPDATE_BEFORE);
    EXPECT_EQ(equalOutput->getRowKind(1), RowKind::UPDATE_AFTER);
    EXPECT_EQ(equalOutput->getTimestamp(0), 300);
    EXPECT_EQ(equalOutput->getTimestamp(1), 300);
}

TEST(RowTimeDeduplicateTest, KeepFirstSelectsSingleWinnerForSameKeyWithinBatch)
{
    json parsedJson = json::parse(description);
    parsedJson["operators"][0]["description"]["keepLastRow"].get_ref<json::boolean_t&>() = false;
    OperatorConfig opConfig(
        uniqueName,
        "Deduplicate(keep=[FirstRow], key=[$1, $2], order=[ROWTIME])",
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    auto* output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto* keyedOp = dynamic_cast<KeyedProcessOperator<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*>*>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));
    ASSERT_NE(keyedOp, nullptr);

    auto* env = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env);
    auto* keyFields = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    auto* keySerializer = new RowDataSerializer(new omnistream::RowType(false, *keyFields));

    keyedOp->initializeState(initializer, keySerializer);
    keyedOp->open();

    // KeepFirst resolves an equal minimum rowtime in favor of the earlier row.
    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({3, 3, 7, 5}, 100)));
    auto* firstOutput = output->getVectorBatch();
    ASSERT_NE(firstOutput, nullptr);
    ASSERT_EQ(firstOutput->GetRowCount(), 1);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(firstOutput->Get(2))->GetValue(0), 3);
    EXPECT_EQ(firstOutput->getTimestamp(0), 100);
    EXPECT_EQ(firstOutput->getRowKind(0), RowKind::INSERT);

    // A later row in another batch must not replace the rowtime=3 state.
    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({4}, 200)));
    auto* secondOutput = output->getVectorBatch();
    ASSERT_NE(secondOutput, nullptr);
    EXPECT_EQ(secondOutput->GetRowCount(), 0);

    // Across batches KeepFirst also keeps the existing row on an equal rowtime.
    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({3}, 300)));
    auto* equalOutput = output->getVectorBatch();
    ASSERT_NE(equalOutput, nullptr);
    EXPECT_EQ(equalOutput->GetRowCount(), 0);
}

TEST(RowTimeDeduplicateTest, UpdateAfterOnlyModeDoesNotEmitInsertOrUpdateBefore)
{
    json parsedJson = json::parse(description);
    auto& operatorDescription = parsedJson["operators"][0]["description"];
    operatorDescription["generateUpdateBefore"].get_ref<json::boolean_t&>() = false;
    operatorDescription["generateInsert"].get_ref<json::boolean_t&>() = false;
    OperatorConfig opConfig(
        uniqueName,
        "Deduplicate(keep=[LastRow], key=[$1, $2], order=[ROWTIME])",
        operatorDescription["inputTypes"],
        operatorDescription["outputTypes"],
        operatorDescription);

    auto* output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto* keyedOp = dynamic_cast<KeyedProcessOperator<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*>*>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));
    ASSERT_NE(keyedOp, nullptr);

    auto* env = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env);
    auto* keyFields = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    auto* keySerializer = new RowDataSerializer(new omnistream::RowType(false, *keyFields));

    keyedOp->initializeState(initializer, keySerializer);
    keyedOp->open();

    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({3}, 100)));
    auto* insertOutput = output->getVectorBatch();
    ASSERT_NE(insertOutput, nullptr);
    ASSERT_EQ(insertOutput->GetRowCount(), 1);
    EXPECT_EQ(insertOutput->getRowKind(0), RowKind::UPDATE_AFTER);
    EXPECT_EQ(insertOutput->getTimestamp(0), 100);

    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({4}, 200)));
    auto* updateOutput = output->getVectorBatch();
    ASSERT_NE(updateOutput, nullptr);
    ASSERT_EQ(updateOutput->GetRowCount(), 1);
    EXPECT_EQ(updateOutput->getRowKind(0), RowKind::UPDATE_AFTER);
    EXPECT_EQ(updateOutput->getTimestamp(0), 200);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(updateOutput->Get(2))->GetValue(0), 4);
}

TEST(RowTimeDeduplicateTest, RocksDBWithoutCacheStoresAndReadsSelectedRowData)
{
    json parsedJson = json::parse(description);
    auto& operatorDescription = parsedJson["operators"][0]["description"];
    OperatorConfig opConfig(
        uniqueName,
        "Deduplicate(keep=[LastRow], key=[$1, $2], order=[ROWTIME])",
        operatorDescription["inputTypes"],
        operatorDescription["outputTypes"],
        operatorDescription);

    auto* output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto* keyedOp = dynamic_cast<KeyedProcessOperator<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*>*>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));
    ASSERT_NE(keyedOp, nullptr);

    const auto rocksPath = std::filesystem::temp_directory_path() / "rowtime-deduplicate-rocksdb-no-cache";
    std::filesystem::remove_all(rocksPath);

    auto* env = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("EmbeddedRocksDBStateBackend");
    env->setRocksDBSharedResources(
        RocksDBMemoryControllerUtils::allocateRocksDBSharedResources(32 * 1024 * 1024, 0.5, 0.1, false));
    taskInfo->setRocksdbStorePaths({rocksPath.string()});
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    auto jobId = omnistream::JobIDPOD(1, 2);
    auto jobVertexId = omnistream::JobVertexID(3, 4);
    auto attemptId = omnistream::ExecutionAttemptIDPOD::randomId();
    auto* stateStore = new TaskLocalStateStore(jobId, jobVertexId, 0, nullptr);
    auto* responder = new omnistream::NoOpCheckpoingResponder();
    env->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>(jobId, attemptId, stateStore, responder));
    env->setTaskConfiguration(*taskInfo);
    auto* rocksBackend = new EmbeddedRocksDBStateBackend(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(rocksBackend, env);
    auto* keyFields = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    auto* keySerializer = new RowDataSerializer(new omnistream::RowType(false, *keyFields));

    keyedOp->initializeState(initializer, keySerializer);
    keyedOp->open();

    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({3}, 100)));
    auto* insertOutput = output->getVectorBatch();
    ASSERT_NE(insertOutput, nullptr);
    ASSERT_EQ(insertOutput->GetRowCount(), 1);
    EXPECT_EQ(insertOutput->getRowKind(0), RowKind::INSERT);

    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({4}, 200)));
    auto* updateOutput = output->getVectorBatch();
    ASSERT_NE(updateOutput, nullptr);
    ASSERT_EQ(updateOutput->GetRowCount(), 2);
    EXPECT_EQ(updateOutput->getRowKind(0), RowKind::UPDATE_BEFORE);
    EXPECT_EQ(updateOutput->getRowKind(1), RowKind::UPDATE_AFTER);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(updateOutput->Get(2))->GetValue(1), 4);

    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({2}, 300)));
    auto* olderOutput = output->getVectorBatch();
    ASSERT_NE(olderOutput, nullptr);
    EXPECT_EQ(olderOutput->GetRowCount(), 0);
}

TEST(RowTimeDeduplicateTest, RocksDBCacheMissReadsAndUpdatesSelectedRowData)
{
    struct FalconConfigGuard {
        FalconConfigGuard()
        {
            Configuration::TM_CONFIG->setConfiguration(
                {{"state.backend.rocksdb.falcon.use-state-cache", "true"},
                 {"state.backend.rocksdb.falcon.state-cache-sizeLimit", "1"}});
        }
        ~FalconConfigGuard()
        {
            Configuration::TM_CONFIG->setConfiguration(
                {{"state.backend.rocksdb.falcon.use-state-cache", "false"},
                 {"state.backend.rocksdb.falcon.state-cache-sizeLimit", "3000"}});
        }
    } falconConfigGuard;

    json parsedJson = json::parse(description);
    auto& operatorDescription = parsedJson["operators"][0]["description"];
    OperatorConfig opConfig(
        uniqueName,
        "Deduplicate(keep=[LastRow], key=[$1, $2], order=[ROWTIME])",
        operatorDescription["inputTypes"],
        operatorDescription["outputTypes"],
        operatorDescription);

    auto* output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;
    auto* keyedOp = dynamic_cast<KeyedProcessOperator<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*>*>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));
    ASSERT_NE(keyedOp, nullptr);

    const auto rocksPath = std::filesystem::temp_directory_path() / "rowtime-deduplicate-rocksdb-cache";
    std::filesystem::remove_all(rocksPath);

    auto* env = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("EmbeddedRocksDBStateBackend");
    env->setRocksDBSharedResources(
        RocksDBMemoryControllerUtils::allocateRocksDBSharedResources(32 * 1024 * 1024, 0.5, 0.1, false));
    taskInfo->setRocksdbStorePaths({rocksPath.string()});
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    auto jobId = omnistream::JobIDPOD(5, 6);
    auto jobVertexId = omnistream::JobVertexID(7, 8);
    auto attemptId = omnistream::ExecutionAttemptIDPOD::randomId();
    auto* stateStore = new TaskLocalStateStore(jobId, jobVertexId, 0, nullptr);
    auto* responder = new omnistream::NoOpCheckpoingResponder();
    env->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>(jobId, attemptId, stateStore, responder));
    env->setTaskConfiguration(*taskInfo);
    auto* rocksBackend = new EmbeddedRocksDBStateBackend(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(rocksBackend, env);
    auto* keyFields = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    auto* keySerializer = new RowDataSerializer(new omnistream::RowType(false, *keyFields));

    keyedOp->initializeState(initializer, keySerializer);
    keyedOp->open();

    auto* function = dynamic_cast<RowTimeDeduplicateFunction*>(keyedOp->getUserFunction());
    ASSERT_NE(function, nullptr);
    auto* rocksState = dynamic_cast<RocksdbValueState<RowData*, VoidNamespace, RowData*>*>(function->getValueState());
    ASSERT_NE(rocksState, nullptr);
    ASSERT_EQ(rocksState->stateCache->getSizeLimit(), 1);

    // Insert key A, then key B. With a cache limit of one this evicts cached A.
    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({3}, 100, 7, 7000)));
    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({5}, 200, 8, 8000)));

    // Reading A again must miss the cache, deserialize RowData from RocksDB,
    // and then update the stored winner from rowtime 3 to rowtime 4.
    keyedOp->processBatch(new StreamRecord(getSameKeyBatch({4}, 300, 7, 7000)));
    auto* updateOutput = output->getVectorBatch();
    ASSERT_NE(updateOutput, nullptr);
    ASSERT_EQ(updateOutput->GetRowCount(), 2);
    EXPECT_EQ(updateOutput->getRowKind(0), RowKind::UPDATE_BEFORE);
    EXPECT_EQ(updateOutput->getRowKind(1), RowKind::UPDATE_AFTER);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(updateOutput->Get(2))->GetValue(0), 3);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(updateOutput->Get(2))->GetValue(1), 4);

    auto probeBatch = std::unique_ptr<omnistream::VectorBatch>(getSameKeyBatch({4}, 400, 7, 7000));
    KeySelector<RowData*> selector({OMNI_LONG, OMNI_LONG}, {0, 1});
    auto probeKey = std::unique_ptr<RowData>(selector.getKey(probeBatch.get(), 0));
    keyedOp->setCurrentKey(probeKey.get());
    rocksState->stateCache->flush();
    rocksState->stateCache->clearAll();
    EXPECT_FALSE(rocksState->stateCache->contains(probeKey.get(), rocksState->getNamespace()));
    std::unique_ptr<RowData> coldRow(rocksState->value());
    ASSERT_NE(coldRow, nullptr);
    // The baseline Falcon key compares raw RowData pointers by address. A lookup
    // copies the key, so this path remains a miss even for the same logical key.
    EXPECT_FALSE(rocksState->stateCache->contains(probeKey.get(), rocksState->getNamespace()));
    std::unique_ptr<RowData> nextColdRow(rocksState->value());
    ASSERT_NE(nextColdRow, nullptr);
    EXPECT_NE(coldRow.get(), nextColdRow.get());
    EXPECT_FALSE(rocksState->stateCache->contains(probeKey.get(), rocksState->getNamespace()));
}

namespace {

int64_t LiveHeapBytes()
{
#if defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 33)
    struct mallinfo2 info = mallinfo2();
    if (info.uordblks == 0) {
        return -1;
    }
    return static_cast<int64_t>(info.uordblks);
#else
    return -1;
#endif
#else
    return -1;
#endif
}

} // namespace

TEST(RowTimeDeduplicateTest, DestructorReleasesKeySelector)
{
    json config = json::parse(description)["operators"][0]["description"];

    const int warmupIterations = 1000;
    const int measuredIterations = 20000;
    const int64_t allowedGrowthBytes = 256 * 1024;
    volatile uintptr_t sink = 0;

    for (int i = 0; i < warmupIterations; i++) {
        auto func = std::make_unique<RowTimeDeduplicateFunction>(config);
        sink ^= reinterpret_cast<uintptr_t>(func.get());
    }

    int64_t before = LiveHeapBytes();
    if (before < 0) {
        GTEST_SKIP() << "allocator does not report live heap bytes";
    }

    for (int i = 0; i < measuredIterations; i++) {
        auto func = std::make_unique<RowTimeDeduplicateFunction>(config);
        sink ^= reinterpret_cast<uintptr_t>(func.get());
    }

    int64_t growth = LiveHeapBytes() - before;
    EXPECT_LT(growth, allowedGrowthBytes)
        << "heap grew " << growth << " bytes over " << measuredIterations << " create/destroy cycles ("
        << (growth / measuredIterations) << " bytes per instance)";
}

TEST(RowTimeDeduplicateTest, FunctionIsNonCopyable)
{
    static_assert(
        !std::is_copy_constructible<RowTimeDeduplicateFunction>::value,
        "RowTimeDeduplicateFunction owns its KeySelector and must not be copied");
    static_assert(
        !std::is_copy_assignable<RowTimeDeduplicateFunction>::value,
        "RowTimeDeduplicateFunction owns its KeySelector and must not be copy-assigned");
    SUCCEED();
}
