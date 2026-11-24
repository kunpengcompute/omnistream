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
#include <memory>
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/keyselector/KeySelector.h"
#include <OmniOperatorJIT/core/test/util/test_util.h>
#include <utility>

TEST(KeySelectorTest, SelectFromVB)
{
    // Create the input VB
    auto rawVB = std::make_unique<omniruntime::vec::VectorBatch>(5);
    std::vector<long> col0{0, 1, 2, 3, 4};
    std::vector<std::string> col1{"hello_world_0", "hello_world_1", "hello_world_2", "hello_world_3", "hello_world_4"};
    std::vector<int> col2{20, 21, 22, 23, 24};
    std::vector<long> col3{1744053374, 1744053375, 1744053376, 1744053377, 1744053378};
    BaseVector* vec = omniruntime::TestUtil::CreateVector(col0.size(), col0.data());
    rawVB->Append(vec);
    vec = omniruntime::TestUtil::CreateVarcharVector(col1.data(), col1.size());
    rawVB->Append(vec);
    vec = omniruntime::TestUtil::CreateVector(col2.size(), col2.data());
    rawVB->Append(vec);
    vec = omniruntime::TestUtil::CreateVector(col3.size(), col3.data());
    rawVB->Append(vec);
    omnistream::VectorBatch vb(std::move(rawVB), nullptr, nullptr);

    // build a new row as key
    std::vector<int> keyCols{0, 1, 2, 3};
    std::vector<int> keyTypes{
        omniruntime::type::OMNI_LONG,
        omniruntime::type::OMNI_VARCHAR,
        omniruntime::type::OMNI_INT,
        omniruntime::type::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE};
    KeySelector<StringRef> selector(keyTypes, keyCols);
    auto key = selector.getKey(&vb, 3);
    // The expected key is false, 3, false, "hello_world_3", false, 23, false, 43
    char* expected = new char[24]{};
    // The long
    memset(expected, 1, 1); // How many bytes (1)
    memcpy(expected + 1, col0.data() + 3, 1);

    // The varchar
    memset(expected + 2, 1, 1);  // How many bytes to store length (1)
    memset(expected + 3, 13, 1); // string length (13)
    memcpy(expected + 4, col1[3].data(), col1[3].size());

    // The int
    memset(expected + 17, 1, 1);               // How many bytes (1)
    memcpy(expected + 18, col2.data() + 3, 1); // val

    // The timestamp
    memset(expected + 19, 4, 1);               // How many bytes (4)
    memcpy(expected + 20, col3.data() + 3, 4); // val

    StringRef expectedKey(expected, 24);
    EXPECT_TRUE(expectedKey == key);
}

TEST(KeySelectorTest, DeserializeToVB)
{
    // Create the input VB
    auto rawVB = std::make_unique<omniruntime::vec::VectorBatch>(1);
    auto vec0 = new omniruntime::vec::Vector<int64_t>(1);
    auto vec1 = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(1);
    auto vec2 = new omniruntime::vec::Vector<int32_t>(1);
    auto vec3 = new omniruntime::vec::Vector<int64_t>(1);
    rawVB->Append(vec0);
    rawVB->Append(vec1);
    rawVB->Append(vec2);
    rawVB->Append(vec3);
    omnistream::VectorBatch vb(std::move(rawVB), nullptr, nullptr);
    long key0 = 3;
    std::string key1 = "hello_world_3";
    int key2 = 23;
    long key3 = 1744053374;
    // The expected key is false, 3, false, "hello_world_3", false, 23, false, 43
    char* key = new char[24]{};
    // The long
    memset(key, 1, 1); // How many bytes (1)
    memcpy(key + 1, &key0, 1);

    // The varchar
    memset(key + 2, 1, 1);  // How many bytes to store length (1)
    memset(key + 3, 13, 1); // string length (13)
    memcpy(key + 4, key1.data(), key1.size());

    // The int
    memset(key + 17, 1, 1);     // How many bytes (1)
    memcpy(key + 18, &key2, 1); // val

    // The timestamp
    memset(key + 19, 4, 1);     // How many bytes (4)
    memcpy(key + 20, &key3, 4); // val
    StringRef keyRef(key, 24);

    std::vector<int> keyCols{0, 1, 2, 3};
    std::vector<int> keyTypes{
        omniruntime::type::OMNI_LONG,
        omniruntime::type::OMNI_VARCHAR,
        omniruntime::type::OMNI_INT,
        omniruntime::type::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE};
    KeySelector<StringRef> selector(keyTypes, keyCols);

    selector.fillKeyToVectorBatch(keyRef, &vb, 0, {0, 1, 2, 3});

    std::cout
        << vb.GetValueAt<int64_t>(0, 0) << " "
        << std::string(
               reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                   vb.Get(1))
                   ->GetValue(0))
        << " " << vb.GetValueAt<int32_t>(2, 0) << " " << vb.GetValueAt<int64_t>(3, 0) << std::endl;
    EXPECT_TRUE(vb.GetValueAt<int64_t>(0, 0) == key0);
    EXPECT_TRUE(
        std::string(
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                vb.Get(1))
                ->GetValue(0)) == key1);
    EXPECT_TRUE(vb.GetValueAt<int32_t>(2, 0) == key2);
    EXPECT_TRUE(vb.GetValueAt<int64_t>(3, 0) == key3);
}

TEST(KeySelectorTest, BinaryRowStringRefMatchesVectorBatchEncoding)
{
    const std::vector<omniruntime::type::DataTypeId> types = {OMNI_LONG, OMNI_VARCHAR, OMNI_INT};
    auto batch = std::unique_ptr<omnistream::VectorBatch>(omnistream::VectorBatch::CreateVectorBatch(1, types));
    static_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(0))->SetValue(0, 42);
    std::string_view keyValue = "key";
    static_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(batch->Get(1))
        ->SetValue(0, keyValue);
    batch->Get(2)->SetNull(0);
    auto row = std::unique_ptr<RowData>(batch->extractRowData(0));
    KeySelector<omniruntime::type::StringRef> vectorSelector({OMNI_LONG, OMNI_VARCHAR, OMNI_INT}, {0, 1, 2});
    KeySelector<omniruntime::type::StringRef> rowSelector({OMNI_LONG, OMNI_VARCHAR, OMNI_INT}, {0, 1, 2});
    EXPECT_TRUE(vectorSelector.getKey(batch.get(), 0) == rowSelector.getKey(row.get()));
}
