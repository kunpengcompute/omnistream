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
#include "table/runtime/operators/rank/Top1Comparator.h"
#include "table/data/vectorbatch/VectorBatch.h"

// Mock implementation of getVectorBatch for creating a VectorBatch.
omnistream::VectorBatch* getVectorBatch()
{
    auto vbatch = new omnistream::VectorBatch(9);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(9);
    for (int j = 0; j < 9; j++) {
        vKey->SetValue(j, j % 3);
    }
    vbatch->Append(vKey);

    // Price Column
    auto vPrice = new omniruntime::vec::Vector<int64_t>(9);
    auto vDate = new omniruntime::vec::Vector<int64_t>(9);
    for (int j = 0; j < 9; j++) {
        if (j % 2 == 0) {
            vPrice->SetValue(j, 1000);
            vDate->SetValue(j, 200);
        } else {
            vPrice->SetValue(j, 2000);
            vDate->SetValue(j, 100);
        }
    }
    vDate->SetValue(6, 150);
    vDate->SetValue(8, 300);
    vbatch->Append(vPrice);
    vbatch->Append(vDate);

    return vbatch;
}

// Test: Basic functionality.
TEST(Top1ComparatorTest, BasicFunctionality)
{
    std::vector<int> sortColumnIds = {1}; // Sort by Price column.
    std::vector<bool> ascending = {true}; // Descending order.

    omnistream::VectorBatch* vectorBatch = getVectorBatch();
    std::vector<int32_t> pkTypes = {omniruntime::type::OMNI_LONG};
    std::vector<int> pk = {0};
    auto* comparator = new Top1Comparator<int64_t>(pkTypes, pk, sortColumnIds, ascending);
    auto top1RowIds = comparator->findTop1RowIdsByPartitionV2(vectorBatch);

    // Check results for each partition (KeyCol).
    EXPECT_EQ(top1RowIds[0], 0);
    EXPECT_EQ(top1RowIds[1], 4);
    EXPECT_EQ(top1RowIds[2], 2);

    delete comparator;

    ascending[0] = false;
    comparator = new Top1Comparator<int64_t>(pkTypes, pk, sortColumnIds, ascending);
    top1RowIds = comparator->findTop1RowIdsByPartitionV2(vectorBatch);

    EXPECT_EQ(top1RowIds[0], 3);
    EXPECT_EQ(top1RowIds[1], 1);
    EXPECT_EQ(top1RowIds[2], 5);

    delete comparator;
    delete vectorBatch;
}

// Test: Tie-breaking with multiple sort columns.
TEST(Top1ComparatorTest, TieBreaking)
{
    std::vector<int> sortColumnIds = {1, 2}; // Sort by Price, then by Date.
    std::vector<bool> ascending = {true, false};

    omnistream::VectorBatch* vectorBatch = getVectorBatch();
    std::vector<int32_t> pkTypes = {omniruntime::type::OMNI_LONG};
    std::vector<int> pk = {0};
    auto* comparator = new Top1Comparator<int64_t>(pkTypes, pk, sortColumnIds, ascending);
    auto top1RowIds = comparator->findTop1RowIdsByPartitionV2(vectorBatch);

    // Check results for each partition (KeyCol), resolving ties with Date.
    EXPECT_EQ(top1RowIds[0], 0);
    EXPECT_EQ(top1RowIds[1], 4);
    EXPECT_EQ(top1RowIds[2], 8);

    delete comparator;
    delete vectorBatch;
}

// Test: Empty batch.
TEST(Top1ComparatorTest, EmptyBatch)
{
    std::vector<int> sortColumnIds = {1, 2}; // Sort by Price, then by Date.
    std::vector<bool> ascending = {true, false};
    // Create an empty VectorBatch.
    auto emptyBatch = new omnistream::VectorBatch(3);
    std::vector<int32_t> pkTypes = {omniruntime::type::OMNI_LONG};
    std::vector<int> pk = {0};
    auto* comparator = new Top1Comparator<RowData*>(pkTypes, pk, sortColumnIds, ascending);

    auto top1RowIds = comparator->findTop1RowIdsByPartition(emptyBatch);

    EXPECT_TRUE(top1RowIds.empty()); // Should return an empty result.
    delete emptyBatch;
}

//// Test: All equal values.
TEST(Top1ComparatorTest, AllEqualValues)
{
    omnistream::VectorBatch* vectorBatch = getVectorBatch();
    auto priceColumn = dynamic_cast<omniruntime::vec::Vector<int64_t>*>(vectorBatch->Get(1));
    for (int j = 0; j < 9; j++) {
        priceColumn->SetValue(j, 1000); // Set all prices to the same value.
    }

    std::vector<int> sortColumnIds = {1};  // Sort by Price.
    std::vector<bool> ascending = {false}; // Descending order.
    std::vector<int32_t> pkTypes = {omniruntime::type::OMNI_LONG};
    std::vector<int> pk = {0};
    auto* comparator = new Top1Comparator<int64_t>(pkTypes, pk, sortColumnIds, ascending);

    auto top1RowIds = comparator->findTop1RowIdsByPartition(vectorBatch);

    // With equal values, the first row in each partition should be selected.
    EXPECT_EQ(top1RowIds[0], 0); // Key 0, first row in partition.
    EXPECT_EQ(top1RowIds[1], 1); // Key 1, first row in partition.
    EXPECT_EQ(top1RowIds[2], 2); // Key 2, first row in partition.
}

/*
    row| key0	key1	Col2	Col3	Col4	Col5	Col6
    0  | 1000	1001	200	    300	    400	    500	    600
    1  | 1002	1003	201	    301	    401	    501 	601
    2  | 1000	1002	202	    302	    402 	502	    602
    3  | 1002	1003	200	    300	    400 	500	    600
    4  | 1000	1002	201	    301	    401 	501	    601
    5  | 1004	1005	202	    302	    402 	502	    602
    6  | 1004	1005	200	    300	    400 	500	    600
    7  | 1000	1002	201	    301	    401 	501     601
*/
omnistream::VectorBatch* getTwoKeyVectorBatch()
{
    constexpr int rows = 8;
    constexpr int cols = 7;
    auto vBatch = new omnistream::VectorBatch(rows);

    auto key0 = new omniruntime::vec::Vector<int64_t>(rows);
    auto key1 = new omniruntime::vec::Vector<int64_t>(rows);

    std::vector<std::pair<int64_t, int64_t>> keys = {
        {1000, 1001}, {1002, 1003}, {1000, 1002}, {1002, 1003}, {1000, 1002}, {1004, 1005}, {1004, 1005}, {1000, 1002}};

    for (int i = 0; i < rows; ++i) {
        key0->SetValue(i, keys[i].first);
        key1->SetValue(i, keys[i].second);
    }
    vBatch->Append(key0);
    vBatch->Append(key1);

    for (int col = 2; col < cols; ++col) {
        auto vec = new omniruntime::vec::Vector<int64_t>(rows);
        for (int i = 0; i < rows; ++i) {
            vec->SetValue(i, col * 100 + (i % 3));
        }
        vBatch->Append(vec);
    }

    return vBatch;
}

TEST(Top1ComparatorTest, BasicFunctionalityWithTwoPKeys)
{
    std::vector<int> sortColumnIds = {5}; // Sort by 5th column.
    std::vector<bool> ascending = {true}; // top1 has the smallest element in the sortColumn

    omnistream::VectorBatch* vectorBatch = getTwoKeyVectorBatch();
    std::vector<int32_t> pkTypes = {omniruntime::type::OMNI_LONG, omniruntime::type::OMNI_LONG};
    std::vector<int32_t> pk = {0, 1};

    // Create comparator
    auto* comparator = new Top1Comparator<RowData*>(pkTypes, pk, sortColumnIds, ascending);
    // template K = BinaryRowData*
    auto top1RowIds = comparator->findTop1RowIdsByPartitionV2(vectorBatch);

    /*  top1RowIds
        (1000, 1001), 0
        (1002, 1003), 3
        (1000, 1002), 4
        (1004, 1005), 6
    */

    for (auto& [keyPtr, rowId] : top1RowIds) {
        long* p0 = keyPtr->getLong(0);
        ASSERT_NE(p0, nullptr);
        int64_t k0 = *p0;

        long* p1 = keyPtr->getLong(1);
        ASSERT_NE(p1, nullptr);
        int64_t k1 = *p1;

        if (k0 == 1000 && k1 == 1001) {
            EXPECT_EQ(rowId, 0);
        } else if (k0 == 1002 && k1 == 1003) {
            EXPECT_EQ(rowId, 3);
        } else if (k0 == 1000 && k1 == 1002) {
            EXPECT_EQ(rowId, 4);
        } else if (k0 == 1004 && k1 == 1005) {
            EXPECT_EQ(rowId, 6);
        } else {
            FAIL() << "Unexpected partition key combination: (" << k0 << "," << k1 << ")";
        }
    }

    delete comparator;
    ascending[0] = false; // top1 has the largest element in the sortColumn
    // Create another comparator
    comparator = new Top1Comparator<RowData*>(pkTypes, pk, sortColumnIds, ascending);
    // template K = BinaryRowData*
    top1RowIds = comparator->findTop1RowIdsByPartitionV2(vectorBatch);

    /*  top1RowIds
        (1000, 1001), 0
        (1002, 1003), 1
        (1000, 1002), 2
        (1004, 1005), 5
    */

    for (auto& [keyPtr, rowId] : top1RowIds) {
        long* p0 = keyPtr->getLong(0);
        ASSERT_NE(p0, nullptr);
        int64_t k0 = *p0;

        long* p1 = keyPtr->getLong(1);
        ASSERT_NE(p1, nullptr);
        int64_t k1 = *p1;

        if (k0 == 1000 && k1 == 1001) {
            EXPECT_EQ(rowId, 0);
        } else if (k0 == 1002 && k1 == 1003) {
            EXPECT_EQ(rowId, 1);
        } else if (k0 == 1000 && k1 == 1002) {
            EXPECT_EQ(rowId, 2);
        } else if (k0 == 1004 && k1 == 1005) {
            EXPECT_EQ(rowId, 5);
        } else {
            FAIL() << "Unexpected partition key combination: (" << k0 << "," << k1 << ")";
        }
    }

    delete comparator;
    delete vectorBatch;
}

TEST(Top1ComparatorTest, TieBreakingWithTwoPKeys)
{
    std::vector<int32_t> pkTypes = {omniruntime::type::OMNI_LONG, omniruntime::type::OMNI_LONG};
    std::vector<int32_t> pk = {0, 1};
    // Sort by 5th column ascending, then 6th column descending
    std::vector<int> sortColumnIds = {2, 3};
    std::vector<bool> ascending = {true, false};

    omnistream::VectorBatch* vectorBatch = getTwoKeyVectorBatch();
    // Create comparator
    auto* comparator = new Top1Comparator<RowData*>(pkTypes, pk, sortColumnIds, ascending);
    // template K = BinaryRowData*
    auto top1RowIds = comparator->findTop1RowIdsByPartitionV2(vectorBatch);

    /*  top1RowIds
        (1000, 1001), 0
        (1002, 1003), 3
        (1000, 1002), 4
        (1004, 1005), 6
    */

    for (auto& [keyPtr, rowId] : top1RowIds) {
        long* p0 = keyPtr->getLong(0);
        ASSERT_NE(p0, nullptr);
        int64_t k0 = *p0;

        long* p1 = keyPtr->getLong(1);
        ASSERT_NE(p1, nullptr);
        int64_t k1 = *p1;

        if (k0 == 1000 && k1 == 1001) {
            EXPECT_EQ(rowId, 0);
        } else if (k0 == 1002 && k1 == 1003) {
            EXPECT_EQ(rowId, 3);
        } else if (k0 == 1000 && k1 == 1002) {
            EXPECT_EQ(rowId, 4);
        } else if (k0 == 1004 && k1 == 1005) {
            EXPECT_EQ(rowId, 6);
        } else {
            FAIL() << "Unexpected partition key combination: (" << k0 << "," << k1 << ")";
        }
    }

    delete comparator;
    delete vectorBatch;
}

TEST(Top1ComparatorTest, EmptyBatchWithTwoPKeys)
{
    std::vector<int> sortColumnIds = {5};
    std::vector<bool> ascending = {true};
    omnistream::VectorBatch* emptyBatch = new omnistream::VectorBatch(3);
    std::vector<int32_t> pkTypes = {omniruntime::type::OMNI_LONG, omniruntime::type::OMNI_LONG};
    std::vector<int32_t> pk = {0, 1};
    auto* comparator = new Top1Comparator<RowData*>(pkTypes, pk, sortColumnIds, ascending);
    auto top1RowIds = comparator->findTop1RowIdsByPartition(emptyBatch);
    EXPECT_TRUE(top1RowIds.empty());

    delete comparator;
    delete emptyBatch;
}

TEST(Top1ComparatorTest, AllEqualValuesWithTwoPKeys)
{
    std::vector<int> sortColumnIds = {5}; // Sort by 5th column.
    std::vector<bool> ascending = {true}; // top1 has the smallest element in the sortColumn
    std::vector<int32_t> pkTypes = {omniruntime::type::OMNI_LONG, omniruntime::type::OMNI_LONG};
    std::vector<int32_t> pk = {0, 1};
    omnistream::VectorBatch* vectorBatch = getTwoKeyVectorBatch();
    auto sortColumn = dynamic_cast<omniruntime::vec::Vector<int64_t>*>(vectorBatch->Get(5));
    for (int row = 0; row < 8; row++) {
        sortColumn->SetValue(row, 500);
    }
    auto* comparator = new Top1Comparator<RowData*>(pkTypes, pk, sortColumnIds, ascending);
    auto top1RowIds = comparator->findTop1RowIdsByPartition(vectorBatch);

    for (auto& [keyPtr, rowId] : top1RowIds) {
        long* p0 = keyPtr->getLong(0);
        ASSERT_NE(p0, nullptr);
        int64_t k0 = *p0;

        long* p1 = keyPtr->getLong(1);
        ASSERT_NE(p1, nullptr);
        int64_t k1 = *p1;

        if (k0 == 1000 && k1 == 1001) {
            EXPECT_EQ(rowId, 0);
        } else if (k0 == 1002 && k1 == 1003) {
            EXPECT_EQ(rowId, 1);
        } else if (k0 == 1000 && k1 == 1002) {
            EXPECT_EQ(rowId, 2);
        } else if (k0 == 1004 && k1 == 1005) {
            EXPECT_EQ(rowId, 5);
        } else {
            FAIL() << "Unexpected partition key combination: (" << k0 << "," << k1 << ")";
        }
    }

    delete comparator;
    delete vectorBatch;
}
