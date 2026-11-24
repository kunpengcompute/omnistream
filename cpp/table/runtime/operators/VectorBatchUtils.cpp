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

#include "VectorBatchUtils.h"

void VectorBatchUtils::AppendLongVectorForInt64(
    omnistream::VectorBatch* outputVB,
    std::vector<std::unique_ptr<RowData>>& collectedRows,
    int numRowsPerVB,
    int colIndex)
{
    auto* vector = new omniruntime::vec::Vector<int64_t>(numRowsPerVB);
    for (int rowIndex = 0; rowIndex < numRowsPerVB; ++rowIndex) {
        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
    }
    outputVB->Append(vector);
}

void VectorBatchUtils::AppendLongVectorForDouble(
    omnistream::VectorBatch* outputVB,
    std::vector<std::unique_ptr<RowData>>& collectedRows,
    int numRowsPerVB,
    int colIndex)
{
    auto* vector = new omniruntime::vec::Vector<double>(numRowsPerVB);
    for (int rowIndex = 0; rowIndex < numRowsPerVB; ++rowIndex) {
        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
    }
    outputVB->Append(vector);
}

void VectorBatchUtils::AppendIntVector(
    omnistream::VectorBatch* outputVB,
    std::vector<std::unique_ptr<RowData>>& collectedRows,
    int numRowsPerVB,
    int colIndex)
{
    auto* vector = new omniruntime::vec::Vector<int32_t>(numRowsPerVB);
    for (int rowIndex = 0; rowIndex < numRowsPerVB; ++rowIndex) {
        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
    }
    outputVB->Append(vector);
}

void VectorBatchUtils::AppendIntVectorForBool(
    omnistream::VectorBatch* outputVB,
    std::vector<std::unique_ptr<RowData>>& collectedRows,
    int numRowsPerVB,
    int colIndex)
{
    auto* vector = new omniruntime::vec::Vector<bool>(numRowsPerVB);
    for (int rowIndex = 0; rowIndex < numRowsPerVB; ++rowIndex) {
        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
    }
    outputVB->Append(vector);
}

void VectorBatchUtils::AppendStringVector(
    omnistream::VectorBatch* outputVB,
    std::vector<std::unique_ptr<RowData>>& collectedRows,
    int numRowsPerVB,
    int colIndex)
{
    // Add bounds checking to prevent infinite loop
    if (numRowsPerVB < 0 || numRowsPerVB > static_cast<int>(collectedRows.size())) {
        throw std::runtime_error("Invalid numRowsPerVB: " + std::to_string(numRowsPerVB));
    }
    auto* vector = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(numRowsPerVB);
    for (int rowIndex = 0; rowIndex < numRowsPerVB; ++rowIndex) {
        std::string_view strView = collectedRows[rowIndex]->getStringView(colIndex);
        vector->SetValue(rowIndex, strView);
    }
    outputVB->Append(vector);
}
