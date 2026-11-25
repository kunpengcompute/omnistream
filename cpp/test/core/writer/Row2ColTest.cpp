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

#include <iostream>
#include <string_view>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "OmniOperatorJIT/core/src/vector/large_string_container.h"
#include "OmniOperatorJIT/core/src/vector/vector_batch.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "table/data/binary/BinaryRowData.h"
#include "vectorBatchTestUtils.h"

namespace omnistream {

omniruntime::vec::VectorBatch* convertToVectorBatch(
    const std::vector<BinaryRowData*>& binaryRows, const std::vector<omniruntime::type::DataTypeId>& inputTypes)
{
    size_t rowCount = binaryRows.size();
    size_t colCount = inputTypes.size();
    auto* vectorBatch = omnistream::createVectorBatch(inputTypes, rowCount); // not `doSetValue`

    // Assuming each BinaryRowData corresponds to a row in the VectorBatch
    for (size_t rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        BinaryRowData* rowData = binaryRows[rowIndex];

        for (size_t colIndex = 0; colIndex < inputTypes.size(); ++colIndex) {
            omniruntime::type::DataTypeId type = inputTypes[colIndex];

            if (type == omniruntime::type::OMNI_LONG) {
                long* value = rowData->getLong(colIndex);
                omniruntime::vec::VectorHelper::SetValue(vectorBatch->Get(colIndex), rowIndex, (void*)value);
            } else if (type == omniruntime::type::OMNI_CHAR) {
                std::string_view value = rowData->getStringView(colIndex);
                std::string convertedValue = std::string(value);
                omniruntime::vec::VectorHelper::SetValue(vectorBatch->Get(colIndex), rowIndex, (void*)&convertedValue);
            }
        }
    }

    return vectorBatch;
}

} // namespace omnistream

TEST(ConvertToVectorBatchTest, BasicConversion)
{
    // Define the schema
    std::vector<omniruntime::type::DataTypeId> inputTypes = {
        omniruntime::type::OMNI_LONG,
        omniruntime::type::OMNI_LONG,
        omniruntime::type::OMNI_LONG,
        omniruntime::type::OMNI_CHAR};

    // Create some sample BinaryRowData
    std::vector<BinaryRowData*> binaryRows;
    for (int i = 0; i < 5; ++i) { // Create 5 rows for example
        auto* rowData = BinaryRowData::createBinaryRowDataWithMem(inputTypes.size());

        std::cout << "Row " << i << ": ";
        for (int j = 0; j < inputTypes.size(); ++j) {
            if (inputTypes[j] == omniruntime::type::OMNI_LONG) {
                rowData->setLong(j, static_cast<long>(i * 10 + j));
                std::cout << i * 10 + j << " ";
            } else if (inputTypes[j] == omniruntime::type::OMNI_CHAR) {
                std::string prefix = "row";
                std::string suffix = std::to_string(i);
                std::string value = prefix + suffix;
                std::string_view sv = value;
                rowData->setStringView(j, sv);

                std::cout << value << " ";
            }
        }
        std::cout << std::endl;
        binaryRows.push_back(rowData);
    }

    // Convert to VectorBatch
    omniruntime::vec::VectorBatch* vectorBatch = omnistream::convertToVectorBatch(binaryRows, inputTypes);

    // Verify the results
    for (int colIndex = 0; colIndex < inputTypes.size(); ++colIndex) {
        auto* vector = vectorBatch->Get(colIndex);
        std::cout << "Column " << colIndex << ": ";
        for (int rowIndex = 0; rowIndex < vectorBatch->GetRowCount(); ++rowIndex) {
            if (inputTypes[colIndex] == omniruntime::type::OMNI_LONG) {
                omniruntime::vec::Vector<int64_t>* vector_int64 =
                    static_cast<omniruntime::vec::Vector<int64_t>*>(vector);
                std::cout << vector_int64->GetValue(rowIndex) << " ";
                EXPECT_EQ(vector_int64->GetValue(rowIndex), static_cast<long>(rowIndex * 10 + colIndex));
            } else if (inputTypes[colIndex] == omniruntime::type::OMNI_CHAR) {
                omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>* vector_string_view =
                    static_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                        vector);
                std::cout << vector_string_view->GetValue(rowIndex).data() << " ";
                EXPECT_EQ(vector_string_view->GetValue(rowIndex), std::string("row" + std::to_string(rowIndex)));
            }
        }
        std::cout << std::endl;
    }

    // Clean up
    delete vectorBatch;
    for (auto* rowData : binaryRows) {
        delete rowData;
    }
}
