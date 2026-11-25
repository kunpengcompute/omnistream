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
#include <vector>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "OmniOperatorJIT/core/src/vector/large_string_container.h"
#include "OmniOperatorJIT/core/src/vector/vector_batch.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "vectorBatchTestUtils.h"

namespace omnistream {

// Shuffle the vector batch according to the shuffle map
std::vector<omniruntime::vec::VectorBatch*> shuffle(
    omniruntime::vec::VectorBatch* vectorBatch,
    std::vector<omniruntime::type::DataTypeId> inputTypes,
    std::vector<std::vector<int>> shuffleMap)
{
    std::vector<omniruntime::vec::VectorBatch*> result;

    for (int i = 0; i < shuffleMap.size(); ++i) { // ith shuffled sub partition
        result.push_back(createVectorBatch(
            inputTypes,
            shuffleMap[i].size())); // create a new vector batch for current sub partition

        for (int j = 0; j < shuffleMap[i].size(); ++j) { // `shuffleMap[i][j]` is the row index in original vector batch
                                                         // to be shuffled to current sub partition
            for (int k = 0; k < vectorBatch->GetVectorCount(); ++k) { // set value for each column in the row
                if (inputTypes[k] == omniruntime::type::OMNI_LONG) {
                    int64_t value =
                        omnistream::VectorGetValue<omniruntime::type::OMNI_LONG>(vectorBatch->Get(k), shuffleMap[i][j]);
                    omniruntime::vec::VectorHelper::SetValue(result[i]->Get(k), j, (void*)&value);
                } else if (inputTypes[k] == omniruntime::type::OMNI_CHAR) {
                    std::string_view stringView =
                        omnistream::VectorGetValue<omniruntime::type::OMNI_CHAR>(vectorBatch->Get(k), shuffleMap[i][j]);
                    std::string str(stringView.data(), stringView.size());
                    // str.resize(stringView.size()); // need this, because SetValue is using `data()` and `length()` to
                    // get the value, the columnar stringView is not null terminated, thus length() is counted till the
                    // end of the column
                    omniruntime::vec::VectorHelper::SetValue(result[i]->Get(k), j, (void*)&str);
                } else {
                    throw std::runtime_error("Unsupported vector type");
                }
            }
        }
    }

    return result;
}

TEST(ShuffleTest, SimpleShuffle)
{
    std::vector<omniruntime::type::DataTypeId> inputTypes = {
        omniruntime::type::OMNI_CHAR, omniruntime::type::OMNI_LONG, omniruntime::type::OMNI_CHAR};
    int rowCount = 10;
    omniruntime::vec::VectorBatch* vectorBatch = createVectorBatch(inputTypes, rowCount, true);

    std::vector<std::vector<int>> shuffleMap = {{0, 4, 7}, {8, 6, 1}, {2, 3, 5, 9}}; // shuffle map for 3 sub partitions

    omniruntime::vec::VectorHelper::PrintVecBatch(vectorBatch);

    std::vector<omniruntime::vec::VectorBatch*> result = shuffle(vectorBatch, inputTypes, shuffleMap);

    for (int i = 0; i < result.size(); ++i) {
        std::cout << std::endl << "Sub partition: " << i + 1 << std::endl;
        omniruntime::vec::VectorHelper::PrintVecBatch(result[i]);
    }
}

} // namespace omnistream
