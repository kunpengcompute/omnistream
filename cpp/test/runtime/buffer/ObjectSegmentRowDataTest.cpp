/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>
#include <memory>
#include "runtime/buffer/ObjectSegment.h"

TEST(ObjectSegmentRowDataTest, RejectsJoinedRowsBeforeVectorBatchClone)
{
    auto left = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    auto right = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    JoinedRowData joined(left.get(), right.get());
    StreamRecord record(&joined, 123L);
    record.setValueType(RecordValueType::JOINED_ROW_DATA);
    omnistream::ObjectSegment source(1);
    source.putObject(0, &record);
    omnistream::ObjectSegment copy(1);
    EXPECT_THROW(copy.put(0, &source, 0, 1), std::invalid_argument);
    EXPECT_EQ(source.getObject(0), &record);
}
