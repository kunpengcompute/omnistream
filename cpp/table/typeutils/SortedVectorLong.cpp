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

#include "SortedVectorLong.h"

#include <vector>

void* SortedVectorLong::deserialize(DataInputView& source)
{
    int eleNum = source.readInt();
    std::vector<long>* vecPtr = new std::vector<long>();
    vecPtr->reserve(eleNum);
    for (int i = 0; i < eleNum; i++) {
        long value = source.readLong();
        vecPtr->push_back(value);
    }
    return vecPtr;
}

void SortedVectorLong::serialize(void* record, DataOutputSerializer& target)
{
    std::vector<long>* vecPtr = static_cast<std::vector<long>*>(record);
    int size = vecPtr->size();
    target.writeInt(size);
    for (const long& value : *vecPtr) {
        target.writeLong(value);
    }
}

SortedVectorLong* SortedVectorLong::INSTANCE = new SortedVectorLong();
