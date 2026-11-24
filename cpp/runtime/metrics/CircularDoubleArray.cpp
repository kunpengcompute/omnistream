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

#include "CircularDoubleArray.h"
#include <algorithm> // for std::copy

namespace omnistream {
CircularDoubleArray::CircularDoubleArray(int windowSize)
    : backingArray(windowSize, 0.0),
      nextPos(0),
      fullSize(false),
      elementsSeen(0)
{
}

void CircularDoubleArray::AddValue(double value)
{
    std::lock_guard<std::mutex> lock(mtx);
    backingArray[nextPos] = value;
    ++elementsSeen;
    ++nextPos;
    if (nextPos == static_cast<int>(backingArray.size())) {
        nextPos = 0;
        fullSize = true;
    }
}

std::vector<double> CircularDoubleArray::ToUnsortedArray()
{
    std::lock_guard<std::mutex> lock(mtx);
    int size = GetSize();
    std::vector<double> result(size);
    std::copy(backingArray.begin(), backingArray.begin() + size, result.begin());
    return result;
}

int CircularDoubleArray::GetSize()
{
    // Called within a lock so no additional locking is required.
    return fullSize ? static_cast<int>(backingArray.size()) : nextPos;
}

long CircularDoubleArray::GetElementsSeen()
{
    // Called within a lock so no additional locking is required.
    return elementsSeen;
}
} // namespace omnistream
