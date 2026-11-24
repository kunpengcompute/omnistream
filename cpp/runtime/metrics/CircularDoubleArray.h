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

#ifndef CIRCULARDOUBLEARRAY_H
#define CIRCULARDOUBLEARRAY_H
#include <mutex>
#include <vector>

namespace omnistream {
class CircularDoubleArray {
public:
    explicit CircularDoubleArray(int windowSize);
    void AddValue(double value);
    std::vector<double> ToUnsortedArray();
    int GetSize();
    long GetElementsSeen();

private:
    std::vector<double> backingArray;
    int nextPos;
    bool fullSize;
    long elementsSeen;
    std::mutex mtx;
};
} // namespace omnistream
#endif // CIRCULARDOUBLEARRAY_H
