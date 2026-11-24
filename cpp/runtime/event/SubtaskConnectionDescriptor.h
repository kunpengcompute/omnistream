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

#ifndef OMNISTREAM_SUBTASKCONNECTIONDESCRIPTOR_H
#define OMNISTREAM_SUBTASKCONNECTIONDESCRIPTOR_H

#include "RuntimeEvent.h"
#include "sstream"

namespace omnistream {
class SubtaskConnectionDescriptor : public RuntimeEvent {
public:
    SubtaskConnectionDescriptor(int inputIndex, int outputIndex)
        : inputSubtaskIndex(inputIndex),
          outputSubtaskIndex(outputIndex)
    {
    }
    bool operator==(const SubtaskConnectionDescriptor& other) const
    {
        INFO_RELEASE("SubtaskConnectionDescriptor this :" << this->toString() << ",other:" << other.toString());
        return this == &other || (inputSubtaskIndex == other.getInputSubtaskIndex() &&
                                  outputSubtaskIndex == other.getOutputSubtaskIndex());
    }

    std::string toString() const
    {
        std::stringstream ss;
        ss << "SubtaskConnectionDescriptor{ inputSubtaskIndex=" << inputSubtaskIndex
           << ", outputSubtaskIndex=" << outputSubtaskIndex << "}";
        return ss.str();
    }

    std::size_t hashCode() const
    {
        constexpr std::size_t kGoldenRatio = 0x9e3779b9; // 2^32 / φ (φ ≈ 1.618)

        std::size_t seed = 0;

        std::size_t h1 = std::hash<int>{}(inputSubtaskIndex);
        seed ^= h1 + kGoldenRatio + (seed << 6) + (seed >> 2);

        std::size_t h2 = std::hash<int>{}(outputSubtaskIndex);
        seed ^= h2 + kGoldenRatio + (seed << 6) + (seed >> 2);

        return seed;
    }

    int getInputSubtaskIndex() const
    {
        return inputSubtaskIndex;
    }
    int getOutputSubtaskIndex() const
    {
        return outputSubtaskIndex;
    }

    long getComplexId()
    {
        return (((long)(inputSubtaskIndex)) << 32) | (outputSubtaskIndex & 0xFFFFFFFFL);
    }

    std::string GetEventClassName() override
    {
        return "SubtaskConnectionDescriptor";
    }

private:
    int inputSubtaskIndex;
    int outputSubtaskIndex;
};
} // namespace omnistream

#endif // OMNISTREAM_SUBTASKCONNECTIONDESCRIPTOR_H
