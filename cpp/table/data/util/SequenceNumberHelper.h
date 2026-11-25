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

#pragma once

#include <vector>
#include <cstdint>

#include "common.h"

namespace omnistream {
class SequenceNumberHelper {
public:
    static constexpr uint32_t MAX_SEQUENCE_NUMBER = UINT32_MAX - 1;
    SequenceNumberHelper() : nextSequenceNumberByKeyGroup(128, 0)
    {
    }

    SequenceNumberHelper(int32_t maxParallelism) : nextSequenceNumberByKeyGroup(maxParallelism, 0)
    {
    }

    ~SequenceNumberHelper() = default;

    uint32_t getNextSequenceNumber(int32_t keyGroup)
    {
        checkKeyGroup(keyGroup);
        checkSequenceNumber(nextSequenceNumberByKeyGroup[keyGroup], MAX_SEQUENCE_NUMBER);
        return nextSequenceNumberByKeyGroup[keyGroup];
    }

    void addNextSequenceNumber(int32_t keyGroup)
    {
        checkKeyGroup(keyGroup);
        checkSequenceNumber(nextSequenceNumberByKeyGroup[keyGroup], MAX_SEQUENCE_NUMBER);
        ++nextSequenceNumberByKeyGroup[keyGroup];
    }

    /**
     * update the nextSequenceNumber of the keyGroup, if the nextSequenceNumber is greater than the current one,
     * @param keyGroup
     * @param nextSequenceNumber
     */
    void updateNextSequenceNumber(int32_t keyGroup, uint32_t nextSequenceNumber)
    {
        checkKeyGroup(keyGroup);
        if (nextSequenceNumber == 0) {
            THROW_RUNTIME_ERROR("updated nextSequenceNumber cannot be zero: " << keyGroup);
        }
        auto newCurSequenceNumber = nextSequenceNumber - 1;
        checkSequenceNumber(newCurSequenceNumber, MAX_SEQUENCE_NUMBER);

        if (nextSequenceNumber > nextSequenceNumberByKeyGroup[keyGroup]) {
            nextSequenceNumberByKeyGroup[keyGroup] = nextSequenceNumber;
            LOG("update nextSequenceNumber of keyGroup" << keyGroup << " to " << nextSequenceNumber);
        }
    }

private:
    std::vector<uint32_t> nextSequenceNumberByKeyGroup{};

    void checkKeyGroup(int32_t keyGroup)
    {
        if (keyGroup < 0 || keyGroup >= nextSequenceNumberByKeyGroup.size()) {
            THROW_RUNTIME_ERROR("keyGroup out of range: " << keyGroup);
        }
    }

    static void checkSequenceNumber(uint32_t sequenceNumber, uint32_t checkValue)
    {
        if (sequenceNumber > checkValue) {
            THROW_RUNTIME_ERROR("sequenceNumber cannot be greater than checkValue: " << checkValue);
        }
    }
};
} // namespace omnistream
