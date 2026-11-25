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

#include <stdexcept>
#include <nlohmann/json.hpp>
#include "core/utils/Iterator.h"

// todo: pointer is not necessary
class KeyGroupRange {
public:
    KeyGroupRange()
    {
        startKeyGroup = 0;
        endKeyGroup = -1;
    }

    static KeyGroupRange* EMPTY_KEY_GROUP_RANGE()
    {
        return new KeyGroupRange();
    }

    KeyGroupRange(int startKeyGroup, int endKeyGroup)
    {
        if (startKeyGroup < 0) {
            throw std::invalid_argument("IllegalArgumentException");
        }
        if (startKeyGroup > endKeyGroup) {
            throw std::invalid_argument("IllegalArgumentException");
        }
        this->startKeyGroup = startKeyGroup;
        this->endKeyGroup = endKeyGroup;
        if (getNumberOfKeyGroups() < 0) {
            throw std::invalid_argument("Potential Overflow Detected.");
        }
    }

    bool contains(int keyGroup) const
    {
        return keyGroup >= startKeyGroup && keyGroup <= endKeyGroup;
    }

    int getNumberOfKeyGroups()
    {
        return 1 + endKeyGroup - startKeyGroup;
    }

    int getStartKeyGroup() const
    {
        return startKeyGroup;
    }

    int getEndKeyGroup() const
    {
        return endKeyGroup;
    }

    int getKeyGroupId(int idx)
    {
        return startKeyGroup + idx;
    }

    bool equals(KeyGroupRange other)
    {
        return startKeyGroup == other.startKeyGroup && endKeyGroup == other.endKeyGroup;
    }

    std::string ToString() const
    {
        nlohmann::json json;
        json["startKeyGroup"] = startKeyGroup;
        json["endKeyGroup"] = endKeyGroup;
        json["numberOfKeyGroups"] = getNumberOfKeyGroups();
        return json.dump();
    }
    int getNumberOfKeyGroups() const
    {
        return 1 + endKeyGroup - startKeyGroup;
    }
    KeyGroupRange* getIntersection(const KeyGroupRange& other) const
    {
        int start = std::max(startKeyGroup, other.startKeyGroup);
        int end = std::min(endKeyGroup, other.endKeyGroup);
        if (start <= end) {
            return new KeyGroupRange(start, end);
        } else {
            return EMPTY_KEY_GROUP_RANGE();
        }
    }
    bool operator==(const KeyGroupRange& other) const
    {
        return startKeyGroup == other.startKeyGroup && endKeyGroup == other.endKeyGroup;
    }

    bool operator!=(const KeyGroupRange& other) const
    {
        return !(*this == other);
    }

    static KeyGroupRange* of(int startKeyGroupInput, int endKeyGroupInput)
    {
        if (startKeyGroupInput <= endKeyGroupInput) {
            return new KeyGroupRange(startKeyGroupInput, endKeyGroupInput);
        } else {
            return EMPTY_KEY_GROUP_RANGE();
        }
    }

    std::size_t hashCode() const
    {
        std::size_t seed = 0x43987123;
        seed ^= (seed << 6) + (seed >> 2) + 0x1762F1AB + std::hash<int>()(startKeyGroup);
        seed ^= (seed << 6) + (seed >> 2) + 0x13572357 + std::hash<int>()(endKeyGroup);
        return seed;
    }
    // Iterator
    class KeyGroupIterator : public omnistream::utils::Iterator<int> {
    public:
        explicit KeyGroupIterator(const KeyGroupRange& r) : range(r), iteratorPos(0)
        {
        }
        bool hasNext()
        {
            return iteratorPos < 1 + range.endKeyGroup - range.startKeyGroup;
        }
        int next()
        {
            return range.startKeyGroup + (iteratorPos++);
        }

    private:
        const KeyGroupRange& range;
        int iteratorPos;
    };
    KeyGroupIterator iterator()
    {
        return KeyGroupIterator(*this);
    }

private:
    int startKeyGroup;
    int endKeyGroup;
};
