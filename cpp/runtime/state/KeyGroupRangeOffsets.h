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

#ifndef FLINK_TNEL_KEYGROUPRANGEOFFSETS_H
#define FLINK_TNEL_KEYGROUPRANGEOFFSETS_H
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/KeyedStateHandle.h"
#include <sstream>

class KeyGroupRangeOffsets {
public:
    static std::invalid_argument newIllegalKeyGroupException(int keyGroup, KeyGroupRange keyGroupRange)
    {
        std::string msg =
            "Key Group" + std::to_string(keyGroup) + "is not in" + keyGroupRange.ToString() +
            ". Unless you're directly using low level state access APIs, this"
            " is most likely caused by non-deterministic shuffle key (hashCode and equals implementation).";
        return std::invalid_argument(msg);
    }

    KeyGroupRangeOffsets(const KeyGroupRange& keyGroupRange, const std::vector<int64_t>& offsets)
        : keyGroupRange_(keyGroupRange),
          offsets_(offsets)
    {
        auto keyGroupSize = static_cast<size_t>(keyGroupRange_.getNumberOfKeyGroups());
        if (offsets_.size() != keyGroupSize) {
            std::string err = "Offsets length (" + std::to_string(offsets_.size()) +
                              ") does not match number of key groups (" + std::to_string(keyGroupSize) + ")";
            throw std::invalid_argument(err);
        }
    }

    KeyGroupRangeOffsets(int rangeStart, int rangeEnd, const std::vector<int64_t>& offsets)
        : KeyGroupRangeOffsets(*std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(rangeStart, rangeEnd)), offsets)
    {
    }

    KeyGroupRangeOffsets(int rangeStart, int rangeEnd)
        : KeyGroupRangeOffsets(*std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(rangeStart, rangeEnd)))
    {
    }

    KeyGroupRangeOffsets(KeyGroupRange& keyGroupRange)
        : KeyGroupRangeOffsets(keyGroupRange, std::vector<int64_t>(keyGroupRange.getNumberOfKeyGroups(), 0))
    {
    }

    int64_t getKeyGroupOffset(int keyGroup) const
    {
        size_t idx = computeKeyGroupIndex(keyGroup);
        return offsets_[idx];
    }

    void setKeyGroupOffset(int keyGroup, int64_t offset)
    {
        size_t idx = computeKeyGroupIndex(keyGroup);
        offsets_[idx] = offset;
    }

    const KeyGroupRange getKeyGroupRange() const
    {
        return keyGroupRange_;
    }

    const std::vector<int64_t>& getOffsets() const
    {
        return offsets_;
    }

    KeyGroupRangeOffsets getIntersection(const KeyGroupRange& keyGroupRange) const
    {
        std::unique_ptr<KeyGroupRange> ptr(keyGroupRange_.getIntersection(keyGroupRange));
        KeyGroupRange intersection = *ptr;
        size_t n = static_cast<size_t>(intersection.getNumberOfKeyGroups());
        std::vector<int64_t> subOffsets(n);
        if (n > 0) {
            size_t startIdx = computeKeyGroupIndex(intersection.getStartKeyGroup());
            std::copy(offsets_.begin() + startIdx, offsets_.begin() + startIdx + n, subOffsets.begin());
        }
        return KeyGroupRangeOffsets(intersection, subOffsets);
    }

    bool operator==(const KeyGroupRangeOffsets& other) const
    {
        if (this == &other) return true;
        if (keyGroupRange_ != other.keyGroupRange_) return false;
        return offsets_ == other.offsets_;
    }

    bool operator!=(const KeyGroupRangeOffsets& other) const
    {
        return !(*this == other);
    }

    std::string ToString() const
    {
        std::stringstream ss;
        ss << "{" << "\"keyGroupRange\":" << keyGroupRange_.ToString();
        ss << ",\"offsets\":[";
        for (size_t i = 0; i < offsets_.size(); ++i) {
            if (i > 0) ss << ",";
            ss << std::to_string(offsets_[i]);
        }
        ss << "]";
        ss << "}";
        return ss.str();
    }

    std::size_t hashCode() const
    {
        std::size_t seed = 0x6431A8E3;

        seed ^= (seed << 6) + (seed >> 2) + 0x13572468 + keyGroupRange_.hashCode();

        for (auto offset : offsets_) {
            seed ^= (seed << 6) + (seed >> 2) + 0x24681357 + std::hash<int64_t>()(offset);
        }

        return seed;
    }

private:
    KeyGroupRange keyGroupRange_;
    std::vector<int64_t> offsets_;

    size_t computeKeyGroupIndex(int keyGroup) const
    {
        int idx = keyGroup - keyGroupRange_.getStartKeyGroup();
        if (idx < 0 || static_cast<size_t>(idx) >= offsets_.size()) {
            throw newIllegalKeyGroupException(keyGroup, keyGroupRange_);
        }
        return idx;
    }
};

#endif // FLINK_TNEL_KEYGROUPRANGEOFFSETS_H
