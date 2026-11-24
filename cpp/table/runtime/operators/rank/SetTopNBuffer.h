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
#include "data/util/VectorBatchUtil.h"

#include <set>
#include <vector>

/**
 * In-memory ordered TopN buffer.
 * Stores comboIds as int64_t, matching the state backend representation.
 * Uses multiset so equal-sort rows can coexist.
 */
template <typename Comparator>
class SetTopNBuffer {
public:
    using Buffer = std::multiset<int64_t, Comparator>;

    explicit SetTopNBuffer(Comparator cmp) : buffer_(cmp), bufferId(-99)
    {
    }

    inline auto begin()
    {
        return buffer_.begin();
    }
    inline auto end()
    {
        return buffer_.end();
    }

    inline bool AddElement(int64_t id)
    {
        buffer_.insert(id);
        return true;
    }

    inline void RemoveSmallestElement()
    {
        if (!buffer_.empty()) {
            auto it = buffer_.end();
            --it;
            buffer_.erase(it);
        }
    }

    inline int GetSize() const
    {
        return (int)buffer_.size();
    }

    inline int64_t GetSmallestElement() const
    {
        auto it = buffer_.end();
        --it;
        return *it;
    }

    inline void LoadFromPlainVector(const std::vector<int64_t>& plain)
    {
        buffer_.clear();
        buffer_.insert(plain.begin(), plain.end());
    }

    inline std::vector<int64_t>* ToPlainVector() const
    {
        return new std::vector<int64_t>(buffer_.begin(), buffer_.end());
    }
    inline void SetBufferId(int id)
    {
        bufferId = id;
    }
    inline int GetBufferId() const
    {
        return bufferId;
    }

private:
    Buffer buffer_;
    int bufferId; // unique buffer identifier
};
