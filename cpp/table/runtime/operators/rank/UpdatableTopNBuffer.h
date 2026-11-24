/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAM_UPDATABLETOPNBUFFER_H
#define OMNISTREAM_UPDATABLETOPNBUFFER_H

#include <map>
#include <string>
#include <vector>

#include "table/typeutils/SortKeyCmp.h"

// Native port of org.apache.flink.table.runtime.operators.rank.TopNBuffer as used
// by UpdatableTopNFunction. Stores mapping from sort key to the list of row keys
// (as serialized bytes) under that sort key; the list preserves insertion order
// (Flink's LinkedHashSet). The buffer also tracks the total number of records so
// the without-row-number algorithm can detect overflow.
//
// Layout mirrors the Java TreeMap<RowData, LinkedHashSet<RowData>>:
//   std::map<RowData*, std::vector<std::string>*, SortKeyCmp>
// The map keys are RowData sort keys ordered by SortKeyCmp. The list elements are
// the serialized row-key bytes (std::string), which index the operator's
// rowKeyMap (also keyed by the serialized bytes).
//
// Ownership: the buffer owns the stored sort-key RowData* copies (created by
// put() and freed on erase/removeAll/destruction). Probes passed in by the caller
// are never retained; the caller frees them.
class UpdatableTopNBuffer {
public:
    using Map = std::map<RowData*, std::vector<std::string>*, SortKeyCmp>;

    explicit UpdatableTopNBuffer(SortKeyCmp comparator) : treeMap_(comparator)
    {
    }

    ~UpdatableTopNBuffer()
    {
        clearAll();
    }

    UpdatableTopNBuffer(const UpdatableTopNBuffer&) = delete;
    UpdatableTopNBuffer& operator=(const UpdatableTopNBuffer&) = delete;

    // Appends a record into the buffer under sortKey. Returns the size of the
    // list under sortKey. The sortKey probe is copied into the buffer; the caller
    // still owns the probe.
    int put(RowData* sortKey, const std::string& rowKeyBytes)
    {
        currentTopNum_ += 1;
        auto it = treeMap_.find(sortKey);
        std::vector<std::string>* list = nullptr;
        if (it == treeMap_.end()) {
            list = new std::vector<std::string>();
            treeMap_.emplace(sortKey->copy(), list);
        } else {
            list = it->second;
        }
        list->push_back(rowKeyBytes);
        return static_cast<int>(list->size());
    }

    // Removes a single record under sortKey. The probe is caller-owned.
    void remove(RowData* sortKey, const std::string& rowKeyBytes)
    {
        auto it = treeMap_.find(sortKey);
        if (it == treeMap_.end()) {
            return;
        }
        std::vector<std::string>* list = it->second;
        auto lit = std::find(list->begin(), list->end(), rowKeyBytes);
        if (lit != list->end()) {
            list->erase(lit);
            currentTopNum_ -= 1;
        }
        if (list->empty()) {
            RowData* storedKey = it->first;
            treeMap_.erase(it);
            delete storedKey;
            delete list;
        }
    }

    // Removes all records under sortKey. The probe is caller-owned.
    void removeAll(RowData* sortKey)
    {
        auto it = treeMap_.find(sortKey);
        if (it == treeMap_.end()) {
            return;
        }
        currentTopNum_ -= static_cast<int>(it->second->size());
        RowData* storedKey = it->first;
        std::vector<std::string>* list = it->second;
        treeMap_.erase(it);
        delete storedKey;
        delete list;
    }

    // Removes and returns the last record of the last entry. Returns an empty
    // string when the buffer is empty.
    std::string removeLast()
    {
        if (treeMap_.empty()) {
            return {};
        }
        auto it = treeMap_.end();
        --it;
        std::vector<std::string>* list = it->second;
        std::string lastElement = list->back();
        list->pop_back();
        currentTopNum_ -= 1;
        if (list->empty()) {
            RowData* storedKey = it->first;
            treeMap_.erase(it);
            delete storedKey;
            delete list;
        }
        return lastElement;
    }

    // Returns the last record of the last entry (not removed). Empty string when
    // the buffer is empty.
    std::string lastElement() const
    {
        if (treeMap_.empty()) {
            return {};
        }
        auto it = treeMap_.end();
        --it;
        return it->second->back();
    }

    // Returns the record list under sortKey (or nullptr). The returned pointer
    // stays valid only until the next modifying call on the buffer.
    std::vector<std::string>* get(RowData* sortKey) const
    {
        auto it = treeMap_.find(sortKey);
        if (it == treeMap_.end()) {
            return nullptr;
        }
        return it->second;
    }

    bool containsKey(RowData* key) const
    {
        return treeMap_.find(key) != treeMap_.end();
    }

    // Checks whether a record with the given sort key should be put into the
    // buffer (Flink's TopNBuffer.checkSortKeyInBufferRange). Mirrors Flink: if the
    // buffer is empty -> true; if sortKey sorts strictly better than the worst
    // key -> true; otherwise only when the current size is below topNum.
    bool checkSortKeyInBufferRange(RowData* sortKey, long topNum) const
    {
        if (treeMap_.empty()) {
            return true;
        }
        auto it = treeMap_.end();
        --it;
        RowData* worstKey = it->first;
        if (treeMap_.key_comp()(sortKey, worstKey)) {
            return true;
        }
        return getCurrentTopNum() < topNum;
    }

    int getCurrentTopNum() const
    {
        return currentTopNum_;
    }

    Map::iterator begin()
    {
        return treeMap_.begin();
    }

    Map::iterator end()
    {
        return treeMap_.end();
    }

    Map::const_iterator begin() const
    {
        return treeMap_.begin();
    }

    Map::const_iterator end() const
    {
        return treeMap_.end();
    }

private:
    void clearAll()
    {
        for (auto& entry : treeMap_) {
            delete entry.first;
            delete entry.second;
        }
        treeMap_.clear();
        currentTopNum_ = 0;
    }

    Map treeMap_;
    int currentTopNum_ = 0;
};

#endif // OMNISTREAM_UPDATABLETOPNBUFFER_H
