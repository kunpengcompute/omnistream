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

#ifndef OMNISTREAM_SORTEDMAPVALUE_H
#define OMNISTREAM_SORTEDMAPVALUE_H

#include <map>

#include "SortKeyCmp.h"

// The persisted sorted map value of Flink's "sorted-map" ValueState<SortedMap<RowData, Long>>.
// Owns every RowData* sort key inserted into the map and frees them on destruction.
class SortedMapValue {
public:
    using Map = std::map<RowData*, int64_t, SortKeyCmp>;

    explicit SortedMapValue(const SortKeyCmp& cmp)
        : map_(cmp)
    {
    }

    ~SortedMapValue()
    {
        for (auto& entry : map_) {
            delete entry.first;
        }
    }

    Map& map()
    {
        return map_;
    }

private:
    Map map_;
};

#endif // OMNISTREAM_SORTEDMAPVALUE_H
