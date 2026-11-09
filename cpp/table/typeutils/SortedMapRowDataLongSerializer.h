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

#ifndef OMNISTREAM_SORTEDMAPROWDATALONGSERIALIZER_H
#define OMNISTREAM_SORTEDMAPROWDATALONGSERIALIZER_H

#include <string>
#include <vector>

#include "SortKeyCmp.h"
#include "SortedMapValue.h"
#include "core/typeutils/TypeSerializerSingleton.h"
#include "table/data/RowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"

// Wire format is byte-identical to Flink's SortedMapSerializer (used by
// SortedMapTypeInfo for RetractableTopNFunction's "sorted-map" state):
//   [int32 mapSize]
//   per entry in comparator order:
//     [sortKey RowData via RowDataSerializer]  (length-prefixed BinaryRowData)
//     [boolean value-null-flag]                (always false for a Long count)
//     [int64 count]                            (8-byte big-endian)
class SortedMapRowDataLongSerializer : public TypeSerializerSingleton {
public:
    SortedMapRowDataLongSerializer(int sortKeyArity,
                                   std::vector<omniruntime::type::DataTypeId> sortKeyTypeIds,
                                   std::vector<bool> ascending,
                                   std::vector<bool> nullsIsLast);

    ~SortedMapRowDataLongSerializer() override;

    // Returns a newly allocated SortedMapValue* the caller owns.
    void* deserialize(DataInputView& source) override;

    // record is a SortedMapValue*.
    void serialize(void* record, DataOutputSerializer& target) override;

    const char* getName() const override
    {
        return "SortedMapRowDataLongSerializer";
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::SORTED_MAP_BK;
    }

    std::string toJson() override;

private:
    BinaryRowDataSerializer* sortKeySerializer_;
    SortKeyCmp cmp_;
};

#endif // OMNISTREAM_SORTEDMAPROWDATALONGSERIALIZER_H
