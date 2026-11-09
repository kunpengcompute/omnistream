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

#include "SortedMapRowDataLongSerializer.h"

#include <utility>

#include "core/include/common.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"

SortedMapRowDataLongSerializer::SortedMapRowDataLongSerializer(int sortKeyArity,
                                                               std::vector<omniruntime::type::DataTypeId> sortKeyTypeIds,
                                                               std::vector<bool> ascending,
                                                               std::vector<bool> nullsIsLast)
    : sortKeySerializer_(new BinaryRowDataSerializer(sortKeyArity)),
      cmp_(std::move(sortKeyTypeIds), std::move(ascending), std::move(nullsIsLast))
{
}

SortedMapRowDataLongSerializer::~SortedMapRowDataLongSerializer()
{
    delete sortKeySerializer_;
}

void SortedMapRowDataLongSerializer::serialize(void* record, DataOutputSerializer& target)
{
    auto* value = static_cast<SortedMapValue*>(record);
    target.writeInt(static_cast<uint32_t>(value->map().size()));
    for (const auto& entry : value->map()) {
        sortKeySerializer_->serialize(entry.first, target);
        target.writeBoolean(false);
        target.writeLong(entry.second);
    }
}

void* SortedMapRowDataLongSerializer::deserialize(DataInputView& source)
{
    const int size = source.readInt();
    auto* value = new SortedMapValue(cmp_);
    for (int i = 0; i < size; ++i) {
        // BinaryRowDataSerializer returns a reused buffer; copy so the map owns its key.
        auto* key = static_cast<RowData*>(sortKeySerializer_->deserialize(source));
        const bool isNull = source.readBoolean();
        if (isNull) {
            delete value;
            THROW_LOGIC_EXCEPTION("SortedMapRowDataLongSerializer: unexpected null map value");
        }
        const int64_t count = source.readLong();
        value->map()[key->copy()] = count;
    }
    return value;
}

std::string SortedMapRowDataLongSerializer::toJson()
{
    SerializerJsonInfo typeJson = {SerializerType::MAP, "", sortKeySerializer_, LongSerializer::INSTANCE};
    return typeJson.toJson();
}
