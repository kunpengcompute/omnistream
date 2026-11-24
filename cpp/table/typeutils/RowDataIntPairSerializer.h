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

#ifndef OMNISTREAM_ROWDATAINTPAIRSERIALIZER_H
#define OMNISTREAM_ROWDATAINTPAIRSERIALIZER_H

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/TypeSerializerSingleton.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"

// Value of UpdatableTopNFunction's "data-state-with-update" MapState
// (Flink: MapState<rowKey RowData, Tuple2<RowData, Integer>> where the
// Tuple2 f0 is the record and f1 is the inner rank within the same sort key).
//
// Wire format is byte-identical to Flink's TupleSerializer<Tuple2<RowData,Integer>>
// (TupleSerializer does not write an arity prefix):
//   [int32 rowSize]
//   [row bytes, preserving the RowKind byte]
//   [int32 innerRank]
//
// The row bytes are serialized with an inner BinaryRowDataSerializer whose arity
// is the full input row arity. `row` in RowDataIntPair is owned by the pair and
// must be deleted together with it.
struct RowDataIntPair {
    RowData* row; // owned deep copy of the record
    int innerRank;
};

class RowDataIntPairSerializer : public TypeSerializerSingleton {
public:
    explicit RowDataIntPairSerializer(int numFields)
        : rowSerializer_(numFields), numFields_(numFields)
    {
    }

    // Returns a newly allocated RowDataIntPair* the caller owns.
    void* deserialize(DataInputView& source) override
    {
        const int length = source.readInt();
        if (length <= 0) {
            ERROR_RELEASE("RowDataIntPairSerializer::deserialize invalid length: " << length);
            THROW_LOGIC_EXCEPTION("RowDataIntPairSerializer::deserialize invalid length " + std::to_string(length));
        }
        if (length > MAX_ROW_BYTES) {
            ERROR_RELEASE("RowDataIntPairSerializer::deserialize length " << length << " exceeds max " << MAX_ROW_BYTES);
            THROW_LOGIC_EXCEPTION(
                "RowDataIntPairSerializer::deserialize length " + std::to_string(length) + " exceeds max " +
                std::to_string(MAX_ROW_BYTES));
        }
        auto* bytes = new uint8_t[length];
        source.readFully(bytes, length, 0, length);
        auto* row = new BinaryRowData(numFields_);
        row->own(bytes, 0, length, length);
        const int innerRank = source.readInt();
        return new RowDataIntPair{row, innerRank};
    }

    // record is a RowDataIntPair*.
    void serialize(void* record, DataOutputSerializer& target) override
    {
        auto* pair = static_cast<RowDataIntPair*>(record);
        rowSerializer_.serialize(pair->row, target);
        target.writeInt(static_cast<uint32_t>(pair->innerRank));
    }

    [[nodiscard]] const char* getName() const override
    {
        return "RowDataIntPairSerializer";
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::ROW_INT_PAIR_BK;
    }

private:
    static constexpr int MAX_ROW_BYTES = 2048;

    BinaryRowDataSerializer rowSerializer_;
    int numFields_;
};

#endif // OMNISTREAM_ROWDATAINTPAIRSERIALIZER_H
